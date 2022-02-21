import json as _json
import signal
from asyncio import (AbstractEventLoop as _AbstractEventLoop,
                     Queue as _Queue,
                     get_event_loop as _get_event_loop,
                     new_event_loop as _new_event_loop,
                     set_event_loop as _set_event_loop)
from typing import (Any as _Any,
                    Awaitable as _Awaitable,
                    Callable as _Callable,
                    Collection as _Collection,
                    Dict as _Dict)

from aiohttp import (ClientError as _ClientError,
                     ClientSession as _ClientSession,
                     hdrs as _hdrs,
                     web as _web,
                     web_ws as _web_ws)
from reprit.base import generate_repr as _generate_repr
from yarl import URL as _URL

from .hints import Time as _Time
from .messages import MessageKind as _MessageKind
from .node import Node as _Node
from .receiver import Receiver as _Receiver
from .result import (Error as _Error,
                     Ok as _Ok,
                     Result as _Result)
from .sender import (ReceiverUnavailable as _ReceiverUnavailable,
                     Sender as _Sender)


class Receiver(_Receiver):
    __slots__ = '_app', '_is_running', '_node'

    def __new__(cls, _node: _Node) -> 'Receiver':
        if not isinstance(_node.sender, Sender):
            raise TypeError('node supposed to have compatible sender type, '
                            f'but found {type(_node.sender)}')
        self = super().__new__(cls)
        self._node = _node
        self._is_running = False
        app = self._app = _web.Application()

        @_web.middleware
        async def error_middleware(
                request: _web.Request,
                handler: _Callable[[_web.Request],
                                   _Awaitable[_web.StreamResponse]],
                log: _Callable[[str], None] = _node.logger.exception
        ) -> _web.StreamResponse:
            try:
                result = await handler(request)
            except _web.HTTPException:
                raise
            except Exception:
                log('Something unexpected happened:')
                raise
            else:
                return result

        app.middlewares.append(error_middleware)
        app.router.add_delete('/', self._handle_delete)
        app.router.add_post('/', self._handle_post)
        app.router.add_route(Sender.HTTP_METHOD, '/',
                             self._handle_communication)
        for action in _node.processors.keys():
            route = app.router.add_post(f'/{action}', self._handle_record)
            resource = route.resource
            _node.logger.debug(f'registered resource {resource.canonical}')
        return self

    __repr__ = _generate_repr(__new__)

    @property
    def is_running(self) -> bool:
        return self._is_running

    def start(self) -> None:
        if self.is_running:
            raise RuntimeError('Already running')
        url = self._node.url
        _web.run_app(self.app,
                     host=url.host,
                     port=url.port,
                     loop=self._node.loop,
                     print=lambda message: (self._set_running(True)
                                            or print(message)))

    def stop(self) -> None:
        if self._is_running:
            try:
                signal.raise_signal(signal.SIGINT)
            finally:
                self._set_running(False)

    async def _handle_communication(self, request: _web.Request
                                    ) -> _web_ws.WebSocketResponse:
        websocket = _web_ws.WebSocketResponse()
        await websocket.prepare(request)
        async for message in websocket:
            message: _web_ws.WSMessage
            contents = message.json()
            reply = await self._node.receive(
                    kind=_MessageKind(contents['kind']),
                    message=contents['message']
            )
            await websocket.send_json(reply)
        return websocket

    async def _handle_delete(self, request: _web.Request) -> _web.Response:
        text = await request.text()
        if text:
            raw_nodes_urls = _json.loads(text)
            assert isinstance(raw_nodes_urls, list)
            nodes_urls = [_URL(raw_url) for raw_url in raw_nodes_urls]
            error_message = await self._node.detach_nodes(nodes_urls)
        else:
            error_message = await self._node.detach()
        result = {'error': error_message}
        return _web.json_response(result)

    async def _handle_post(self, request: _web.Request) -> _web.Response:
        text = await request.text()
        if text:
            raw_urls = _json.loads(text)
            assert isinstance(raw_urls, list)
            nodes_urls = [_URL(raw_url) for raw_url in raw_urls]
            error_message = await self._node.attach_nodes(nodes_urls)
        else:
            error_message = await self._node.solo()
        return _web.json_response({'error': error_message})

    async def _handle_record(self, request: _web.Request) -> _web.Response:
        parameters = await request.json()
        error_message = await self._node.enqueue(request.path[1:], parameters)
        return _web.json_response({'error': error_message})

    def _set_running(self, value: bool) -> None:
        self._is_running = value


class Sender(_Sender):
    HTTP_METHOD = _hdrs.METH_PATCH
    assert (HTTP_METHOD is not _hdrs.METH_POST
            and HTTP_METHOD is not _hdrs.METH_DELETE)

    def __init__(self,
                 *,
                 heartbeat: _Time,
                 urls: _Collection[_URL]) -> None:
        self._heartbeat, self._urls = heartbeat, urls
        self._loop = _safe_get_event_loop()
        self._messages: _Dict[_URL, _Queue] = {receiver: _Queue()
                                               for receiver in self._urls}
        self._results = {url: {kind: _Queue() for kind in _MessageKind}
                         for url in self._urls}
        self._session = _ClientSession(loop=self._loop)
        self._channels = {url: self._loop.create_task(self._channel(url))
                          for url in self._urls}

    __repr__ = _generate_repr(__init__)

    @property
    def heartbeat(self) -> _Time:
        return self._heartbeat

    @property
    def urls(self) -> _Collection[_URL]:
        return self._urls

    @urls.setter
    def urls(self, value: _Collection[_URL]) -> None:
        new_urls, old_urls = set(value), set(self._urls)
        for removed_url in old_urls - new_urls:
            self._disconnect(removed_url)
        self._urls = value
        for added_url in new_urls - old_urls:
            self._connect(added_url)

    async def send(self, *, kind: _MessageKind, message: _Any, url: _URL
                   ) -> _Any:
        assert kind in _MessageKind, kind
        try:
            messages = self._messages[url]
        except KeyError:
            raise _ReceiverUnavailable(url)
        messages.put_nowait((kind, message))
        result: _Result = await self._results[url][kind].get()
        try:
            return result.value
        except (_ClientError, OSError):
            raise _ReceiverUnavailable(url)

    async def _channel(self, url: _URL) -> None:
        messages, results = self._messages[url], self._results[url]
        kind, message = await messages.get()
        while True:
            try:
                async with self._session.ws_connect(
                        url,
                        heartbeat=self.heartbeat,
                        method=self.HTTP_METHOD,
                        timeout=self.heartbeat
                ) as connection:
                    try:
                        await connection.send_json({'kind': kind,
                                                    'message': message})
                    except (_ClientError, OSError):
                        continue
                    async for reply in connection:
                        reply: _web_ws.WSMessage
                        results[kind].put_nowait(_Ok(reply.json()))
                        kind, message = await messages.get()
                        try:
                            await connection.send_json({'kind': kind,
                                                        'message': message})
                        except (_ClientError, OSError):
                            continue
            except (_ClientError, OSError) as exception:
                results[kind].put_nowait(_Error(exception))
                kind, message = await messages.get()

    def _connect(self, url: _URL) -> None:
        self._messages[url] = _Queue()
        self._results[url] = {kind: _Queue() for kind in _MessageKind}
        self._channels[url] = self._loop.create_task(self._channel(url))

    def _disconnect(self, url: _URL) -> None:
        self._channels.pop(url).cancel()
        del self._messages[url], self._results[url]


def _safe_get_event_loop() -> _AbstractEventLoop:
    try:
        result = _get_event_loop()
    except RuntimeError:
        result = _new_event_loop()
        _set_event_loop(result)
    return result
