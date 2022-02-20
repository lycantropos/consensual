import json as _json
from asyncio import (Queue as _Queue,
                     get_event_loop as _get_event_loop)
from typing import (Any as _Any,
                    Awaitable as _Awaitable,
                    Callable as _Callable,
                    Collection as _Collection,
                    Dict as _Dict,
                    TypeVar as _TypeVar)

from aiohttp import (ClientError as _ClientError,
                     ClientSession as _ClientSession,
                     hdrs as _hdrs,
                     web as _web,
                     web_ws as _web_ws)
from reprit.base import generate_repr as _generate_repr
from yarl import URL as _URL

from .hints import Time as _Time
from .messages import CallKey as _CallKey
from .node import Node as _Node
from .receiver import Receiver as _Receiver
from .result import (Error as _Error,
                     Ok as _Ok,
                     Result as _Result)
from .sender import (ReceiverUnavailable as _ReceiverUnavailable,
                     Sender as _Sender)


class Receiver(_Receiver):
    def __init__(self, *, app: _web.Application, node: _Node) -> None:
        self.app, self.node = app, node

    @classmethod
    def from_node(cls, node: _Node) -> 'Receiver':
        app = _web.Application()

        @_web.middleware
        async def error_middleware(
                request: _web.Request,
                handler: _Callable[[_web.Request],
                                   _Awaitable[_web.StreamResponse]],
                log: _Callable[[str], None] = node.logger.exception
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
        self = cls(app=app,
                   node=node)
        app.router.add_delete('/', self._handle_delete)
        app.router.add_post('/', self._handle_post)
        app.router.add_route(Sender.HTTP_METHOD, '/',
                             self._handle_communication)
        for path in node.processors.keys():
            app.router.add_post(path, self._handle_record)
        return self

    def start(self) -> None:
        url = self.node.url
        _web.run_app(self.app,
                     host=url.host,
                     port=url.port,
                     loop=self.node.loop)

    async def _handle_communication(self, request: _web.Request
                                    ) -> _web_ws.WebSocketResponse:
        websocket = _web_ws.WebSocketResponse()
        await websocket.prepare(request)
        async for message in websocket:
            message: _web_ws.WSMessage
            contents = message.json()
            reply = await self.node.receive_json(_CallKey(contents['key']),
                                                 contents['message'])
            await websocket.send_json(reply)
        return websocket

    async def _handle_delete(self, request: _web.Request) -> _web.Response:
        text = await request.text()
        if text:
            raw_nodes_urls = _json.loads(text)
            assert isinstance(raw_nodes_urls, list)
            nodes_urls = [_URL(raw_url) for raw_url in raw_nodes_urls]
            error_message = await self.node.detach_nodes(nodes_urls)
        else:
            error_message = await self.node.detach()
        result = {'error': error_message}
        return _web.json_response(result)

    async def _handle_post(self, request: _web.Request) -> _web.Response:
        text = await request.text()
        if text:
            raw_urls = _json.loads(text)
            assert isinstance(raw_urls, list)
            nodes_urls = [_URL(raw_url) for raw_url in raw_urls]
            error_message = await self.node.attach_nodes(nodes_urls)
        else:
            error_message = await self.node.solo()
        return _web.json_response({'error': error_message})

    async def _handle_record(self, request: _web.Request) -> _web.Response:
        parameters = await request.json()
        error_message = await self.node.enqueue(request.path, parameters)
        return _web.json_response({'error': error_message})


_Key = _TypeVar('_Key')


class Sender(_Sender[_URL, _Key]):
    HTTP_METHOD = _hdrs.METH_PATCH
    assert (HTTP_METHOD is not _hdrs.METH_POST
            and HTTP_METHOD is not _hdrs.METH_DELETE)

    def __init__(self,
                 *,
                 heartbeat: _Time,
                 keys: _Collection[_Key],
                 receivers: _Collection[_URL]) -> None:
        self._heartbeat, self._keys, self._receivers = (
            heartbeat, keys, receivers
        )
        self._loop = _get_event_loop()
        self._messages: _Dict[_URL, _Queue] = {receiver: _Queue()
                                               for receiver in self._receivers}
        self._results: _Dict[_URL, _Dict[_Key, _Queue]] = {
            receiver: {key: _Queue() for key in self.keys}
            for receiver in self._receivers
        }
        self._session = _ClientSession(loop=self._loop)
        self._channels = {
            receiver: self._loop.create_task(self._channel(receiver))
            for receiver in self._receivers
        }

    __repr__ = _generate_repr(__init__)

    @property
    def heartbeat(self) -> _Time:
        return self._heartbeat

    @property
    def keys(self) -> _Collection[_Key]:
        return self._keys

    @property
    def receivers(self) -> _Collection[_URL]:
        return self._receivers

    @receivers.setter
    def receivers(self, value: _Collection[_URL]) -> None:
        new_receivers, old_receivers = set(value), set(self._receivers)
        for removed_receiver in old_receivers - new_receivers:
            self._disconnect(removed_receiver)
        self._receivers = value
        for added_receiver in new_receivers - old_receivers:
            self._connect(added_receiver)

    async def send_json(self, receiver: _URL, key: _Key, message: _Any
                        ) -> _Any:
        assert key in self.keys, key
        try:
            messages = self._messages[receiver]
        except KeyError:
            raise _ReceiverUnavailable(receiver)
        messages.put_nowait((key, message))
        result: _Result = await self._results[receiver][key].get()
        try:
            return result.value
        except (_ClientError, OSError):
            raise _ReceiverUnavailable(receiver)

    async def _channel(self, receiver: _URL) -> None:
        messages, results = self._messages[receiver], self._results[receiver]
        key, message = await messages.get()
        while True:
            try:
                async with self._session.ws_connect(
                        receiver,
                        heartbeat=self.heartbeat,
                        method=self.HTTP_METHOD,
                        timeout=self.heartbeat
                ) as connection:
                    try:
                        await connection.send_json({'key': key,
                                                    'message': message})
                    except (_ClientError, OSError):
                        continue
                    async for reply in connection:
                        reply: _web_ws.WSMessage
                        results[key].put_nowait(_Ok(reply.json()))
                        key, message = await messages.get()
                        try:
                            await connection.send_json({'key': key,
                                                        'message': message})
                        except (_ClientError, OSError):
                            continue
            except (_ClientError, OSError) as exception:
                results[key].put_nowait(_Error(exception))
                key, message = await messages.get()

    def _connect(self, receiver: _URL) -> None:
        self._messages[receiver] = _Queue()
        self._results[receiver] = {key: _Queue() for key in self.keys}
        self._channels[receiver] = self._loop.create_task(
                self._channel(receiver)
        )

    def _disconnect(self, receiver: _URL) -> None:
        self._channels.pop(receiver).cancel()
        del self._messages[receiver], self._results[receiver]
