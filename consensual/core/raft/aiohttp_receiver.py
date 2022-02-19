import json
from typing import (Awaitable,
                    Callable)

from aiohttp import (web,
                     web_ws)
from yarl import URL

from .aiohttp_sender import AiohttpSender
from .messages import CallKey
from .node import Node
from .receiver import Receiver


class AiohttpReceiver(Receiver):
    def __init__(self, *, app: web.Application, node: Node) -> None:
        self.app, self.node = app, node

    @classmethod
    def from_node(cls, node: Node) -> 'AiohttpReceiver':
        app = web.Application()

        @web.middleware
        async def error_middleware(
                request: web.Request,
                handler: Callable[[web.Request],
                                  Awaitable[web.StreamResponse]],
                log: Callable[[str], None] = node.logger.exception
        ) -> web.StreamResponse:
            try:
                result = await handler(request)
            except web.HTTPException:
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
        app.router.add_route(AiohttpSender.HTTP_METHOD, '/',
                             self._handle_communication)
        for path in node.processors.keys():
            app.router.add_post(path, self._handle_record)
        return self

    def start(self) -> None:
        url = self.node.url
        web.run_app(self.app,
                    host=url.host,
                    port=url.port,
                    loop=self.node.loop)

    async def _handle_communication(self, request: web.Request
                                    ) -> web_ws.WebSocketResponse:
        websocket = web_ws.WebSocketResponse()
        await websocket.prepare(request)
        async for message in websocket:
            message: web_ws.WSMessage
            contents = message.json()
            reply = await self.node.receive_json(CallKey(contents['key']),
                                                 contents['message'])
            await websocket.send_json(reply)
        return websocket

    async def _handle_delete(self, request: web.Request) -> web.Response:
        text = await request.text()
        if text:
            raw_nodes_urls = json.loads(text)
            assert isinstance(raw_nodes_urls, list)
            nodes_urls = [URL(raw_url) for raw_url in raw_nodes_urls]
            error_message = await self.node.detach_nodes(nodes_urls)
        else:
            error_message = await self.node.detach()
        result = {'error': error_message}
        return web.json_response(result)

    async def _handle_post(self, request: web.Request) -> web.Response:
        text = await request.text()
        if text:
            raw_urls = json.loads(text)
            assert isinstance(raw_urls, list)
            nodes_urls = [URL(raw_url) for raw_url in raw_urls]
            error_message = await self.node.attach_nodes(nodes_urls)
        else:
            error_message = await self.node.solo()
        return web.json_response({'error': error_message})

    async def _handle_record(self, request: web.Request) -> web.Response:
        parameters = await request.json()
        error_message = await self.node.enqueue(request.path, parameters)
        return web.json_response({'error': error_message})
