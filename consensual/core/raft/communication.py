import asyncio
from collections import deque
from typing import (Any,
                    Collection,
                    Dict,
                    Generic,
                    TypeVar)

from aiohttp import (ClientError,
                     ClientSession,
                     hdrs,
                     web_ws)
from reprit.base import generate_repr

from .configuration import AnyClusterConfiguration
from .hints import NodeId
from .utils import (Error,
                    Ok,
                    Result)

_T = TypeVar('_T')


class Communication(Generic[_T]):
    HTTP_METHOD = hdrs.METH_PATCH
    assert HTTP_METHOD is not hdrs.METH_POST

    def __init__(self,
                 configuration: AnyClusterConfiguration,
                 paths: Collection[_T]) -> None:
        self.configuration = configuration
        self._paths = paths
        self._latencies: Dict[NodeId, deque] = {
            node_id: deque([0],
                           maxlen=10)
            for node_id in self.configuration.nodes_ids}
        self._loop = asyncio.get_event_loop()
        self._messages = {node_id: asyncio.Queue()
                          for node_id in self.configuration.nodes_ids}
        self._results = {node_id: {path: asyncio.Queue()
                                   for path in self.paths}
                         for node_id in self.configuration.nodes_ids}
        self._session = ClientSession(loop=self._loop)
        self._channels = {
            node_id: self._loop.create_task(self._channel(node_id))
            for node_id in self.configuration.nodes_ids
        }

    __repr__ = generate_repr(__init__)

    @property
    def paths(self) -> Collection[_T]:
        return self._paths

    async def send(self, receiver: NodeId, path: _T, message: Any) -> Any:
        assert path in self.paths
        self._messages[receiver].put_nowait((path, message))
        result: Result = await self._results[receiver][path].get()
        return result.value

    def connect(self, node_id: NodeId) -> None:
        self._latencies[node_id] = deque([0],
                                         maxlen=10)
        self._messages[node_id] = asyncio.Queue()
        self._results[node_id] = {path: asyncio.Queue() for path in self.paths}
        self._channels[node_id] = self._loop.create_task(
                self._channel(node_id)
        )

    def disconnect(self, node_id: NodeId) -> None:
        self._channels.pop(node_id).cancel()
        del (self._messages[node_id],
             self._latencies[node_id],
             self._results[node_id])

    def to_expected_broadcast_time(self) -> float:
        return sum(max(latencies) for latencies in self._latencies.values())

    async def _channel(self, receiver: NodeId) -> None:
        receiver_url = self.configuration.nodes_urls[receiver]
        messages, results, latencies = (self._messages[receiver],
                                        self._results[receiver],
                                        self._latencies[receiver])
        path, message = await messages.get()
        to_time = self._loop.time
        while True:
            try:
                async with self._session.ws_connect(
                        receiver_url,
                        method=self.HTTP_METHOD,
                        timeout=self.configuration.heartbeat,
                        heartbeat=self.configuration.heartbeat) as connection:
                    message_start = to_time()
                    await connection.send_json({'path': path,
                                                'message': message})
                    async for reply in connection:
                        reply: web_ws.WSMessage
                        reply_end = to_time()
                        latency = reply_end - message_start
                        latencies.append(latency)
                        results[path].put_nowait(Ok(reply.json()))
                        path, message = await messages.get()
                        message_start = to_time()
                        await connection.send_json({'path': path,
                                                    'message': message})
            except (ClientError, OSError) as exception:
                results[path].put_nowait(Error(exception))
                path, message = await messages.get()


def update_communication_configuration(communication: Communication,
                                       configuration: AnyClusterConfiguration
                                       ) -> None:
    new_nodes_ids, old_nodes_ids = (set(configuration.nodes_ids),
                                    set(communication.configuration.nodes_ids))
    for removed_node_id in old_nodes_ids - new_nodes_ids:
        communication.disconnect(removed_node_id)
    for added_node_id in new_nodes_ids - old_nodes_ids:
        communication.connect(added_node_id)
    communication.configuration = configuration
