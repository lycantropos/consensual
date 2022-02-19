import asyncio
from collections import deque
from typing import (Any,
                    Collection,
                    Dict,
                    Generic,
                    Mapping,
                    TypeVar)

from aiohttp import (ClientError,
                     ClientSession,
                     hdrs,
                     web_ws)
from reprit.base import generate_repr
from yarl import URL

from .hints import Time
from .result import (Error,
                     Ok,
                     Result)

_Receiver = TypeVar('_Receiver')
_Path = TypeVar('_Path')


class ReceiverNotFound(Exception):
    pass


class Communication(Generic[_Receiver, _Path]):
    HTTP_METHOD = hdrs.METH_PATCH
    assert (HTTP_METHOD is not hdrs.METH_POST
            and HTTP_METHOD is not hdrs.METH_DELETE)

    def __init__(self,
                 *,
                 heartbeat: Time,
                 paths: Collection[_Path],
                 registry: Mapping[_Receiver, URL]) -> None:
        self._heartbeat, self._paths, self.registry = (heartbeat, paths,
                                                       registry)
        self._latencies: Dict[_Receiver, deque] = {
            receiver: deque([0],
                            maxlen=10)
            for receiver in self.registry.keys()
        }
        self._loop = asyncio.get_event_loop()
        self._messages: Dict[_Receiver, asyncio.Queue] = {
            receiver: asyncio.Queue()
            for receiver in self.registry.keys()
        }
        self._results: Dict[_Receiver, Dict[_Path, asyncio.Queue]] = {
            receiver: {path: asyncio.Queue()
                       for path in self.paths}
            for receiver in self.registry.keys()
        }
        self._session = ClientSession(loop=self._loop)
        self._channels = {
            receiver: self._loop.create_task(self._channel(receiver))
            for receiver in self.registry.keys()
        }

    __repr__ = generate_repr(__init__)

    @property
    def heartbeat(self) -> Time:
        return self._heartbeat

    @property
    def paths(self) -> Collection[_Path]:
        return self._paths

    async def send(self,
                   receiver: _Receiver,
                   path: _Path,
                   message: Any) -> Any:
        assert path in self.paths
        try:
            messages = self._messages[receiver]
        except KeyError:
            raise ReceiverNotFound(receiver)
        messages.put_nowait((path, message))
        result: Result = await self._results[receiver][path].get()
        return result.value

    def connect(self, receiver: _Receiver) -> None:
        self._latencies[receiver] = deque([0],
                                          maxlen=10)
        self._messages[receiver] = asyncio.Queue()
        self._results[receiver] = {path: asyncio.Queue()
                                   for path in self.paths}
        self._channels[receiver] = self._loop.create_task(
                self._channel(receiver)
        )

    def disconnect(self, receiver: _Receiver) -> None:
        self._channels.pop(receiver).cancel()
        del (self._messages[receiver],
             self._latencies[receiver],
             self._results[receiver])

    def to_expected_broadcast_time(self) -> float:
        return sum(max(latencies) for latencies in self._latencies.values())

    async def _channel(self, receiver: _Receiver) -> None:
        receiver_url = self.registry[receiver]
        messages, results, latencies = (self._messages[receiver],
                                        self._results[receiver],
                                        self._latencies[receiver])
        path, message = await messages.get()
        to_time = self._loop.time
        while True:
            try:
                async with self._session.ws_connect(
                        receiver_url,
                        heartbeat=self.heartbeat,
                        method=self.HTTP_METHOD,
                        timeout=self.heartbeat
                ) as connection:
                    message_start = to_time()
                    try:
                        await connection.send_json({'path': path,
                                                    'message': message})
                    except (ClientError, OSError):
                        continue
                    async for reply in connection:
                        reply: web_ws.WSMessage
                        reply_end = to_time()
                        latency = reply_end - message_start
                        latencies.append(latency)
                        results[path].put_nowait(Ok(reply.json()))
                        path, message = await messages.get()
                        message_start = to_time()
                        try:
                            await connection.send_json({'path': path,
                                                        'message': message})
                        except (ClientError, OSError):
                            continue
            except (ClientError, OSError) as exception:
                results[path].put_nowait(Error(exception))
                path, message = await messages.get()


def update_communication_registry(communication: Communication,
                                  registry: Mapping[_Receiver, URL]) -> None:
    new_receivers, old_receivers = (registry.keys(),
                                    communication.registry.keys())
    for removed_receiver in old_receivers - new_receivers:
        communication.disconnect(removed_receiver)
    communication.registry = registry
    for added_receiver in new_receivers - old_receivers:
        communication.connect(added_receiver)
