from asyncio import (Queue,
                     get_event_loop)
from typing import (Any,
                    Collection,
                    Dict,
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
from .sender import (ReceiverUnavailable,
                     Sender)

_Key = TypeVar('_Key')


class AiohttpSender(Sender[URL, _Key]):
    HTTP_METHOD = hdrs.METH_PATCH
    assert (HTTP_METHOD is not hdrs.METH_POST
            and HTTP_METHOD is not hdrs.METH_DELETE)

    def __init__(self,
                 *,
                 heartbeat: Time,
                 keys: Collection[_Key],
                 receivers: Collection[URL]) -> None:
        self._heartbeat, self._keys, self._receivers = (
            heartbeat, keys, receivers
        )
        self._loop = get_event_loop()
        self._messages: Dict[URL, Queue] = {receiver: Queue()
                                            for receiver in self._receivers}
        self._results: Dict[URL, Dict[_Key, Queue]] = {
            receiver: {key: Queue() for key in self.keys}
            for receiver in self._receivers
        }
        self._session = ClientSession(loop=self._loop)
        self._channels = {
            receiver: self._loop.create_task(self._channel(receiver))
            for receiver in self._receivers
        }

    __repr__ = generate_repr(__init__)

    @property
    def heartbeat(self) -> Time:
        return self._heartbeat

    @property
    def keys(self) -> Collection[_Key]:
        return self._keys

    @property
    def receivers(self) -> Collection[URL]:
        return self._receivers

    @receivers.setter
    def receivers(self, value: Collection[URL]) -> None:
        new_receivers, old_receivers = set(value), set(self._receivers)
        for removed_receiver in old_receivers - new_receivers:
            self._disconnect(removed_receiver)
        self._receivers = value
        for added_receiver in new_receivers - old_receivers:
            self._connect(added_receiver)

    async def send_json(self, receiver: URL, key: _Key, message: Any) -> Any:
        assert key in self.keys, key
        try:
            messages = self._messages[receiver]
        except KeyError:
            raise ReceiverUnavailable(receiver)
        messages.put_nowait((key, message))
        result: Result = await self._results[receiver][key].get()
        try:
            return result.value
        except (ClientError, OSError):
            raise ReceiverUnavailable(receiver)

    async def _channel(self, receiver: URL) -> None:
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
                    except (ClientError, OSError):
                        continue
                    async for reply in connection:
                        reply: web_ws.WSMessage
                        results[key].put_nowait(Ok(reply.json()))
                        key, message = await messages.get()
                        try:
                            await connection.send_json({'key': key,
                                                        'message': message})
                        except (ClientError, OSError):
                            continue
            except (ClientError, OSError) as exception:
                results[key].put_nowait(Error(exception))
                key, message = await messages.get()

    def _connect(self, receiver: URL) -> None:
        self._messages[receiver] = Queue()
        self._results[receiver] = {key: Queue() for key in self.keys}
        self._channels[receiver] = self._loop.create_task(
                self._channel(receiver)
        )

    def _disconnect(self, receiver: URL) -> None:
        self._channels.pop(receiver).cancel()
        del self._messages[receiver], self._results[receiver]
