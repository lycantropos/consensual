from abc import (ABC,
                 abstractmethod)
from typing import (Any,
                    Collection)

from yarl import URL

from .messages import MessageKind


class ReceiverUnavailable(Exception):
    pass


class Sender(ABC):
    __slots__ = ()

    urls: Collection[URL]

    @abstractmethod
    async def send(self, *, kind: MessageKind, message: Any, url: URL) -> Any:
        """
        Sends given message of given kind to given URL
        or raises ``ReceiverUnavailable`` exception in case of failure.
        """
