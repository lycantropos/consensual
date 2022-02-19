from abc import (ABC,
                 abstractmethod)
from typing import (Any,
                    Collection,
                    Generic,
                    TypeVar)

_Receiver = TypeVar('_Receiver')
_Key = TypeVar('_Key')


class ReceiverUnavailable(Exception):
    pass


class Sender(ABC, Generic[_Receiver, _Key]):
    receivers: Collection[_Receiver]

    @abstractmethod
    async def send_json(self,
                        receiver: _Receiver,
                        key: _Key,
                        message: Any) -> Any:
        """
        Sends JSON-serializable message to a receiver by given key
        or raises ``ReceiverUnavailable`` exception in case of failure.
        """
