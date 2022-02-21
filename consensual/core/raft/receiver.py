from abc import (ABC,
                 abstractmethod)


class Receiver(ABC):
    __slots__ = ()

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Checks whether receiver is running."""

    @abstractmethod
    def start(self) -> None:
        """Starts receiving messages."""

    @abstractmethod
    def stop(self) -> None:
        """Stops receiving messages."""
