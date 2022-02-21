from abc import (ABC,
                 abstractmethod)

from .node import Node


class Receiver(ABC):
    @classmethod
    @abstractmethod
    def from_node(cls, node: Node) -> 'Receiver':
        """Constructs receiver from given node."""

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
