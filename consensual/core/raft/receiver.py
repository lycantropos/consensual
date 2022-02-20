from abc import (ABC,
                 abstractmethod)

from .node import Node


class Receiver(ABC):
    @classmethod
    @abstractmethod
    def from_node(cls, node: Node) -> 'Receiver':
        """Constructs receiver from given node."""

    @abstractmethod
    def start(self) -> None:
        """Starts receiving messages."""
