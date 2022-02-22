from typing import (Collection,
                    MutableMapping)

from reprit.base import generate_repr
from yarl import URL

from consensual.raft import (Node,
                             Receiver,
                             Sender,
                             plain)


class RaftCommunication:
    def __init__(self, nodes: MutableMapping[str, Node]) -> None:
        self.nodes = nodes

    __repr__ = generate_repr(__init__)

    def to_receiver(self, node: Node) -> Receiver:
        return plain.Receiver(node, self.nodes)

    def to_sender(self, urls: Collection[URL]) -> Sender:
        return plain.Sender(urls, self.nodes)
