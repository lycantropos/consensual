from typing import (Any,
                    Collection,
                    Mapping,
                    MutableMapping)

from reprit.base import generate_repr
from yarl import URL

from consensual.raft import (MessageKind,
                             Node,
                             Receiver,
                             ReceiverUnavailable,
                             Sender)


class RaftReceiver(Receiver):
    nodes: MutableMapping[str, Node]

    def __init__(self, node: Node) -> None:
        self.node = node

    __repr__ = generate_repr(__init__)

    @classmethod
    def from_node(cls, node: Node) -> 'RaftReceiver':
        return cls(node)

    def start(self) -> None:
        result = self.nodes.setdefault(self.node.url.authority,
                                       self.node)
        if result is not self.node:
            raise OSError()


class RaftSender(Sender):
    nodes: Mapping[str, Node]

    def __init__(self, urls: Collection[URL]) -> None:
        self.urls = urls

    __repr__ = generate_repr(__init__)

    async def send(self, *, kind: MessageKind, message: Any, url: URL
                   ) -> Any:
        if url not in self.urls:
            raise ReceiverUnavailable(url)
        try:
            receiver = self.nodes[url.authority]
        except KeyError as exception:
            raise ReceiverUnavailable(url) from exception
        return await receiver.receive(kind=kind,
                                      message=message)


class RaftCommunication:
    def __init__(self, nodes: MutableMapping[str, Node]) -> None:
        self.nodes = nodes

    __repr__ = generate_repr(__init__)

    def to_receiver(self, node: Node) -> Receiver:
        result = RaftReceiver.from_node(node)
        result.nodes = self.nodes
        return result

    def to_sender(self, urls: Collection[URL]) -> Sender:
        result = RaftSender(urls)
        result.nodes = self.nodes
        return result