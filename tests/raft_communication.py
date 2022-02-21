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
    def __init__(self, node: Node, nodes: MutableMapping[str, Node]) -> None:
        self.node, self.nodes = node, nodes
        self._is_running = False

    __repr__ = generate_repr(__init__)

    @property
    def is_running(self) -> bool:
        return self._is_running

    def start(self) -> None:
        if self._is_running:
            return
        result = self.nodes.setdefault(self.node.url.authority,
                                       self.node)
        if result is not self.node:
            raise OSError()
        else:
            self._is_running = True

    def stop(self) -> None:
        if self.is_running:
            del self.nodes[self.node.url.authority]
            self._is_running = False


class RaftSender(Sender):
    def __init__(self, urls: Collection[URL], nodes: Mapping[str, Node]
                 ) -> None:
        self.nodes, self.urls = nodes, urls

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
        return RaftReceiver(node, self.nodes)

    def to_sender(self, urls: Collection[URL]) -> Sender:
        return RaftSender(urls, self.nodes)
