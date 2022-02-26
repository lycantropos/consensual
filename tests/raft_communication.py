from asyncio import sleep
from typing import (Any,
                    Callable,
                    Collection,
                    MutableMapping)

from reprit.base import generate_repr
from yarl import URL

from consensual.raft import (MessageKind,
                             Node,
                             Receiver,
                             Sender,
                             plain)


class SenderWithSimulatedLatency(plain.Sender):
    def __init__(self,
                 *args: Any,
                 random_latency_generator: Callable[[], float],
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.random_latency_generator = random_latency_generator

    async def send(self, *, kind: MessageKind, message: Any, url: URL) -> Any:
        await sleep(self.random_latency_generator())
        result = super().send(kind=kind,
                              message=message,
                              url=url)
        await sleep(self.random_latency_generator())
        return result


class RaftCommunication:
    def __init__(self, nodes: MutableMapping[str, Node]) -> None:
        self.nodes = nodes

    __repr__ = generate_repr(__init__)

    def to_receiver(self, node: Node) -> Receiver:
        return plain.Receiver(node, self.nodes)

    def to_sender(self,
                  urls: Collection[URL],
                  random_latency_generator: Callable[[], float]) -> Sender:
        return SenderWithSimulatedLatency(
                urls, self.nodes,
                random_latency_generator=random_latency_generator
        )
