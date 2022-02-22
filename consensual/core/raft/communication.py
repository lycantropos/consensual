from typing import (Any as _Any,
                    Collection as _Collection,
                    Mapping as _Mapping,
                    MutableMapping as _MutableMapping)

from reprit.base import generate_repr as _generate_repr
from yarl import URL as _URL

from .messages import MessageKind as _MessageKind
from .node import Node as _Node
from .receiver import Receiver as _Receiver
from .sender import (ReceiverUnavailable as _ReceiverUnavailable,
                     Sender as _Sender)


class Receiver(_Receiver):
    __slots__ = '_is_running', '_node', '_nodes'

    def __init__(self, _node: _Node, _nodes: _MutableMapping[str, _Node]
                 ) -> None:
        self._node, self._nodes = _node, _nodes
        self._is_running = False

    __repr__ = _generate_repr(__init__)

    @property
    def is_running(self) -> bool:
        return self._is_running

    def start(self) -> None:
        if self.is_running:
            raise RuntimeError('is already running')
        result = self._nodes.setdefault(self._node.url.authority, self._node)
        if result is not self._node:
            raise OSError('already exists')
        else:
            self._is_running = True

    def stop(self) -> None:
        if self.is_running:
            del self._nodes[self._node.url.authority]
            self._is_running = False


class Sender(_Sender):
    __slots__ = 'urls', '_nodes'

    def __init__(self, urls: _Collection[_URL], _nodes: _Mapping[str, _Node]
                 ) -> None:
        self._nodes, self.urls = _nodes, urls

    __repr__ = _generate_repr(__init__)

    async def send(self, *, kind: _MessageKind, message: _Any, url: _URL
                   ) -> _Any:
        if url not in self.urls:
            raise _ReceiverUnavailable(url)
        try:
            receiver_node = self._nodes[url.authority]
        except KeyError as exception:
            raise _ReceiverUnavailable(url) from exception
        return await receiver_node.receive(kind=kind,
                                           message=message)
