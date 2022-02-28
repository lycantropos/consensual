from typing import (Any,
                    Collection,
                    Dict,
                    Mapping,
                    Union)

from reprit.base import generate_repr
from yarl import URL

from .cluster_id import (ClusterId,
                         RawClusterId)
from .hints import (NodeId,
                    Time)


class DisjointCluster:
    __slots__ = '_heartbeat', '_id', '_nodes_urls', '_stable'

    def __new__(cls,
                _id: ClusterId,
                *,
                heartbeat: Time,
                nodes_urls: Mapping[NodeId, URL],
                stable: bool) -> 'DisjointCluster':
        if heartbeat < 0:
            raise ValueError('heartbeat should be non-negative')
        self = super().__new__(cls)
        self._heartbeat, self._id, self._nodes_urls, self._stable = (
            heartbeat, _id, nodes_urls, stable
        )
        return self

    __repr__ = generate_repr(__new__)

    def __eq__(self, other: Any) -> Any:
        return ((self.id == other.id
                 and self.heartbeat == other.heartbeat
                 and self.nodes_urls == other.nodes_urls
                 and self.stable is other.stable)
                if isinstance(other, DisjointCluster)
                else NotImplemented)

    @property
    def heartbeat(self) -> Time:
        return self._heartbeat

    @property
    def id(self) -> ClusterId:
        return self._id

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.nodes_urls.keys()

    @property
    def nodes_urls(self) -> Mapping[NodeId, URL]:
        return self._nodes_urls

    @property
    def stable(self) -> bool:
        return self._stable

    @classmethod
    def from_json(cls,
                  *,
                  heartbeat: int,
                  id_: RawClusterId,
                  nodes_urls: Dict[NodeId, str],
                  stable: bool) -> 'DisjointCluster':
        return cls(ClusterId.from_json(id_),
                   heartbeat=heartbeat,
                   nodes_urls={
                       node_id: URL(raw_node_url)
                       for node_id, raw_node_url in nodes_urls.items()
                   },
                   stable=stable)

    def as_json(self) -> Dict[str, Any]:
        return {'heartbeat': self.heartbeat,
                'id_': self.id.as_json(),
                'nodes_urls': {
                    node_id: str(node_url)
                    for node_id, node_url in self.nodes_urls.items()
                },
                'stable': self.stable}

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return (len(frozenset(nodes_ids) & frozenset(self.nodes_ids))
                >= ceil_division(len(self.nodes_ids) + 1, 2))

    def stabilize(self) -> 'DisjointCluster':
        assert not self.stable
        return DisjointCluster(self.id,
                               heartbeat=self.heartbeat,
                               nodes_urls=self.nodes_urls,
                               stable=True)


class JointCluster:
    __slots__ = '_id', '_new', '_old'

    def __new__(cls,
                *,
                old: DisjointCluster,
                new: DisjointCluster) -> 'JointCluster':
        self = super().__new__(cls)
        self._id, self._new, self._old = old.id.join_with(new.id), new, old
        return self

    __repr__ = generate_repr(__new__)

    def __eq__(self, other: Any) -> Any:
        return (self.old == other.old and self.new == other.new
                if isinstance(other, JointCluster)
                else NotImplemented)

    @property
    def heartbeat(self) -> Time:
        return self.new.heartbeat

    @property
    def id(self) -> ClusterId:
        return self._id

    @property
    def new(self) -> DisjointCluster:
        return self._new

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.old.nodes_urls.keys() | self.new.nodes_urls.keys()

    @property
    def nodes_urls(self) -> Mapping[NodeId, URL]:
        return {**self.old.nodes_urls, **self.new.nodes_urls}

    @property
    def old(self) -> DisjointCluster:
        return self._old

    @property
    def stable(self) -> bool:
        return False

    @classmethod
    def from_json(cls,
                  *,
                  old: Dict[str, Any],
                  new: Dict[str, Any]) -> 'JointCluster':
        return cls(old=DisjointCluster.from_json(**old),
                   new=DisjointCluster.from_json(**new))

    def as_json(self) -> Dict[str, Any]:
        return {'old': self.old.as_json(), 'new': self.new.as_json()}

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return (self.old.has_majority(nodes_ids)
                and self.new.has_majority(nodes_ids))


Cluster = Union[DisjointCluster, JointCluster]


def ceil_division(dividend: int, divisor: int) -> int:
    return -((-dividend) // divisor)
