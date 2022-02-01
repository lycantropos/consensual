from typing import (Any,
                    Collection,
                    Dict,
                    List,
                    Mapping,
                    Union)

from reprit.base import generate_repr
from yarl import URL

from .hints import Time
from .node_state import NodeId

RawClusterId = List[str]


class ClusterId:
    __slots__ = '_variants',

    def __new__(cls, *_variants: str) -> 'ClusterId':
        self = super().__new__(cls)
        self._variants = frozenset(_variants)
        return self

    __repr__ = generate_repr(__new__)

    def __bool__(self) -> bool:
        return bool(self._variants)

    def __eq__(self, other: 'ClusterId') -> Any:
        return (self._variants == other._variants
                if isinstance(other, ClusterId)
                else NotImplemented)

    @classmethod
    def from_json(cls, _raw: RawClusterId) -> 'ClusterId':
        return cls(*_raw)

    def agrees_with(self, other: 'ClusterId') -> bool:
        return not self._variants.isdisjoint(other._variants)

    def as_json(self) -> RawClusterId:
        return list(self._variants)

    def join_with(self, other: 'ClusterId') -> 'ClusterId':
        assert self._variants.isdisjoint(other._variants)
        return ClusterId(*self._variants, *other._variants)


class StableClusterState:
    stable = True

    def __init__(self,
                 _id: ClusterId,
                 *,
                 heartbeat: Time,
                 nodes_urls: Mapping[NodeId, URL]) -> None:
        self._heartbeat, self._id, self._nodes_urls = (heartbeat, _id,
                                                       nodes_urls)

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: Any) -> Any:
        return ((self.heartbeat == other.heartbeat
                 and self.nodes_urls == other.nodes_urls)
                if isinstance(other, StableClusterState)
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

    @classmethod
    def from_json(cls,
                  *,
                  heartbeat: int,
                  id_: RawClusterId,
                  nodes_urls: Dict[NodeId, str]) -> 'StableClusterState':
        return cls(
                ClusterId.from_json(id_),
                heartbeat=heartbeat,
                nodes_urls={node_id: URL(raw_node_url)
                            for node_id, raw_node_url in nodes_urls.items()},
        )

    def as_json(self) -> Dict[str, Any]:
        return {
            'heartbeat': self.heartbeat,
            'id_': self.id.as_json(),
            'nodes_urls': {node_id: str(node_url)
                           for node_id, node_url in self.nodes_urls.items()},
        }

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return len(nodes_ids) >= ceil_division(len(self.nodes_ids) + 1, 2)


class TransitionalClusterState:
    stable = False

    def __init__(self,
                 *,
                 old: StableClusterState,
                 new: StableClusterState) -> None:
        self._id, self._new, self._old = old.id.join_with(new.id), new, old

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: Any) -> Any:
        return (self.old == other.old and self.new == other.new
                if isinstance(other, TransitionalClusterState)
                else NotImplemented)

    @property
    def heartbeat(self) -> Time:
        return self.new.heartbeat

    @property
    def id(self) -> ClusterId:
        return self._id

    @property
    def new(self) -> StableClusterState:
        return self._new

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.old.nodes_urls.keys() | self.new.nodes_urls.keys()

    @property
    def nodes_urls(self) -> Mapping[NodeId, URL]:
        return {**self.old.nodes_urls, **self.new.nodes_urls}

    @property
    def old(self) -> StableClusterState:
        return self._old

    @classmethod
    def from_json(cls,
                  *,
                  old: Dict[str, Any],
                  new: Dict[str, Any]) -> 'TransitionalClusterState':
        return cls(old=StableClusterState.from_json(**old),
                   new=StableClusterState.from_json(**new))

    def as_json(self) -> Dict[str, Any]:
        return {'old': self.old.as_json(), 'new': self.new.as_json()}

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return (self.old.has_majority(nodes_ids)
                and self.new.has_majority(nodes_ids))


AnyClusterState = Union[StableClusterState, TransitionalClusterState]


def ceil_division(dividend: int, divisor: int) -> int:
    return -((-dividend) // divisor)
