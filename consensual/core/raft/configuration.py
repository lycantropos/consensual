from typing import (Any,
                    Collection,
                    Dict,
                    Mapping,
                    Optional,
                    Union)

from reprit.base import generate_repr
from yarl import URL

from .hints import (NodeId,
                    Time)


class StableClusterConfiguration:
    def __init__(self,
                 nodes_urls: Mapping[NodeId, URL],
                 *,
                 active_nodes_ids: Optional[Collection[NodeId]] = None,
                 heartbeat: Time = 5) -> None:
        self._heartbeat = heartbeat
        self._nodes_urls = nodes_urls
        self._active_nodes_ids = (set(nodes_urls.keys())
                                  if active_nodes_ids is None
                                  else set(active_nodes_ids))

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: Any) -> Any:
        return ((self.heartbeat == other.heartbeat
                 and self.nodes_urls == other.nodes_urls
                 and self.active_nodes_ids == other.active_nodes_ids)
                if isinstance(other, StableClusterConfiguration)
                else NotImplemented)

    @property
    def active_nodes_ids(self) -> Collection[NodeId]:
        return self._active_nodes_ids

    @property
    def heartbeat(self) -> Time:
        return self._heartbeat

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.nodes_urls.keys()

    @property
    def nodes_urls(self) -> Mapping[NodeId, URL]:
        return self._nodes_urls

    @classmethod
    def from_json(cls,
                  nodes_urls: Dict[NodeId, str],
                  **kwargs: Any) -> 'StableClusterConfiguration':
        return cls({node_id: URL(raw_node_url)
                    for node_id, raw_node_url in nodes_urls.items()},
                   **kwargs)

    def as_json(self) -> Dict[str, Any]:
        return {
            'nodes_urls': {node_id: str(node_url)
                           for node_id, node_url in self.nodes_urls.items()},
            'active_nodes_ids': list(self.active_nodes_ids),
            'heartbeat': self.heartbeat,
        }

    def activate(self, node_id: NodeId) -> None:
        assert node_id in self.nodes_ids
        assert node_id not in self.active_nodes_ids
        self._active_nodes_ids.add(node_id)

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return len(nodes_ids) >= ceil_division(len(self.nodes_ids) + 1, 2)


class TransitionalClusterConfiguration:
    def __init__(self,
                 old: StableClusterConfiguration,
                 new: StableClusterConfiguration) -> None:
        self._new, self._old = new, old

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: Any) -> Any:
        return (self.old == other.old and self.new == other.new
                if isinstance(other, TransitionalClusterConfiguration)
                else NotImplemented)

    @property
    def active_nodes_ids(self) -> Collection[NodeId]:
        return set(self.old.active_nodes_ids) | set(self.new.active_nodes_ids)

    @property
    def heartbeat(self) -> Time:
        return self.new.heartbeat

    @property
    def new(self) -> StableClusterConfiguration:
        return self._new

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.old.nodes_urls.keys() | self.new.nodes_urls.keys()

    @property
    def nodes_urls(self) -> Mapping[NodeId, URL]:
        return {**self.old.nodes_urls, **self.new.nodes_urls}

    @property
    def old(self) -> StableClusterConfiguration:
        return self._old

    @classmethod
    def from_json(cls,
                  *,
                  old: Dict[str, Any],
                  new: Dict[str, Any]) -> 'TransitionalClusterConfiguration':
        return cls(old=StableClusterConfiguration.from_json(**old),
                   new=StableClusterConfiguration.from_json(**new))

    def activate(self, node_id: NodeId) -> None:
        self.new.activate(node_id)

    def as_json(self) -> Dict[str, Any]:
        return {'old': self.old.as_json(), 'new': self.new.as_json()}

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return (self.old.has_majority(nodes_ids)
                and self.new.has_majority(nodes_ids))


AnyClusterConfiguration = Union[StableClusterConfiguration,
                                TransitionalClusterConfiguration]


def ceil_division(dividend: int, divisor: int) -> int:
    return -((-dividend) // divisor)
