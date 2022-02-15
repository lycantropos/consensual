from typing import List

from reprit.base import generate_repr

from consensual.core.raft.cluster_id import (ClusterId,
                                             RawClusterId)
from consensual.core.raft.hints import NodeId


class RaftClusterState:
    def __init__(self,
                 _id: ClusterId,
                 *,
                 heartbeat: float,
                 nodes_ids: List[NodeId],
                 stable: bool) -> None:
        self.heartbeat, self._id, self.nodes_ids, self.stable = (
            heartbeat, _id, nodes_ids, stable
        )

    __repr__ = generate_repr(__init__)

    @property
    def id(self) -> ClusterId:
        return self._id

    @classmethod
    def from_json(cls,
                  id_: RawClusterId,
                  *,
                  heartbeat: float,
                  nodes_ids: List[NodeId],
                  stable: bool) -> 'RaftClusterState':
        return cls(ClusterId.from_json(id_),
                   heartbeat=heartbeat,
                   nodes_ids=nodes_ids,
                   stable=stable)
