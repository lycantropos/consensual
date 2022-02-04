from typing import List

from reprit.base import generate_repr

from consensual.raft import (ClusterId,
                             NodeId)


class RunningClusterState:
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
