from typing import (List,
                    Optional)

from reprit.base import generate_repr

from consensual.raft import (NodeId,
                             Record,
                             Role,
                             Term)


class RunningNodeState:
    def __init__(self,
                 _id: NodeId,
                 *,
                 commit_length: int,
                 leader_node_id: Optional[NodeId],
                 log: List[Record],
                 role: Role,
                 supported_node_id: Optional[NodeId],
                 term: Term) -> None:
        self._id = _id
        self.commit_length = commit_length
        self.leader_node_id = leader_node_id
        self.log = log
        self.role = role
        self.supported_node_id = supported_node_id
        self.term = term

    __repr__ = generate_repr(__init__)

    @property
    def id(self) -> NodeId:
        return self._id
