from typing import (Any,
                    Dict,
                    List,
                    Optional)

from reprit.base import generate_repr

from consensual.core.raft.hints import (NodeId,
                                        Term)
from consensual.core.raft.record import Record
from consensual.core.raft.role import RoleKind


class RaftNodeState:
    def __init__(self,
                 _id: NodeId,
                 *,
                 commit_length: int,
                 leader_node_id: Optional[NodeId],
                 log: List[Record],
                 role_kind: RoleKind,
                 term: Term) -> None:
        self._id = _id
        self.commit_length = commit_length
        self.leader_node_id = leader_node_id
        self.log = log
        self.role_kind = role_kind
        self.term = term

    __repr__ = generate_repr(__init__)

    @property
    def id(self) -> NodeId:
        return self._id

    @classmethod
    def from_json(cls,
                  id_: NodeId,
                  *,
                  commit_length: int,
                  leader_node_id: Optional[NodeId],
                  log: List[Dict[str, Any]],
                  role_kind: int,
                  term) -> 'RaftNodeState':
        return cls(id_,
                   commit_length=commit_length,
                   leader_node_id=leader_node_id,
                   log=[Record.from_json(**record) for record in log],
                   role_kind=RoleKind(role_kind),
                   term=term)
