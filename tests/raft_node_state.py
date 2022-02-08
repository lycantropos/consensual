from typing import (Any,
                    Dict,
                    List,
                    Optional)

from reprit.base import generate_repr

from consensual.raft import (NodeId,
                             Record,
                             Role,
                             Term)


class RaftNodeState:
    def __init__(self,
                 _id: NodeId,
                 *,
                 commit_length: int,
                 leader_node_id: Optional[NodeId],
                 log: List[Record],
                 role: Role,
                 supporters_nodes_ids: List[NodeId],
                 supported_node_id: Optional[NodeId],
                 term: Term) -> None:
        self._id = _id
        self.commit_length = commit_length
        self.leader_node_id = leader_node_id
        self.log = log
        self.role = role
        self.supporters_nodes_ids = supporters_nodes_ids
        self.supported_node_id = supported_node_id
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
                  role: int,
                  supporters_nodes_ids: List[NodeId],
                  supported_node_id: Optional[NodeId],
                  term) -> 'RaftNodeState':
        return cls(id_,
                   commit_length=commit_length,
                   leader_node_id=leader_node_id,
                   log=[Record.from_json(**record) for record in log],
                   role=Role(role),
                   supported_node_id=supported_node_id,
                   supporters_nodes_ids=supporters_nodes_ids,
                   term=term)
