from typing import (Any,
                    Dict,
                    List,
                    Optional)

from reprit.base import generate_repr

from consensual.core.raft.command import Command
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
                 processed_external_commands: List[Command],
                 processed_internal_commands: List[Command],
                 role_kind: RoleKind,
                 term: Term) -> None:
        self._id = _id
        self.commit_length = commit_length
        self.leader_node_id = leader_node_id
        self.log = log
        self.processed_external_commands = processed_external_commands
        self.processed_internal_commands = processed_internal_commands
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
                  processed_external_commands: List[Dict[str, Any]],
                  processed_internal_commands: List[Dict[str, Any]],
                  role_kind: int,
                  term: Term) -> 'RaftNodeState':
        return cls(id_,
                   commit_length=commit_length,
                   leader_node_id=leader_node_id,
                   log=[Record.from_json(**record) for record in log],
                   processed_external_commands=[
                       Command.from_json(**command)
                       for command in processed_external_commands
                   ],
                   processed_internal_commands=[
                       Command.from_json(**command)
                       for command in processed_internal_commands
                   ],
                   role_kind=RoleKind(role_kind),
                   term=term)
