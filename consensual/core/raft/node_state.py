import enum
from typing import (Collection,
                    List,
                    MutableMapping,
                    MutableSet,
                    Optional)

from reprit.base import generate_repr

from .hints import Term
from .record import Record

NodeId = str


class Role(enum.IntEnum):
    CANDIDATE = 0
    FOLLOWER = 1
    LEADER = 2


class NodeState:
    def __init__(self,
                 _id: NodeId,
                 *,
                 nodes_ids: Optional[Collection[NodeId]] = None,
                 log: Optional[List[Record]] = None,
                 supported_node_id: Optional[NodeId] = None,
                 term: Term = 0) -> None:
        self._id = _id
        self.nodes_ids = {self.id} if nodes_ids is None else nodes_ids
        self._accepted_lengths = {node_id: 0 for node_id in self.nodes_ids}
        self._sent_lengths = {node_id: 0 for node_id in self.nodes_ids}
        self._commit_length = 0
        self._leader_node_id = None
        self._log = [] if log is None else log
        self._rejectors_nodes_ids = set()
        self._role = Role.FOLLOWER
        self._supported_node_id = supported_node_id
        self._supporters_nodes_ids = set()
        self._term = term

    __repr__ = generate_repr(__init__)

    @property
    def accepted_lengths(self) -> MutableMapping[NodeId, int]:
        return self._accepted_lengths

    @property
    def commit_length(self) -> int:
        return self._commit_length

    @commit_length.setter
    def commit_length(self, value: int) -> None:
        assert value >= 0
        assert self.commit_length < value
        self._commit_length = value

    @property
    def id(self) -> NodeId:
        return self._id

    @property
    def leader_node_id(self) -> Optional[NodeId]:
        return self._leader_node_id

    @leader_node_id.setter
    def leader_node_id(self, value: Optional[NodeId]) -> None:
        self._leader_node_id = value

    @property
    def log(self) -> List[Record]:
        return self._log

    @property
    def log_term(self) -> Term:
        return self.log[-1].term if self.log else 0

    @property
    def rejectors_nodes_ids(self) -> MutableSet[NodeId]:
        return self._rejectors_nodes_ids

    @property
    def role(self) -> Role:
        return self._role

    @role.setter
    def role(self, value: Role) -> None:
        assert value in Role
        self._role = value

    @property
    def sent_lengths(self) -> MutableMapping[NodeId, int]:
        return self._sent_lengths

    @property
    def supported_node_id(self) -> Optional[NodeId]:
        return self._supported_node_id

    @supported_node_id.setter
    def supported_node_id(self, value: Optional[NodeId]) -> None:
        assert value is None or value in self.nodes_ids
        self._supported_node_id = value

    @property
    def supporters_nodes_ids(self) -> MutableSet[NodeId]:
        return self._supporters_nodes_ids

    @property
    def term(self) -> Term:
        return self._term

    @term.setter
    def term(self, value: Term) -> None:
        assert value > self.term
        self._term = value


def append_record(state: NodeState, record: Record) -> None:
    state.log.append(record)


def append_records(state: NodeState, records: List[Record]) -> None:
    state.log.extend(records)


def state_to_nodes_ids_that_accepted_more_records(state: NodeState
                                                  ) -> Collection[NodeId]:
    return [node_id
            for node_id, length in state.accepted_lengths.items()
            if length > state.commit_length]


def update_state_nodes_ids(state: NodeState,
                           nodes_ids: Collection[NodeId]) -> None:
    new_nodes_ids, old_nodes_ids = set(nodes_ids), set(state.nodes_ids)
    for removed_node_id in old_nodes_ids - new_nodes_ids:
        del (state.accepted_lengths[removed_node_id],
             state.sent_lengths[removed_node_id])
    added_nodes_ids = new_nodes_ids - old_nodes_ids
    state.accepted_lengths.update({node_id: 0 for node_id in added_nodes_ids})
    state.sent_lengths.update({node_id: 0 for node_id in added_nodes_ids})
    state.nodes_ids = nodes_ids


def update_state_term(state: NodeState, value: Term) -> None:
    state.leader_node_id = None
    state.supported_node_id = None
    state.term = value
