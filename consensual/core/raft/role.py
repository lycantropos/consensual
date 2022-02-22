import enum
from typing import (Collection,
                    MutableSet,
                    Optional,
                    Union)

from reprit.base import generate_repr

from .hints import (NodeId,
                    Term)


class RoleKind(enum.IntEnum):
    CANDIDATE = 0
    FOLLOWER = 1
    LEADER = 2


class Candidate:
    __slots__ = ('_rejectors_nodes_ids', 'supported_node_id',
                 '_supporters_nodes_ids', '_term')

    def __init__(self, term: Term) -> None:
        (
            self._rejectors_nodes_ids, self.supported_node_id,
            self._supporters_nodes_ids, self._term
        ) = set(), None, set(), term

    __repr__ = generate_repr(__init__)

    @property
    def kind(self) -> RoleKind:
        return RoleKind.CANDIDATE

    @property
    def leader_node_id(self) -> Optional[NodeId]:
        return None

    @property
    def rejectors_nodes_ids(self) -> MutableSet[NodeId]:
        return self._rejectors_nodes_ids

    @property
    def supporters_nodes_ids(self) -> MutableSet[NodeId]:
        return self._supporters_nodes_ids

    @property
    def term(self) -> Term:
        return self._term


class Follower:
    __slots__ = '_leader_node_id', 'supported_node_id', '_term'

    def __init__(self,
                 *,
                 leader_node_id: Optional[NodeId] = None,
                 supported_node_id: Optional[NodeId] = None,
                 term: Term) -> None:
        self._leader_node_id, self.supported_node_id, self._term = (
            leader_node_id, supported_node_id, term
        )

    __repr__ = generate_repr(__init__)

    @property
    def kind(self) -> RoleKind:
        return RoleKind.FOLLOWER

    @property
    def leader_node_id(self) -> Optional[NodeId]:
        return self._leader_node_id

    @leader_node_id.setter
    def leader_node_id(self, value: Optional[NodeId]) -> None:
        assert value is None
        self._leader_node_id = value

    @property
    def term(self) -> Term:
        return self._term

    @term.setter
    def term(self, value: Term) -> None:
        self.leader_node_id = None
        self._term = value


class Leader:
    __slots__ = '_leader_node_id', '_supported_node_id', '_term'

    def __init__(self,
                 *,
                 leader_node_id: Optional[NodeId],
                 supported_node_id: Optional[NodeId] = None,
                 term: Term) -> None:
        self._leader_node_id, self._supported_node_id, self._term = (
            leader_node_id, supported_node_id, term
        )

    __repr__ = generate_repr(__init__)

    @property
    def kind(self) -> RoleKind:
        return RoleKind.LEADER

    @property
    def leader_node_id(self) -> Optional[NodeId]:
        return self._leader_node_id

    @property
    def supported_node_id(self) -> Optional[NodeId]:
        return self._supported_node_id

    @property
    def term(self) -> Term:
        return self._term


Role = Union[Candidate, Follower, Leader]


def update_nodes_ids(role: Role, nodes_ids: Collection[NodeId]) -> None:
    if (role.kind is not RoleKind.LEADER
            and role.leader_node_id is not None
            and role.leader_node_id not in nodes_ids):
        role.leader_node_id = None
