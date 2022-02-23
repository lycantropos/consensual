import enum
from typing import (AbstractSet,
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
    __slots__ = ('_rejectors_nodes_ids', '_supported_node_id',
                 '_supporters_nodes_ids', '_term')

    def __init__(self,
                 *,
                 rejectors_nodes_ids: AbstractSet[NodeId] = frozenset(),
                 supported_node_id: Optional[NodeId] = None,
                 supporters_nodes_ids: AbstractSet[NodeId] = frozenset(),
                 term: Term) -> None:
        (
            self._rejectors_nodes_ids, self._supported_node_id,
            self._supporters_nodes_ids, self._term
        ) = rejectors_nodes_ids, supported_node_id, supporters_nodes_ids, term

    __repr__ = generate_repr(__init__)

    @property
    def kind(self) -> RoleKind:
        return RoleKind.CANDIDATE

    @property
    def leader_node_id(self) -> Optional[NodeId]:
        return None

    @property
    def supported_node_id(self) -> Optional[NodeId]:
        return self._supported_node_id

    @property
    def rejectors_nodes_ids(self) -> AbstractSet[NodeId]:
        return self._rejectors_nodes_ids

    @property
    def supporters_nodes_ids(self) -> AbstractSet[NodeId]:
        return self._supporters_nodes_ids

    @property
    def term(self) -> Term:
        return self._term

    def rejected_by(self, node_id: NodeId) -> 'Candidate':
        return Candidate(
                rejectors_nodes_ids=self.rejectors_nodes_ids | {node_id},
                supported_node_id=self.supported_node_id,
                supporters_nodes_ids=self.supporters_nodes_ids,
                term=self.term
        )

    def support(self, node_id: NodeId) -> 'Candidate':
        return Candidate(rejectors_nodes_ids=self.rejectors_nodes_ids,
                         supported_node_id=node_id,
                         supporters_nodes_ids=self.supporters_nodes_ids,
                         term=self.term)

    def supported_by(self, node_id: NodeId) -> 'Candidate':
        return Candidate(
                rejectors_nodes_ids=self.rejectors_nodes_ids,
                supported_node_id=self.supported_node_id,
                supporters_nodes_ids=self.supporters_nodes_ids | {node_id},
                term=self.term
        )


class Follower:
    __slots__ = '_leader_node_id', '_supported_node_id', '_term'

    def __init__(self,
                 *,
                 leader_node_id: Optional[NodeId] = None,
                 supported_node_id: Optional[NodeId] = None,
                 term: Term) -> None:
        self._leader_node_id, self._supported_node_id, self._term = (
            leader_node_id, supported_node_id, term
        )

    __repr__ = generate_repr(__init__)

    @property
    def kind(self) -> RoleKind:
        return RoleKind.FOLLOWER

    @property
    def leader_node_id(self) -> Optional[NodeId]:
        return self._leader_node_id

    @property
    def supported_node_id(self) -> Optional[NodeId]:
        return self._supported_node_id

    @property
    def term(self) -> Term:
        return self._term

    def support(self, node_id: NodeId) -> 'Follower':
        assert self.leader_node_id is None
        return Follower(supported_node_id=node_id,
                        term=self.term)


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
