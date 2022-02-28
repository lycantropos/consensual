import enum
from typing import (Any,
                    Dict,
                    List)

from reprit.base import generate_repr

from .cluster import DisjointCluster
from .cluster_id import (ClusterId,
                         RawClusterId)
from .command import Command
from .hints import (NodeId,
                    Protocol,
                    Term)
from .record import Record


class Call(Protocol):
    def as_json(self) -> Dict[str, Any]:
        return {}


class LogCall:
    __slots__ = '_command', '_node_id'

    def __new__(cls, *, command: Command, node_id: NodeId) -> 'LogCall':
        self = super().__new__(cls)
        self._command, self._node_id = command, node_id
        return self

    __repr__ = generate_repr(__new__)

    @property
    def command(self) -> Command:
        return self._command

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @classmethod
    def from_json(cls,
                  *,
                  command: Dict[str, Any],
                  node_id: NodeId) -> 'LogCall':
        return cls(command=Command.from_json(**command),
                   node_id=node_id)

    def as_json(self) -> Dict[str, Any]:
        return {'command': self.command.as_json(),
                'node_id': self.node_id}


class LogStatus(enum.IntEnum):
    REJECTED = enum.auto()
    SUCCEED = enum.auto()
    UNAVAILABLE = enum.auto()
    UNGOVERNABLE = enum.auto()


class LogReply:
    __slots__ = '_status',

    def __new__(cls, status: LogStatus) -> 'LogReply':
        self = super().__new__(cls)
        self._status = status
        return self

    __repr__ = generate_repr(__new__)

    @property
    def status(self) -> LogStatus:
        return self._status

    @classmethod
    def from_json(cls, status: int) -> 'LogReply':
        return cls(LogStatus(status))

    def as_json(self) -> Dict[str, Any]:
        return {'status': int(self.status)}


class MessageKind(enum.IntEnum):
    LOG = 0
    SYNC = 1
    UPDATE = 2
    VOTE = 3


class SyncCall:
    __slots__ = ('_cluster_id', '_commit_length', '_node_id',
                 '_prefix_cluster_id', '_prefix_length', '_prefix_term',
                 '_suffix', '_term')

    def __new__(cls,
                *,
                cluster_id: ClusterId,
                commit_length: int,
                node_id: NodeId,
                prefix_cluster_id: ClusterId,
                prefix_length: int,
                prefix_term: Term,
                suffix: List[Record],
                term: Term) -> 'SyncCall':
        self = super().__new__(cls)
        (
            self._cluster_id, self._commit_length, self._node_id,
            self._prefix_cluster_id, self._prefix_length, self._prefix_term,
            self._suffix, self._term
        ) = (
            cluster_id, commit_length, node_id, prefix_cluster_id,
            prefix_length, prefix_term, suffix, term,
        )
        return self

    __repr__ = generate_repr(__new__)

    @property
    def cluster_id(self) -> ClusterId:
        return self._cluster_id

    @property
    def commit_length(self) -> int:
        return self._commit_length

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def prefix_cluster_id(self) -> ClusterId:
        return self._prefix_cluster_id

    @property
    def prefix_length(self) -> int:
        return self._prefix_length

    @property
    def prefix_term(self) -> Term:
        return self._prefix_term

    @property
    def suffix(self) -> List[Record]:
        return self._suffix

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  cluster_id: RawClusterId,
                  commit_length: int,
                  node_id: NodeId,
                  prefix_cluster_id: RawClusterId,
                  prefix_length: int,
                  prefix_term: Term,
                  suffix: List[Dict[str, Any]],
                  term: Term) -> 'SyncCall':
        return cls(cluster_id=ClusterId.from_json(cluster_id),
                   commit_length=commit_length,
                   node_id=node_id,
                   prefix_cluster_id=ClusterId.from_json(prefix_cluster_id),
                   prefix_length=prefix_length,
                   prefix_term=prefix_term,
                   suffix=[Record.from_json(**record) for record in suffix],
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_id': self.cluster_id.as_json(),
                'commit_length': self.commit_length,
                'node_id': self.node_id,
                'prefix_cluster_id': self.prefix_cluster_id.as_json(),
                'prefix_length': self.prefix_length,
                'prefix_term': self.prefix_term,
                'suffix': [record.as_json() for record in self.suffix],
                'term': self.term}


class SyncStatus(enum.IntEnum):
    CONFLICT = enum.auto()
    FAILURE = enum.auto()
    SUCCESS = enum.auto()
    UNAVAILABLE = enum.auto()


class SyncReply:
    __slots__ = '_accepted_length', '_node_id', '_status', '_term'

    def __new__(cls,
                *,
                accepted_length: int,
                node_id: NodeId,
                status: SyncStatus,
                term: Term) -> 'SyncReply':
        self = super().__new__(cls)
        self._accepted_length, self._node_id, self._status, self._term = (
            accepted_length, node_id, status, term
        )
        return self

    __repr__ = generate_repr(__new__)

    @property
    def accepted_length(self) -> int:
        return self._accepted_length

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def status(self) -> SyncStatus:
        return self._status

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  accepted_length: int,
                  node_id: NodeId,
                  status: int,
                  term: Term) -> 'SyncReply':
        return cls(accepted_length=accepted_length,
                   node_id=node_id,
                   status=SyncStatus(status),
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'accepted_length': self.accepted_length,
                'node_id': self.node_id,
                'status': int(self.status),
                'term': self.term}


class UpdateCall:
    __slots__ = '_cluster', '_node_id'

    def __new__(cls,
                *,
                cluster: DisjointCluster,
                node_id: NodeId) -> 'UpdateCall':
        self = super().__new__(cls)
        self._cluster, self._node_id = cluster, node_id
        return self

    __repr__ = generate_repr(__new__)

    @property
    def cluster(self) -> DisjointCluster:
        return self._cluster

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @classmethod
    def from_json(cls,
                  *,
                  cluster: Dict[str, Any],
                  node_id: NodeId) -> 'UpdateCall':
        return cls(cluster=DisjointCluster.from_json(**cluster),
                   node_id=node_id)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster': self.cluster.as_json(),
                'node_id': self.node_id}


class UpdateStatus(enum.IntEnum):
    REJECTED = enum.auto()
    SUCCEED = enum.auto()
    UNAVAILABLE = enum.auto()
    UNGOVERNABLE = enum.auto()
    UNSTABLE = enum.auto()


class UpdateReply:
    __slots__ = '_status',

    def __new__(cls, status: UpdateStatus) -> 'UpdateReply':
        self = super().__new__(cls)
        self._status = status
        return self

    __repr__ = generate_repr(__new__)

    @property
    def status(self) -> UpdateStatus:
        return self._status

    @classmethod
    def from_json(cls, status: int) -> 'UpdateReply':
        return cls(UpdateStatus(status))

    def as_json(self) -> Dict[str, Any]:
        return {'status': int(self.status)}


class VoteCall:
    __slots__ = '_log_length', '_log_term', '_node_id', '_term'

    def __new__(cls,
                *,
                log_length: int,
                log_term: Term,
                node_id: NodeId,
                term: Term) -> 'VoteCall':
        self = super().__new__(cls)
        self._log_length, self._log_term, self._node_id, self._term = (
            log_length, log_term, node_id, term
        )
        return self

    __repr__ = generate_repr(__new__)

    @property
    def log_length(self) -> int:
        return self._log_length

    @property
    def log_term(self) -> Term:
        return self._log_term

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  log_length: int,
                  log_term: Term,
                  node_id: NodeId,
                  term: Term) -> 'VoteCall':
        return cls(log_length=log_length,
                   log_term=log_term,
                   node_id=node_id,
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'log_length': self.log_length,
                'log_term': self.log_term,
                'node_id': self.node_id,
                'term': self.term}


class VoteStatus(enum.IntEnum):
    CONFLICTS = enum.auto()
    IGNORES = enum.auto()
    OPPOSES = enum.auto()
    REJECTS = enum.auto()
    SUPPORTS = enum.auto()
    UNAVAILABLE = enum.auto()


class VoteReply:
    __slots__ = '_node_id', '_status', '_term'

    def __new__(cls,
                *,
                node_id: NodeId,
                status: VoteStatus,
                term: Term) -> 'VoteReply':
        self = super().__new__(cls)
        self._node_id, self._status, self._term = node_id, status, term
        return self

    __repr__ = generate_repr(__new__)

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def status(self) -> VoteStatus:
        return self._status

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  node_id: NodeId,
                  status: int,
                  term: Term) -> 'VoteReply':
        return cls(node_id=node_id,
                   status=VoteStatus(status),
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'node_id': self.node_id,
                'status': int(self.status),
                'term': self.term}
