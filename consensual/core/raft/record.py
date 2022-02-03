from typing import (Any,
                    Dict)

from reprit.base import generate_repr

from .cluster_id import (ClusterId,
                         RawClusterId)
from .command import Command
from .hints import Term


class Record:
    __slots__ = '_cluster_id', '_command', '_term'

    def __new__(cls,
                *,
                cluster_id: ClusterId,
                command: Command,
                term: Term) -> 'Record':
        self = super().__new__(cls)
        self._cluster_id, self._command, self._term = cluster_id, command, term
        return self

    __repr__ = generate_repr(__new__)

    def __eq__(self, other: Any) -> Any:
        return ((self.term == other.term
                 and self.cluster_id == other.cluster_id
                 and self.command == other.command)
                if isinstance(other, Record)
                else NotImplemented)

    @property
    def cluster_id(self) -> ClusterId:
        return self._cluster_id

    @property
    def command(self) -> Command:
        return self._command

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  cluster_id: RawClusterId,
                  command: Dict[str, Any],
                  term: Term) -> 'Record':
        return cls(cluster_id=ClusterId.from_json(cluster_id),
                   command=Command.from_json(**command),
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_id': self.cluster_id.as_json(),
                'command': self.command.as_json(),
                'term': self.term}
