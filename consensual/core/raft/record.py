from typing import (Any,
                    Dict)

from reprit.base import generate_repr

from .command import Command
from .hints import Term


class Record:
    __slots__ = '_command', '_term'

    def __new__(cls,
                *,
                command: Command,
                term: Term) -> 'Record':
        self = super().__new__(cls)
        self._command = command
        self._term = term
        return self

    __repr__ = generate_repr(__new__)

    def __eq__(self, other: Any) -> Any:
        return (self.term == other.term and self.command == other.command
                if isinstance(other, Record)
                else NotImplemented)

    @property
    def command(self) -> Command:
        return self._command

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls, *, command: Dict[str, Any], term: Term) -> 'Record':
        return cls(command=Command.from_json(**command),
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'command': self.command.as_json(),
                'term': self.term}
