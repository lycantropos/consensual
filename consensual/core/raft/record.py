import dataclasses
from typing import (Any,
                    Dict)

from .command import Command
from .hints import Term


@dataclasses.dataclass(frozen=True)
class Record:
    command: Command
    term: Term

    @classmethod
    def from_json(cls,
                  *,
                  command: Dict[str, Any],
                  term: Term) -> 'Record':
        return cls(command=Command(**command),
                   term=term)
