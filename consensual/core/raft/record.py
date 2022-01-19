import dataclasses
from typing import (Any,
                    Dict)

from .event import Event
from .hints import Term


@dataclasses.dataclass(frozen=True)
class Record:
    event: Event
    term: Term

    @classmethod
    def from_json(cls, *, event: Dict[str, Any], term: Term) -> 'Record':
        return cls(event=Event(**event),
                   term=term)
