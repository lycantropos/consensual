import dataclasses
from typing import (Any,
                    Generic,
                    TypeVar)

_T = TypeVar('_T')


@dataclasses.dataclass(frozen=True)
class Event(Generic[_T]):
    action: str
    parameters: Any
