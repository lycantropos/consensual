import dataclasses
from typing import (Any,
                    Generic,
                    TypeVar)

_T = TypeVar('_T')


@dataclasses.dataclass(frozen=True)
class Command(Generic[_T]):
    path: str
    parameters: Any
