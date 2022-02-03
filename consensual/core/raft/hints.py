from typing import (Callable,
                    TypeVar,
                    Union)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

_T = TypeVar('_T')
NodeId = str
Processor = Callable[['Node', _T], None]
Protocol = Protocol
Time = Union[float, int]
Term = int
