from typing import (Callable,
                    TypeVar,
                    Union)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

Time = Union[float, int]
NodeId = str
_T = TypeVar('_T')
Protocol = Protocol
Processor = Callable[['Node', _T], None]
Term = int
