from typing import (Any,
                    Callable,
                    Union)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

NodeId = str
Processor = Callable[[Any], None]
Protocol = Protocol
Time = Union[float, int]
Term = int
