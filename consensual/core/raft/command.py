from typing import (Any,
                    Dict,
                    Generic,
                    TypeVar)

from reprit.base import generate_repr

_T = TypeVar('_T')


class Command(Generic[_T]):
    __slots__ = '_action', '_parameters'

    def __new__(cls,
                *,
                action: str,
                parameters: Any) -> 'Command':
        self = super().__new__(cls)
        self._action, self._parameters = action, parameters
        return self

    __repr__ = generate_repr(__new__)

    @property
    def action(self) -> str:
        return self._action

    @property
    def parameters(self) -> Any:
        return self._parameters

    from_json = classmethod(__new__)

    def as_json(self) -> Dict[str, Any]:
        return {'action': self.action, 'parameters': self.parameters}
