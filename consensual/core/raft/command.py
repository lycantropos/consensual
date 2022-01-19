from typing import (Any,
                    Dict,
                    Generic,
                    TypeVar)

from reprit.base import generate_repr

_T = TypeVar('_T')


class Command(Generic[_T]):
    __slots__ = '_action', '_parameters'

    def __init__(self,
                 *,
                 action: str,
                 parameters: Any) -> None:
        self._action, self._parameters = action, parameters

    __repr__ = generate_repr(__init__)

    @property
    def action(self) -> str:
        return self._action

    @property
    def parameters(self) -> Any:
        return self._parameters

    @classmethod
    def from_json(cls, *, action: str, parameters: Any) -> 'Command':
        return cls(action=action,
                   parameters=parameters)

    def as_json(self) -> Dict[str, Any]:
        return {'action': self.action, 'parameters': self.parameters}
