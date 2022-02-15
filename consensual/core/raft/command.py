from typing import (Any,
                    Dict,
                    Generic,
                    TypeVar)

from reprit.base import generate_repr

_T = TypeVar('_T')


class Command(Generic[_T]):
    __slots__ = '_action', '_internal', '_parameters'

    def __new__(cls,
                *,
                action: str,
                internal: bool,
                parameters: Any) -> 'Command':
        self = super().__new__(cls)
        self._action, self._internal, self._parameters = (
            action, internal, parameters
        )
        return self

    __repr__ = generate_repr(__new__)

    def __eq__(self, other: Any) -> Any:
        return ((self.action == other.action
                 and self.internal is other.internal
                 and self.parameters == other.parameters)
                if isinstance(other, Command)
                else NotImplemented)

    @property
    def action(self) -> str:
        return self._action

    @property
    def internal(self) -> bool:
        return self._internal

    @property
    def external(self) -> bool:
        return not self._internal

    @property
    def parameters(self) -> Any:
        return self._parameters

    from_json = classmethod(__new__)

    def as_json(self) -> Dict[str, Any]:
        return {'action': self.action,
                'internal': self.internal,
                'parameters': self.parameters}
