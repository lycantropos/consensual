from typing import (Any,
                    List)

from reprit.base import generate_repr

RawClusterId = List[str]


class ClusterId:
    __slots__ = '_variants',

    def __new__(cls, *_variants: str) -> 'ClusterId':
        self = super().__new__(cls)
        self._variants = frozenset(_variants)
        return self

    __repr__ = generate_repr(__new__)

    def __bool__(self) -> bool:
        return bool(self._variants)

    def __eq__(self, other: 'ClusterId') -> Any:
        return (self._variants == other._variants
                if isinstance(other, ClusterId)
                else NotImplemented)

    def __hash__(self) -> int:
        return hash(self._variants)

    @classmethod
    def from_json(cls, _raw: RawClusterId) -> 'ClusterId':
        return cls(*_raw)

    def agrees_with(self, other: 'ClusterId') -> bool:
        return not self._variants.isdisjoint(other._variants)

    def as_json(self) -> RawClusterId:
        return list(self._variants)

    def join_with(self, other: 'ClusterId') -> 'ClusterId':
        assert self._variants.isdisjoint(other._variants)
        return ClusterId(*self._variants, *other._variants)
