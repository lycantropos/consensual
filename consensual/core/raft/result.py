from typing import (NoReturn,
                    TypeVar)

from reprit.base import generate_repr

from .hints import Protocol

_T = TypeVar('_T')


class Result(Protocol[_T]):
    value: _T


class Ok:
    def __init__(self, value: _T) -> None:
        self.value = value

    __repr__ = generate_repr(__init__)


class Error:
    def __init__(self, exception: Exception) -> None:
        self.exception = exception

    __repr__ = generate_repr(__init__)

    @property
    def value(self) -> NoReturn:
        raise self.exception
