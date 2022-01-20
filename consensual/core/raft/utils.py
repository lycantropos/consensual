import traceback
from socket import gethostbyname as host_to_ip_address
from typing import (Mapping,
                    NoReturn,
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


def format_exception(value: Exception) -> str:
    return ''.join(traceback.format_exception(type(value), value,
                                              value.__traceback__))


host_to_ip_address = host_to_ip_address

_Key = TypeVar('_Key')
_Value = TypeVar('_Value')


def subtract_mapping(minuend: Mapping[_Key, _Value],
                     subtrahend: Mapping[_Key, _Value]
                     ) -> Mapping[_Key, _Value]:
    return {key: value
            for key, value in minuend.items()
            if key not in subtrahend}


def unite_mappings(left: Mapping[_Key, _Value],
                   right: Mapping[_Key, _Value]) -> Mapping[_Key, _Value]:
    return {**left, **right}
