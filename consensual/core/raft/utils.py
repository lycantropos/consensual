from socket import gethostbyname as host_to_ip_address
from typing import (Mapping,
                    TypeVar)

host_to_ip_address = host_to_ip_address
itemize = ', '.join

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
