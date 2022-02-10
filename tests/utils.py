import multiprocessing
from typing import (List,
                    Tuple,
                    TypeVar,
                    overload)

MAX_RUNNING_NODES_COUNT = max(multiprocessing.cpu_count() - 1, 1)


def implication(antecedent: bool, consequent: bool) -> bool:
    return not antecedent or consequent


_T1 = TypeVar('_T1')
_T2 = TypeVar('_T2')


@overload
def transpose(values: List[Tuple[_T1, _T2]]) -> Tuple[List[_T1], List[_T2]]:
    ...


@overload
def transpose(values: List[Tuple[_T1, ...]]) -> Tuple[List[_T1], ...]:
    ...


def transpose(values):
    assert values
    return tuple(map(list, zip(*values)))
