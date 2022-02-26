from typing import (List,
                    Tuple,
                    TypeVar,
                    overload)

MAX_NODES_COUNT = 100


def equivalence(left: bool, right: bool) -> bool:
    return left is right


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
