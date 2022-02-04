import multiprocessing
from typing import (List,
                    Tuple,
                    TypeVar)

MAX_RUNNING_NODES_COUNT = max(multiprocessing.cpu_count() - 1, 1)


def equivalence(left: bool, right: bool) -> bool:
    return left is right


def implication(antecedent: bool, consequent: bool) -> bool:
    return not antecedent or consequent


_T1 = TypeVar('_T1')
_T2 = TypeVar('_T2')


def transpose(pairs: List[Tuple[_T1, _T2]]) -> Tuple[List[_T1], List[_T2]]:
    return tuple(map(list, zip(*pairs))) if pairs else ([], [])
