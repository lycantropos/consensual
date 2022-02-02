import multiprocessing

MAX_RUNNING_NODES_COUNT = max(multiprocessing.cpu_count() - 1, 1)


def equivalence(left: bool, right: bool) -> bool:
    return left is right
