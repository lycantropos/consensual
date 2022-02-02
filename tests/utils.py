import multiprocessing

MAX_RUNNING_NODES_COUNT = max(multiprocessing.cpu_count() - 1, 3)


def ceil_divide(dividend: int, divisor: int) -> int:
    return -((-dividend) // divisor)
