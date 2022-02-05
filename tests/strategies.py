import string
import time
from typing import (Any,
                    List,
                    Sequence,
                    Tuple)

from hypothesis import strategies
from hypothesis.strategies import SearchStrategy

from consensual.raft import Node
from .raft_cluster_node import RaftClusterNode
from .utils import MAX_RUNNING_NODES_COUNT

data_objects = strategies.data()
heartbeats = strategies.floats(1, 2)
delays = strategies.floats(0, 1)
hosts = strategies.just('localhost')
ports_ranges_starts = strategies.integers(4000, 4500)
ports_ranges_lengths = strategies.integers(100, 500)


def to_ports_range(start: int, length: int) -> Sequence[int]:
    assert start > 0
    assert length >= MAX_RUNNING_NODES_COUNT
    return range(start, start + length)


ports_ranges = strategies.builds(to_ports_range,
                                 ports_ranges_starts,
                                 ports_ranges_lengths)
random_seeds = strategies.integers()


def waiting_processor(node: Node, parameters: float) -> None:
    time.sleep(parameters)


short_paths_letters = strategies.sampled_from(string.digits
                                              + string.ascii_letters)
long_paths_letters = (
        short_paths_letters
        | strategies.sampled_from(string.whitespace
                                  + '!"#$&\'()*+,-./:;<=>?@[\\]^_`|~'))
paths = ((strategies.text(short_paths_letters,
                          min_size=1)
          | strategies.text(long_paths_letters,
                            min_size=2))
         .map('/{}'.format))
processors_parameters = {waiting_processor: strategies.floats(-10, 10)}
processors = strategies.sampled_from(list(processors_parameters))
processors_dicts = strategies.dictionaries(keys=paths,
                                           values=processors)
running_nodes_parameters = strategies.tuples(hosts, ports_ranges,
                                             processors_dicts, random_seeds)
running_nodes_parameters_lists = strategies.lists(
        running_nodes_parameters,
        min_size=1,
        max_size=MAX_RUNNING_NODES_COUNT)


def to_nodes_with_log_arguments(
        node_with_path: Tuple[RaftClusterNode, str]
) -> SearchStrategy[Tuple[RaftClusterNode, str, Any]]:
    node, path = node_with_path
    return strategies.tuples(strategies.just(node), strategies.just(path),
                             processors_parameters[node.processors[path]])


def to_nodes_with_log_arguments_lists(
        nodes: List[RaftClusterNode]
) -> SearchStrategy[List[Tuple[RaftClusterNode, str, Any]]]:
    strategies_list = [
        strategies.tuples(
                strategies.just(node),
                strategies.sampled_from(list(node.processors.keys()))
        ).flatmap(to_nodes_with_log_arguments)
        for node in nodes
        if node.processors
    ]
    return strategies.tuples(*strategies_list).map(list)
