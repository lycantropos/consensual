import string
import time
from asyncio import (get_event_loop,
                     sleep)
from operator import add
from typing import (Any,
                    List,
                    Tuple)

from hypothesis import strategies
from hypothesis.strategies import SearchStrategy
from yarl import URL

from .raft_cluster_node import RaftClusterNode
from .utils import MAX_RUNNING_NODES_COUNT

data_objects = strategies.data()
heartbeats = strategies.floats(1, 2)
delays = strategies.floats(0, 1)
hosts = strategies.just('localhost')
ports = strategies.integers(4000, 4999)
random_seeds = strategies.integers()


def asyncio_waiting_processor(parameters: float) -> None:
    loop = get_event_loop()
    (loop.create_task
     if loop.is_running()
     else loop.run_until_complete)(sleep(parameters))


def waiting_processor(parameters: float) -> None:
    time.sleep(parameters)


plain_paths_letters = strategies.characters(
        whitelist_categories=['Ll', 'Lu', 'Nd', 'Nl', 'No']
)
paths_infixes_letters = plain_paths_letters | strategies.sampled_from(
        string.whitespace + '!"#$&\'()*+,-./:;<=>?@[\\]^_`|~')
paths_infixes = strategies.text(paths_infixes_letters,
                                min_size=1)


def to_longer_actions(strategy: SearchStrategy[str]) -> SearchStrategy[str]:
    return strategies.builds(add, strategy, paths_infixes)


plain_actions = strategies.text(plain_paths_letters,
                                min_size=1)
actions = (plain_actions
           | strategies.builds(add,
                               strategies.recursive(plain_actions,
                                                    to_longer_actions),
                               plain_actions))
processors_parameters = {waiting_processor: strategies.floats(-1, 1),
                         asyncio_waiting_processor: strategies.floats(-1, 1)}
processors = strategies.sampled_from(list(processors_parameters))
processors_dicts = strategies.dictionaries(keys=actions,
                                           values=processors)
urls = strategies.builds(URL.build,
                         scheme=strategies.just('htttp'),
                         host=hosts,
                         port=ports)
running_nodes_parameters = strategies.tuples(urls, processors_dicts,
                                             random_seeds)
running_nodes_parameters_lists = strategies.lists(
        running_nodes_parameters,
        min_size=1,
        max_size=MAX_RUNNING_NODES_COUNT
)


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
