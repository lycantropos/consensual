import string
import time
import urllib.parse
from typing import (Any,
                    Dict,
                    List,
                    Tuple,
                    TypeVar)

from hypothesis import strategies
from hypothesis.strategies import SearchStrategy
from yarl import URL

from consensual.core.raft.node import node_url_to_id
from consensual.raft import (Node,
                             Processor)
from .running_node import RunningNode
from .utils import MAX_RUNNING_NODES_COUNT

heartbeats = strategies.floats(1, 2)
delays = strategies.floats(2, 4)
hosts = strategies.just('localhost')
ports = strategies.integers(4000, 5000)
random_seeds = strategies.integers()
urls = strategies.builds(URL.build,
                         scheme=strategies.just('http'),
                         host=hosts,
                         port=ports)


def waiting_processor(node: Node, parameters: float) -> None:
    time.sleep(parameters)


_T = TypeVar('_T')

paths_letters = strategies.sampled_from(string.digits + string.ascii_letters
                                        + string.whitespace
                                        + '!"#$%&\'()*+,-./:;<=>?@[\\]^_`|~')
paths = (strategies.text(paths_letters,
                         min_size=1)
         .map('/{}'.format))
processors_parameters = {waiting_processor: strategies.floats(-10, 10)}
processors = strategies.sampled_from(list(processors_parameters))
processors_dicts = strategies.dictionaries(keys=paths,
                                           values=processors)
running_nodes_parameters = strategies.tuples(urls, processors_dicts,
                                             random_seeds)


def url_with_random_seed_key(value: Tuple[URL, Dict[str, Processor], int]
                             ) -> Any:
    return node_url_to_id(value[0])


running_nodes_parameters_lists = strategies.lists(
        running_nodes_parameters,
        min_size=1,
        max_size=MAX_RUNNING_NODES_COUNT,
        unique_by=url_with_random_seed_key)


def to_nodes_with_log_arguments(
        node_with_path: Tuple[RunningNode, str]
) -> SearchStrategy[Tuple[RunningNode, str, Any]]:
    node, path = node_with_path
    return strategies.tuples(strategies.just(node),
                             strategies.just(urllib.parse.quote(path)),
                             processors_parameters[node.processors[path]])


def to_nodes_with_log_arguments_lists(
        nodes: List[RunningNode]
) -> SearchStrategy[List[Tuple[RunningNode, str, Any]]]:
    strategies_list = [
        strategies.tuples(
                strategies.just(node),
                strategies.sampled_from(list(node.processors.keys()))
        ).flatmap(to_nodes_with_log_arguments)
        for node in nodes
        if node.processors
    ]
    return strategies.tuples(*strategies_list).map(list)
