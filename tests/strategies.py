import string
import time
import urllib.parse
from typing import (Any,
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

heartbeats = strategies.floats(2, 4)
delays = strategies.floats(0, 2)
hosts = strategies.just('localhost')
ports = strategies.integers(4000, 5000)
cluster_urls = strategies.builds(URL.build,
                                 scheme=strategies.just('http'),
                                 host=hosts,
                                 port=ports)
cluster_urls_lists = strategies.lists(cluster_urls,
                                      min_size=1,
                                      max_size=MAX_RUNNING_NODES_COUNT,
                                      unique_by=node_url_to_id)


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
processors_dicts_lists = strategies.lists(processors_dicts,
                                          max_size=MAX_RUNNING_NODES_COUNT)


def to_parameters_strategy(path_with_processor: Tuple[str, Processor]
                           ) -> SearchStrategy[Tuple[str, Any]]:
    path, processor = path_with_processor
    return strategies.tuples(strategies.just(path),
                             processors_parameters[processor])


def nodes_to_parameters_strategies(nodes: List[RunningNode]
                                   ) -> SearchStrategy[List[Tuple[str, Any]]]:
    strategies_list = [
        (strategies.sampled_from([
            (urllib.parse.quote(path), processor)
            for path, processor in node.processors.items()
        ]).flatmap(to_parameters_strategy))
        for node in nodes
    ]
    return strategies.tuples(*strategies_list).map(list)
