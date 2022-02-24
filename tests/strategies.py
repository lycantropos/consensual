import time
from asyncio import (get_event_loop,
                     sleep)
from typing import (Any,
                    List,
                    Tuple)

from hypothesis import strategies
from hypothesis.strategies import SearchStrategy
from yarl import URL

from consensual.raft import Processor
from .raft_cluster_node import RaftClusterNode
from .utils import MAX_NODES_COUNT

data_objects = strategies.data()
heartbeats = strategies.floats(1, 5)
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


actions = strategies.text(min_size=1)
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
        max_size=MAX_NODES_COUNT
)


def to_log_arguments(action_with_processor: Tuple[str, Processor]
                     ) -> SearchStrategy[Tuple[str, Any]]:
    action, processor = action_with_processor
    return strategies.tuples(strategies.just(action),
                             processors_parameters[processor])


def to_log_arguments_lists(node: RaftClusterNode
                           ) -> SearchStrategy[List[Tuple[str, Any]]]:
    return strategies.lists(
            (strategies.sampled_from(list(node.processors.items()))
             .flatmap(to_log_arguments))
            if node.processors
            else strategies.nothing()
    )
