import logging.config
import multiprocessing
import socket
import sys
import time
from collections import (Counter,
                         defaultdict,
                         deque)
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import (Iterator,
                    List,
                    Optional,
                    Set,
                    Tuple)

import requests
from hypothesis.stateful import (Bundle,
                                 RuleBasedStateMachine,
                                 consumes,
                                 invariant,
                                 rule)
from yarl import URL

from consensual.raft import (ClusterId,
                             Node,
                             NodeId,
                             Record,
                             Role,
                             Term)
from . import strategies
from .utils import MAX_RUNNING_NODES_COUNT


class RunningClusterState:
    def __init__(self,
                 _id: ClusterId,
                 *,
                 heartbeat: float) -> None:
        self.heartbeat, self.id = heartbeat, _id


class RunningNodeState:
    def __init__(self,
                 _id: NodeId,
                 *,
                 commit_length: int,
                 leader_node_id: Optional[NodeId],
                 log: List[Record],
                 role: Role,
                 supported_node_id: Optional[NodeId],
                 term: Term) -> None:
        self.id = _id
        self.commit_length = commit_length
        self.leader_node_id = leader_node_id
        self.log = log
        self.role = role
        self.supported_node_id = supported_node_id
        self.term = term


class RunningNode:
    def __init__(self,
                 url: URL,
                 *,
                 heartbeat: float) -> None:
        self.heartbeat, self.url = heartbeat, url
        self._url_string = str(url)
        assert not is_url_reachable(self.url)
        self._process = multiprocessing.Process(target=_run_node,
                                                args=(url, heartbeat))
        self._process.start()

    def add(self, node: 'RunningNode') -> None:
        assert requests.post(self._url_string,
                             json=[str(node.url)]).ok

    def initialize(self) -> None:
        assert requests.post(self._url_string).ok

    def delete(self) -> None:
        response = requests.delete(self._url_string)
        assert response.ok
        error = response.json()['error']
        assert error is None, error

    def load_state(self) -> Tuple[RunningClusterState, RunningNodeState]:
        return self.load_cluster_state(), self.load_node_state()

    def load_cluster_state(self) -> RunningClusterState:
        response = requests.get(str(self.url.with_path('/cluster')),
                                timeout=self.heartbeat)
        response.raise_for_status()
        raw_state = response.json()
        raw_id, heartbeat = raw_state['id'], raw_state['heartbeat']
        return RunningClusterState(ClusterId.from_json(raw_id),
                                   heartbeat=heartbeat)

    def load_node_state(self) -> RunningNodeState:
        response = requests.get(str(self.url.with_path('/node')),
                                timeout=self.heartbeat)
        response.raise_for_status()
        raw_state = response.json()
        (
            commit_length, id_, leader_node_id, raw_log, raw_node_role,
            supported_node_id, term,
        ) = (
            raw_state['commit_length'], raw_state['id'],
            raw_state['leader_node_id'], raw_state['log'], raw_state['role'],
            raw_state['supported_node_id'], raw_state['term'],
        )
        log = [Record.from_json(**raw_record) for raw_record in raw_log]
        return RunningNodeState(id_,
                                commit_length=commit_length,
                                leader_node_id=leader_node_id,
                                log=log,
                                role=Role(raw_node_role),
                                supported_node_id=supported_node_id,
                                term=term)

    def stop(self) -> None:
        self._process.kill()
        while is_url_reachable(self.url):
            time.sleep(0.5)


def is_url_reachable(url: URL) -> bool:
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.settimeout(2)
    try:
        connection.connect((url.host, url.port))
    except OSError:
        return False
    else:
        connection.close()
        return True


def to_logger(name: str,
              *,
              version: int = 1) -> logging.Logger:
    console_formatter = {'format': '[%(levelname)-8s %(name)s] %(msg)s'}
    formatters = {'console': console_formatter}
    console_handler_config = {'class': 'logging.StreamHandler',
                              'level': logging.DEBUG,
                              'formatter': 'console',
                              'stream': sys.stdout}
    handlers = {'console': console_handler_config}
    loggers = {name: {'level': logging.DEBUG,
                      'handlers': ('console',)}}
    config = {'formatters': formatters,
              'handlers': handlers,
              'loggers': loggers,
              'version': version}
    logging.config.dictConfig(config)
    return logging.getLogger(name)


class Cluster(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self._executor = ThreadPoolExecutor()
        self._nodes: List[RunningNode] = []
        self._states: List[Tuple[RunningClusterState, RunningNodeState]] = []
        self._urls: Set[URL] = set()

    running_nodes = Bundle('running_nodes')
    initialized_nodes = Bundle('initialized_nodes')

    @rule(target=running_nodes,
          heartbeat=strategies.heartbeats,
          urls=strategies.cluster_urls_lists)
    def init_nodes(self,
                   heartbeat: float,
                   urls: List[URL]) -> List[RunningNode]:
        if len(self._urls) == MAX_RUNNING_NODES_COUNT:
            return []
        assert len(self._urls) < MAX_RUNNING_NODES_COUNT
        max_new_nodes_count = MAX_RUNNING_NODES_COUNT - len(self._urls)
        new_urls = list(set(urls) - self._urls)[:max_new_nodes_count]
        new_nodes = list(self._executor.map(partial(RunningNode,
                                                    heartbeat=heartbeat),
                                            new_urls))
        self._urls.update(new_urls)
        self._nodes.extend(new_nodes)
        for _ in range(5):
            try:
                self.update_states()
            except OSError:
                time.sleep(1)
            else:
                break
        return new_nodes

    @rule(target=initialized_nodes,
          initialized_nodes=initialized_nodes,
          nodes=consumes(running_nodes))
    def add_nodes(self,
                  initialized_nodes: List[RunningNode],
                  nodes: List[RunningNode]) -> List[RunningNode]:
        _exhaust(self._executor.map(RunningNode.add, initialized_nodes, nodes))
        return nodes

    @rule(target=initialized_nodes,
          nodes=consumes(running_nodes))
    def initialize_nodes(self, nodes: List[RunningNode]) -> List[RunningNode]:
        _exhaust(self._executor.map(RunningNode.initialize, nodes))
        return nodes

    @rule(target=running_nodes,
          nodes=consumes(initialized_nodes))
    def delete_nodes(self, nodes: List[RunningNode]) -> List[RunningNode]:
        _exhaust(self._executor.map(RunningNode.delete, nodes))
        return nodes

    @rule(delay=strategies.delays)
    def wait(self, delay: float) -> None:
        time.sleep(delay)

    @invariant()
    def term_monotonicity(self) -> None:
        old_states = self._states
        self.update_states()
        new_states = self._states
        old_terms = {node_state.id: node_state.term
                     for _, node_state in old_states}
        new_terms = {node_state.id: node_state.term
                     for _, node_state in new_states}
        assert all(old_terms[node_id] <= new_term
                   for node_id, new_term in new_terms.items())

    @invariant()
    def election_safety(self) -> None:
        self.update_states()
        clusters_leaders_counts = defaultdict(Counter)
        for cluster_state, node_state in self._states:
            clusters_leaders_counts[cluster_state.id][node_state.term] += (
                    node_state.role is Role.LEADER
            )
        assert all(
                leaders_count <= 1
                for cluster_leaders_counts in clusters_leaders_counts.values()
                for leaders_count in cluster_leaders_counts.values())

    def teardown(self) -> None:
        _exhaust(self._executor.map(RunningNode.stop, self._nodes))
        self._executor.shutdown()

    def update_states(self) -> None:
        self._states = list(self._executor.map(RunningNode.load_state,
                                               self._nodes))


def _exhaust(iterator: Iterator) -> None:
    deque(iterator,
          maxlen=0)


def _run_node(url: URL,
              heartbeat: float) -> None:
    node = Node.from_url(url,
                         heartbeat=heartbeat,
                         logger=to_logger(url.authority))
    return node.run()


TestCluster = Cluster.TestCase
