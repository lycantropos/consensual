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
from operator import eq
from typing import (Iterator,
                    List,
                    Optional,
                    Set)

import requests
from hypothesis.stateful import (Bundle,
                                 RuleBasedStateMachine,
                                 consumes,
                                 invariant,
                                 rule)
from reprit.base import generate_repr
from requests import (Response,
                      Session)
from yarl import URL

from consensual.raft import (ClusterId,
                             Node,
                             NodeId,
                             Record,
                             Role,
                             Term)
from . import strategies
from .utils import (MAX_RUNNING_NODES_COUNT,
                    equivalence,
                    implication)


class RunningClusterState:
    def __init__(self,
                 _id: ClusterId,
                 *,
                 heartbeat: float,
                 nodes_ids: List[NodeId],
                 stable: bool) -> None:
        self.heartbeat, self._id, self.nodes_ids, self.stable = (
            heartbeat, _id, nodes_ids, stable
        )

    __repr__ = generate_repr(__init__)

    @property
    def id(self) -> ClusterId:
        return self._id


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
        self._id = _id
        self.commit_length = commit_length
        self.leader_node_id = leader_node_id
        self.log = log
        self.role = role
        self.supported_node_id = supported_node_id
        self.term = term

    __repr__ = generate_repr(__init__)

    @property
    def id(self) -> NodeId:
        return self._id


class RunningNode:
    def __init__(self,
                 url: URL,
                 *,
                 heartbeat: float) -> None:
        self.heartbeat, self.url = heartbeat, url
        self._url_string = str(url)
        assert not is_url_reachable(self.url)
        self._process: Optional[multiprocessing.Process] = None
        self._session = Session()

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: 'RunningNode') -> bool:
        return (self.url == other.url and self.heartbeat == other.heartbeat
                if isinstance(other, RunningNode)
                else NotImplemented)

    def __hash__(self) -> int:
        return hash((self.url, self.heartbeat))

    def add(self, *nodes: 'RunningNode') -> Optional[str]:
        response = requests.post(self._url_string,
                                 json=[str(node.url) for node in nodes])
        response.raise_for_status()
        return response.json()['error']

    def initialize(self) -> None:
        assert requests.post(self._url_string).ok

    def delete(self) -> Optional[str]:
        response = requests.delete(self._url_string)
        response.raise_for_status()
        return response.json()['error']

    def load_cluster_state(self) -> RunningClusterState:
        response = self._get(str(self.url.with_path('/cluster')))
        response.raise_for_status()
        raw_state = response.json()
        raw_id, heartbeat, nodes_ids, stable = (
            raw_state['id'], raw_state['heartbeat'], raw_state['nodes_ids'],
            raw_state['stable'],
        )
        return RunningClusterState(ClusterId.from_json(raw_id),
                                   heartbeat=heartbeat,
                                   nodes_ids=nodes_ids,
                                   stable=stable)

    def load_node_state(self) -> RunningNodeState:
        response = self._get(str(self.url.with_path('/node')))
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

    def start(self) -> None:
        self._process = multiprocessing.Process(
                target=_run_node,
                args=(self.url, self.heartbeat))
        self._process.start()

    def stop(self) -> None:
        self._session.close()
        assert self._process is not None
        while self._process.is_alive():
            self._process.terminate()
            time.sleep(0.5)
        assert not is_url_reachable(self.url)

    def _get(self, endpoint: str) -> Response:
        last_error = None
        for _ in range(10):
            try:
                response = self._session.get(endpoint)
            except OSError as error:
                last_error = error
                time.sleep(1)
            else:
                break
        else:
            raise last_error
        return response


def is_url_reachable(url: URL) -> bool:
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.settimeout(2)
    with connection:
        try:
            connection.connect((url.host, url.port))
        except OSError:
            return False
        else:
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
        self._cluster_states: List[RunningClusterState] = []
        self._nodes_states: List[RunningNodeState] = []
        self._urls: Set[URL] = set()

    @invariant()
    def leader_append_only(self) -> None:
        old_nodes_states = self._nodes_states
        self.update_states()
        new_nodes_states = self._nodes_states
        assert all(implication(new_state.role is Role.LEADER,
                               len(new_state.log) >= len(old_state.log)
                               and all(eq, new_state.log, old_state.log))
                   for old_state, new_state in zip(old_nodes_states,
                                                   new_nodes_states))

    @invariant()
    def log_matching(self) -> None:
        self.update_states()
        clusters_states = self._cluster_states
        nodes_states = self._nodes_states
        same_records = defaultdict(list)
        for node_state in nodes_states:
            for index, record in enumerate(node_state.log):
                (same_records[(index, record.term, record.cluster_id)]
                 .append(record))
        assert all(map(eq, records, records[1:])
                   for records in same_records.values())

    @invariant()
    def election_safety(self) -> None:
        self.update_states()
        clusters_leaders_counts = defaultdict(Counter)
        for cluster_state, node_state in zip(self._cluster_states,
                                             self._nodes_states):
            clusters_leaders_counts[cluster_state.id][node_state.term] += (
                    node_state.role is Role.LEADER
            )
        assert all(
                leaders_count <= 1
                for cluster_leaders_counts in clusters_leaders_counts.values()
                for leaders_count in cluster_leaders_counts.values())

    @invariant()
    def term_monotonicity(self) -> None:
        old_nodes_states = self._nodes_states
        self.update_states()
        new_nodes_states = self._nodes_states
        old_terms = {node_state.id: node_state.term
                     for node_state in old_nodes_states}
        new_terms = {node_state.id: node_state.term
                     for node_state in new_nodes_states}
        assert all(old_terms[node_id] <= new_term
                   for node_id, new_term in new_terms.items())

    running_nodes = Bundle('running_nodes')
    shutdown_nodes = Bundle('shutdown_nodes')

    @rule(source_nodes=running_nodes,
          target_nodes=running_nodes)
    def add_nodes(self,
                  target_nodes: List[RunningNode],
                  source_nodes: List[RunningNode]) -> List[RunningNode]:
        source_nodes = source_nodes[:len(target_nodes)]
        source_nodes_states_before = self.load_nodes_states(source_nodes)
        target_clusters_states_before = self.load_clusters_states(target_nodes)
        target_nodes_states_before = self.load_nodes_states(target_nodes)
        errors = list(self._executor.map(RunningNode.add, target_nodes,
                                         source_nodes))
        assert all(equivalence(error is None,
                               target_cluster_state.stable
                               and target_node_state.leader_node_id is not None
                               and (source_node_state.id
                                    not in target_cluster_state.nodes_ids))
                   for (target_cluster_state, target_node_state,
                        source_node_state, error)
                   in zip(target_clusters_states_before,
                          target_nodes_states_before,
                          source_nodes_states_before, errors))

    @rule(target=running_nodes,
          heartbeat=strategies.heartbeats,
          urls=strategies.cluster_urls_lists)
    def create_nodes(self,
                     heartbeat: float,
                     urls: List[URL]) -> List[RunningNode]:
        if len(self._urls) == MAX_RUNNING_NODES_COUNT:
            return []
        assert len(self._urls) < MAX_RUNNING_NODES_COUNT
        max_new_nodes_count = MAX_RUNNING_NODES_COUNT - len(self._urls)
        new_urls = list(set(urls) - self._urls)[:max_new_nodes_count]
        nodes = list(self._executor.map(partial(RunningNode,
                                                heartbeat=heartbeat),
                                        new_urls))
        _exhaust(self._executor.map(RunningNode.start, nodes))
        self._urls.update(new_urls)
        self._nodes.extend(nodes)
        for _ in range(5):
            try:
                self.update_states()
            except OSError:
                time.sleep(1)
            else:
                break
        return nodes

    @rule(nodes=running_nodes)
    def initialize_nodes(self, nodes: List[RunningNode]) -> None:
        _exhaust(self._executor.map(RunningNode.initialize, nodes))
        clusters_states_after = self.load_clusters_states(nodes)
        assert all(cluster_state.id for cluster_state in clusters_states_after)

    @rule(nodes=running_nodes)
    def delete_nodes(self, nodes: List[RunningNode]) -> None:
        clusters_states_before = self.load_clusters_states(nodes)
        nodes_states_before = self.load_nodes_states(nodes)
        errors = list(self._executor.map(RunningNode.delete, nodes))
        assert all(equivalence(error is None,
                               cluster_state.stable
                               and node_state.leader_node_id is not None)
                   for cluster_state, node_state, error
                   in zip(clusters_states_before, nodes_states_before, errors))

    @rule(target=running_nodes,
          nodes=consumes(shutdown_nodes))
    def restart_nodes(self, nodes: List[RunningNode]) -> List[RunningNode]:
        _exhaust(self._executor.map(RunningNode.start, nodes))
        self._nodes += nodes
        return nodes

    @rule(target=shutdown_nodes,
          nodes=consumes(running_nodes))
    def shutdown_nodes(self, nodes: List[RunningNode]) -> List[RunningNode]:
        _exhaust(self._executor.map(RunningNode.stop, nodes))
        shutdown_nodes = frozenset(nodes)
        self._nodes = [node
                       for node in self._nodes
                       if node not in shutdown_nodes]
        return nodes

    @rule(delay=strategies.delays)
    def wait(self, delay: float) -> None:
        time.sleep(delay)

    def teardown(self) -> None:
        _exhaust(self._executor.map(RunningNode.stop, self._nodes))
        self._executor.shutdown()

    def update_states(self) -> None:
        self._cluster_states = self.load_clusters_states(self._nodes)
        self._nodes_states = self.load_nodes_states(self._nodes)

    def load_clusters_states(self, nodes: List[RunningNode]
                             ) -> List[RunningClusterState]:
        return list(self._executor.map(RunningNode.load_cluster_state, nodes))

    def load_nodes_states(self, nodes: List[RunningNode]
                          ) -> List[RunningNodeState]:
        return list(self._executor.map(RunningNode.load_node_state, nodes))


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
