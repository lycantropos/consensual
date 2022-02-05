import time
from collections import (Counter,
                         defaultdict,
                         deque)
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from itertools import (chain,
                       repeat)
from operator import eq
from typing import (Any,
                    Dict,
                    Iterator,
                    List,
                    Set,
                    Tuple)

from hypothesis.stateful import (Bundle,
                                 RuleBasedStateMachine,
                                 consumes,
                                 invariant,
                                 multiple,
                                 precondition,
                                 rule)
from yarl import URL

from consensual.raft import (Processor,
                             Role)
from . import strategies
from .running_cluster_state import RunningClusterState
from .running_node import RunningNode
from .running_node_state import RunningNodeState
from .utils import (MAX_RUNNING_NODES_COUNT,
                    equivalence,
                    implication,
                    transpose)


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
                               and all(map(eq, new_state.log, old_state.log)))
                   for old_state, new_state in zip(old_nodes_states,
                                                   new_nodes_states))

    @invariant()
    def leader_completeness(self) -> None:
        old_nodes_states = self._nodes_states
        self.update_states()
        new_nodes_states = self._nodes_states
        assert all(
                implication(new_state.role is Role.LEADER,
                            all(map(eq,
                                    new_state.log[:old_state.commit_length],
                                    old_state.log[:old_state.commit_length])))
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
    running_nodes_with_log_arguments = Bundle(
            'running_nodes_with_log_arguments')
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

    @rule(target=running_nodes_with_log_arguments,
          nodes_with_log_arguments=running_nodes.flatmap(
                  strategies.to_nodes_with_log_arguments_lists))
    def attach_log_arguments(self,
                             nodes_with_log_arguments
                             : List[Tuple[RunningNode, str, Any]]
                             ) -> Tuple[List[RunningNode], List[str],
                                        List[Any]]:
        return (transpose(nodes_with_log_arguments)
                if nodes_with_log_arguments
                else multiple())

    def is_not_full(self) -> bool:
        return len(self._urls) < MAX_RUNNING_NODES_COUNT

    @precondition(is_not_full)
    @rule(target=running_nodes,
          heartbeat=strategies.heartbeats,
          nodes_parameters=strategies.running_nodes_parameters_lists)
    def create_nodes(self,
                     heartbeat: float,
                     nodes_parameters
                     : List[Tuple[URL, Dict[str, Processor], int]]
                     ) -> List[RunningNode]:
        new_nodes_parameters = [
                                   node_parameters
                                   for node_parameters in nodes_parameters
                                   if node_parameters[0] not in self._urls
                               ][:MAX_RUNNING_NODES_COUNT - len(self._urls)]
        nodes = list(self._executor.map(partial(RunningNode,
                                                heartbeat=heartbeat),
                                        *transpose(new_nodes_parameters)))
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
    def delete_nodes(self, nodes: List[RunningNode]) -> None:
        clusters_states_before = self.load_clusters_states(nodes)
        nodes_states_before = self.load_nodes_states(nodes)
        errors = list(self._executor.map(RunningNode.delete, nodes))
        assert all(equivalence(error is None,
                               cluster_state.stable
                               and node_state.leader_node_id is not None)
                   for cluster_state, node_state, error
                   in zip(clusters_states_before, nodes_states_before, errors))

    @rule(nodes=running_nodes)
    def initialize_nodes(self, nodes: List[RunningNode]) -> None:
        _exhaust(self._executor.map(RunningNode.initialize, nodes))
        clusters_states_after = self.load_clusters_states(nodes)
        assert all(cluster_state.id for cluster_state in clusters_states_after)

    @rule(nodes_with_arguments=running_nodes_with_log_arguments)
    def log(self,
            nodes_with_arguments
            : Tuple[List[RunningNode], List[str], List[Any]]) -> None:
        nodes_states_before = self.load_nodes_states(nodes)
        errors = list(self._executor.map(RunningNode.log,
                                         *nodes_with_arguments))
        assert all(equivalence(error is None,
                               node_state_before.leader_node_id is not None)
                   for node_state_before, error in zip(nodes_states_before,
                                                       errors))

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


TestCluster = Cluster.TestCase
