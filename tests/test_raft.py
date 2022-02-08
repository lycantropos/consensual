import time
from collections import (Counter,
                         defaultdict,
                         deque)
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from operator import eq
from typing import (Dict,
                    Iterator,
                    List,
                    Sequence,
                    Tuple)

from hypothesis.stateful import (Bundle,
                                 RuleBasedStateMachine,
                                 consumes,
                                 invariant,
                                 multiple,
                                 precondition,
                                 rule)
from hypothesis.strategies import DataObject

from consensual.raft import (Processor,
                             Role)
from . import strategies
from .raft_cluster_node import RaftClusterNode
from .raft_cluster_state import RaftClusterState
from .raft_node_state import RaftNodeState
from .utils import (MAX_RUNNING_NODES_COUNT,
                    equivalence,
                    implication,
                    transpose)


class RaftNetwork(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self._executor = ThreadPoolExecutor()
        self._nodes: List[RaftClusterNode] = []
        self._new_cluster_states: List[RaftClusterState] = []
        self._old_cluster_states: List[RaftClusterState] = []
        self._new_nodes_states: List[RaftNodeState] = []
        self._old_nodes_states: List[RaftNodeState] = []

    @invariant()
    def commit_length_monotonicity(self) -> None:
        old_commit_lengths = {node_state.id: node_state.commit_length
                              for node_state in self._old_nodes_states}
        new_commit_lengths = {node_state.id: node_state.commit_length
                              for node_state in self._new_nodes_states}
        assert all(
                old_commit_lengths.get(node_id, 0) <= new_commit_length
                for node_id, new_commit_length in new_commit_lengths.items())

    @invariant()
    def leader_append_only(self) -> None:
        assert all(implication(new_state.role is Role.LEADER,
                               len(new_state.log) >= len(old_state.log)
                               and all(map(eq, new_state.log, old_state.log)))
                   for old_state, new_state in zip(self._old_nodes_states,
                                                   self._new_nodes_states))

    @invariant()
    def leader_completeness(self) -> None:
        assert all(
                implication(new_state.role is Role.LEADER,
                            all(map(eq,
                                    new_state.log[:old_state.commit_length],
                                    old_state.log[:old_state.commit_length])))
                for old_state, new_state in zip(self._old_nodes_states,
                                                self._new_nodes_states))

    @invariant()
    def log_matching(self) -> None:
        same_records = defaultdict(list)
        for node_state in self._new_nodes_states:
            for index, record in enumerate(node_state.log):
                (same_records[(index, record.term, record.cluster_id)]
                 .append(record))
        assert all(map(eq, records, records[1:])
                   for records in same_records.values())

    @invariant()
    def election_safety(self) -> None:
        clusters_leaders_counts = defaultdict(Counter)
        for cluster_state, node_state in zip(self._new_cluster_states,
                                             self._new_nodes_states):
            clusters_leaders_counts[cluster_state.id][node_state.term] += (
                    node_state.role is Role.LEADER
            )
        assert all(
                leaders_count <= 1
                for cluster_leaders_counts in clusters_leaders_counts.values()
                for leaders_count in cluster_leaders_counts.values())

    @invariant()
    def term_monotonicity(self) -> None:
        old_terms = {node_state.id: node_state.term
                     for node_state in self._old_nodes_states}
        new_terms = {node_state.id: node_state.term
                     for node_state in self._new_nodes_states}
        assert all(old_terms.get(node_id, 0) <= new_term
                   for node_id, new_term in new_terms.items())

    running_nodes = Bundle('running_nodes')
    shutdown_nodes = Bundle('shutdown_nodes')

    @rule(target=running_nodes,
          source_nodes=consumes(running_nodes),
          target_nodes=consumes(running_nodes))
    def add_nodes(self,
                  target_nodes: List[RaftClusterNode],
                  source_nodes: List[RaftClusterNode]
                  ) -> List[RaftClusterNode]:
        source_nodes = source_nodes[:len(target_nodes)]
        source_nodes_states_before = self.load_nodes_states(source_nodes)
        target_clusters_states_before = self.load_clusters_states(target_nodes)
        target_nodes_states_before = self.load_nodes_states(target_nodes)
        errors = list(self._executor.map(RaftClusterNode.add, target_nodes,
                                         source_nodes))
        self.update_states()
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
        return multiple(source_nodes, target_nodes)

    def is_not_full(self) -> bool:
        return len(self._nodes) < MAX_RUNNING_NODES_COUNT

    @precondition(is_not_full)
    @rule(target=running_nodes,
          heartbeat=strategies.heartbeats,
          nodes_parameters=strategies.running_nodes_parameters_lists)
    def create_nodes(self,
                     heartbeat: float,
                     nodes_parameters: List[Tuple[str, Sequence[int],
                                                  Dict[str, Processor], int]]
                     ) -> List[RaftClusterNode]:
        max_new_nodes_count = MAX_RUNNING_NODES_COUNT - len(self._nodes)
        nodes_parameters = nodes_parameters[:max_new_nodes_count]
        nodes = list(self._executor.map(
                partial(RaftClusterNode.running_from_one_of_ports,
                        heartbeat=heartbeat),
                range(len(nodes_parameters)),
                *transpose(nodes_parameters)))
        self.update_states()
        self._nodes.extend(nodes)
        return nodes

    @rule(nodes=running_nodes)
    def delete_nodes(self, nodes: List[RaftClusterNode]) -> None:
        clusters_states_before = self.load_clusters_states(nodes)
        nodes_states_before = self.load_nodes_states(nodes)
        errors = list(self._executor.map(RaftClusterNode.delete, nodes))
        self.update_states()
        assert all(implication(error is None,
                               node_state.leader_node_id is not None
                               and implication(node_state.role is Role.LEADER,
                                               cluster_state.stable))
                   and implication(node_state.role is Role.LEADER
                                   and cluster_state.stable,
                                   error is None)
                   for cluster_state, node_state, error
                   in zip(clusters_states_before, nodes_states_before, errors))

    @rule(nodes=running_nodes)
    def initialize_nodes(self, nodes: List[RaftClusterNode]) -> None:
        _exhaust(self._executor.map(RaftClusterNode.initialize, nodes))
        self.update_states()
        clusters_states_after = self.load_clusters_states(nodes)
        assert all(cluster_state.id for cluster_state in clusters_states_after)

    @rule(data=strategies.data_objects,
          nodes=running_nodes)
    def log(self, data: DataObject, nodes: List[RaftClusterNode]) -> None:
        arguments = data.draw(
                strategies.to_nodes_with_log_arguments_lists(nodes)
        )
        if not arguments:
            return
        nodes_with_arguments = transpose(arguments)
        nodes, *rest_arguments = nodes_with_arguments
        nodes_states_before = self.load_nodes_states(nodes)
        errors = list(self._executor.map(RaftClusterNode.log, nodes,
                                         *rest_arguments))
        self.update_states()
        assert all(equivalence(error is None,
                               node_state_before.leader_node_id is not None)
                   for node_state_before, error in zip(nodes_states_before,
                                                       errors))

    @rule(target=running_nodes,
          nodes=consumes(shutdown_nodes))
    def restart_nodes(self, nodes: List[RaftClusterNode]
                      ) -> List[RaftClusterNode]:
        _exhaust(self._executor.map(RaftClusterNode.restart, nodes))
        self.update_states()
        self._nodes += nodes
        return nodes

    @rule(target=shutdown_nodes,
          nodes=consumes(running_nodes))
    def shutdown_nodes(self, nodes: List[RaftClusterNode]
                       ) -> List[RaftClusterNode]:
        _exhaust(self._executor.map(RaftClusterNode.stop, nodes))
        shutdown_nodes = frozenset(nodes)
        self._nodes = [node
                       for node in self._nodes
                       if node not in shutdown_nodes]
        self.update_states()
        return nodes

    def is_not_empty(self) -> bool:
        return bool(self._nodes)

    @precondition(is_not_empty)
    @rule(delay=strategies.delays)
    def stand_idle(self, delay: float) -> None:
        time.sleep(delay)
        self.update_states()

    def teardown(self) -> None:
        _exhaust(self._executor.map(RaftClusterNode.stop, self._nodes))
        self._executor.shutdown()

    def update_states(self) -> None:
        self._old_cluster_states, self._old_nodes_states = (
            self._new_cluster_states, self._new_nodes_states
        )
        self._new_cluster_states = self.load_clusters_states(self._nodes)
        self._new_nodes_states = self.load_nodes_states(self._nodes)

    def load_clusters_states(self, nodes: List[RaftClusterNode]
                             ) -> List[RaftClusterState]:
        return list(self._executor.map(RaftClusterNode.load_cluster_state,
                                       nodes))

    def load_nodes_states(self, nodes: List[RaftClusterNode]
                          ) -> List[RaftNodeState]:
        return list(self._executor.map(RaftClusterNode.load_node_state, nodes))


def _exhaust(iterator: Iterator) -> None:
    deque(iterator,
          maxlen=0)


TestCluster = RaftNetwork.TestCase
