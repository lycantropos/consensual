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
from .utils import (MAX_RUNNING_NODES_COUNT,
                    equivalence,
                    implication,
                    transpose)


class RaftNetwork(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self._executor = ThreadPoolExecutor()
        self._nodes: List[RaftClusterNode] = []

    @invariant()
    def commit_length_monotonicity(self) -> None:
        old_commit_lengths = {
            node.old_node_state.id: node.old_node_state.commit_length
            for node in self._nodes
        }
        new_commit_lengths = {
            node.new_node_state.id: node.new_node_state.commit_length
            for node in self._nodes
        }
        assert all(
                old_commit_lengths.get(node_id, 0) <= new_commit_length
                for node_id, new_commit_length in new_commit_lengths.items()
        )

    @invariant()
    def leader_append_only(self) -> None:
        assert all(implication(node.new_node_state.role is Role.LEADER,
                               (len(node.new_node_state.log)
                                >= len(node.old_node_state.log))
                               and all(map(eq, node.new_node_state.log,
                                           node.old_node_state.log)))
                   for node in self._nodes)

    @invariant()
    def leader_completeness(self) -> None:
        assert all(implication(node.new_node_state.role is Role.LEADER,
                               all(map(eq,
                                       node.new_node_state.log[
                                       :node.old_node_state.commit_length
                                       ],
                                       node.old_node_state.log[
                                       :node.old_node_state.commit_length
                                       ])))
                   for node in self._nodes)

    @invariant()
    def log_matching(self) -> None:
        same_records = defaultdict(list)
        for node in self._nodes:
            for index, record in enumerate(node.new_node_state.log):
                (same_records[(index, record.term, record.cluster_id)]
                 .append(record))
        assert all(map(eq, records, records[1:])
                   for records in same_records.values())

    @invariant()
    def election_safety(self) -> None:
        clusters_leaders_counts = defaultdict(Counter)
        for node in self._nodes:
            cluster_state, node_state = (node.new_cluster_state,
                                         node.new_node_state)
            clusters_leaders_counts[cluster_state.id][node_state.term] += (
                    node_state.role is Role.LEADER
            )
        assert all(
                leaders_count <= 1
                for cluster_leaders_counts in clusters_leaders_counts.values()
                for leaders_count in cluster_leaders_counts.values()
        )

    @invariant()
    def term_monotonicity(self) -> None:
        old_terms = {node.old_node_state.id: node.old_node_state.term
                     for node in self._nodes}
        new_terms = {node.new_node_state.id: node.new_node_state.term
                     for node in self._nodes}
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
        errors = list(self._executor.map(RaftClusterNode.add, target_nodes,
                                         source_nodes))
        assert all(
                implication(
                        error is None,
                        target_node.old_node_state.leader_node_id is not None
                        and (source_node.old_node_state.id
                             not in target_node.old_cluster_state.nodes_ids)
                        and implication(
                                target_node.old_node_state.role is Role.LEADER,
                                target_node.old_cluster_state.stable
                        )
                )
                for (target_node, source_node, error)
                in zip(target_nodes, source_nodes, errors)
        )
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
        self._nodes.extend(nodes)
        return nodes

    @rule(nodes=running_nodes)
    def delete_nodes(self, nodes: List[RaftClusterNode]) -> None:
        errors = list(self._executor.map(RaftClusterNode.delete, nodes))
        assert all(
                implication(error is None,
                            node.old_node_state.leader_node_id is not None
                            and implication(
                                    node.old_node_state.role is Role.LEADER,
                                    node.old_cluster_state.stable
                            ))
                and implication(node.old_node_state.role is Role.LEADER
                                and node.old_cluster_state.stable,
                                error is None)
                for node, error in zip(nodes, errors)
        )

    @rule(source_nodes=running_nodes,
          target_nodes=running_nodes)
    def delete_many_nodes(self,
                          source_nodes: List[RaftClusterNode],
                          target_nodes: List[RaftClusterNode]) -> None:
        errors = list(self._executor.map(RaftClusterNode.delete, target_nodes,
                                         source_nodes))
        assert all(
                implication(
                        error is None,
                        target_node.old_node_state.leader_node_id is not None
                        and implication(
                                target_node.old_node_state.role is Role.LEADER,
                                target_node.old_cluster_state.stable
                        )
                        and (source_node.new_node_state.id
                             in target_node.old_cluster_state.nodes_ids))
                and implication(
                        target_node.old_node_state.role is Role.LEADER
                        and target_node.old_cluster_state.stable
                        and (source_node.new_node_state.id
                             in target_node.old_cluster_state.nodes_ids),
                        error is None
                )
                for source_node, target_node, error
                in zip(source_nodes, target_nodes, errors)
        )

    @rule(nodes=running_nodes)
    def initialize_nodes(self, nodes: List[RaftClusterNode]) -> None:
        errors = list(self._executor.map(RaftClusterNode.initialize, nodes))
        assert all(error is None for error in errors)
        assert all(
                node.new_cluster_state.id
                and node.new_node_state.id in node.new_cluster_state.nodes_ids
                and len(node.new_cluster_state.nodes_ids) == 1
                and node.new_node_state.role is Role.LEADER
                for node in nodes
        )

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
        errors = list(self._executor.map(RaftClusterNode.log, nodes,
                                         *rest_arguments))
        assert all(implication(error is None,
                               node.old_node_state.leader_node_id is not None
                               and (node.old_node_state.id
                                    in node.old_cluster_state.nodes_ids))
                   and implication(node.old_node_state.role is Role.LEADER,
                                   error is None)
                   for node, error in zip(nodes, errors))

    @rule(target=running_nodes,
          nodes=consumes(shutdown_nodes))
    def restart_nodes(self, nodes: List[RaftClusterNode]
                      ) -> List[RaftClusterNode]:
        _exhaust(self._executor.map(RaftClusterNode.restart, nodes))
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
        _exhaust(self._executor.map(RaftClusterNode.update_states,
                                    self._nodes))


def _exhaust(iterator: Iterator) -> None:
    deque(iterator,
          maxlen=0)


TestCluster = RaftNetwork.TestCase
