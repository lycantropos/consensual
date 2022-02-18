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
                                 precondition,
                                 rule)
from hypothesis.strategies import DataObject

from consensual.core.raft.role import RoleKind
from consensual.raft import Processor
from . import strategies
from .raft_cluster_node import (RaftClusterNode,
                                is_resetted_node)
from .utils import (MAX_RUNNING_NODES_COUNT,
                    implication,
                    transpose)


class RaftNetwork(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self._executor = ThreadPoolExecutor()
        self._nodes: List[RaftClusterNode] = []

    @invariant()
    def commit_length_monotonicity(self) -> None:
        assert all((node.old_node_state.commit_length
                    <= node.new_node_state.commit_length)
                   or (node.new_node_state.commit_length == 0
                       and is_resetted_node(node))
                   for node in self._nodes)

    @invariant()
    def leader_append_only(self) -> None:
        assert all(implication(node.new_node_state.role_kind
                               is RoleKind.LEADER,
                               (len(node.new_node_state.log)
                                >= len(node.old_node_state.log))
                               and all(map(eq, node.new_node_state.log,
                                           node.old_node_state.log)))
                   for node in self._nodes)

    @invariant()
    def leader_completeness(self) -> None:
        assert all(implication(node.new_node_state.role_kind
                               is RoleKind.LEADER,
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
    def processing_completeness(self) -> None:
        nodes_external_commands = [[record.command
                                    for record in node.new_node_state.log
                                    if record.command.external]
                                   for node in self._nodes]
        nodes_internal_commands = [[record.command
                                    for record in node.new_node_state.log
                                    if record.command.internal]
                                   for node in self._nodes]
        assert all(len(node.new_node_state.processed_external_commands)
                   + len(node.new_node_state.processed_internal_commands)
                   <= node.new_node_state.commit_length
                   for node in self._nodes)
        assert all(
                (len(external_commands)
                 >= len(node.new_node_state.processed_external_commands))
                and all(map(eq,
                            node.new_node_state.processed_external_commands,
                            external_commands))
                and (len(internal_commands)
                     >= len(node.new_node_state.processed_internal_commands))
                and all(map(eq,
                            node.new_node_state.processed_internal_commands,
                            internal_commands))
                for node, external_commands, internal_commands
                in zip(self._nodes, nodes_external_commands,
                       nodes_internal_commands)
        )

    @invariant()
    def election_safety(self) -> None:
        clusters_leaders_counts = defaultdict(Counter)
        for node in self._nodes:
            cluster_state, node_state = (node.new_cluster_state,
                                         node.new_node_state)
            clusters_leaders_counts[cluster_state.id][node_state.term] += (
                    node_state.role_kind is RoleKind.LEADER
            )
        assert all(
                leaders_count <= 1
                for cluster_leaders_counts in clusters_leaders_counts.values()
                for leaders_count in cluster_leaders_counts.values()
        )

    @invariant()
    def term_monotonicity(self) -> None:
        assert all(node.new_node_state.term >= node.old_node_state.term
                   or is_resetted_node(node)
                   for node in self._nodes)

    running_nodes = Bundle('running_nodes')
    shutdown_nodes = Bundle('shutdown_nodes')

    @rule(source_nodes=running_nodes,
          target_nodes=running_nodes)
    def add_nodes(self,
                  target_nodes: List[RaftClusterNode],
                  source_nodes: List[RaftClusterNode]) -> None:
        source_nodes = source_nodes[:len(target_nodes)]
        errors = list(self._executor.map(RaftClusterNode.add, target_nodes,
                                         source_nodes))
        assert all(
                implication(
                        error is None,
                        target_node.old_node_state.leader_node_id is not None
                        and (source_node.old_node_state.id
                             not in target_node.old_cluster_state.nodes_ids)
                        and implication(target_node.old_node_state.role_kind
                                        is RoleKind.LEADER,
                                        target_node.old_cluster_state.stable)
                )
                for (target_node, source_node, error)
                in zip(target_nodes, source_nodes, errors)
        )

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
                implication(
                        error is None,
                        len(node.old_cluster_state.nodes_ids) == 1
                        or (node.old_node_state.leader_node_id is not None
                            and implication(node.old_node_state.role_kind
                                            is RoleKind.LEADER,
                                            node.old_cluster_state.stable))
                )
                and implication((node.old_cluster_state.nodes_ids
                                 == [node.old_node_state.id])
                                or ((node.old_node_state.role_kind
                                     is RoleKind.LEADER)
                                    and node.old_cluster_state.stable),
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
                        (source_node.old_node_state.id
                         == target_node.old_node_state.id)
                        if len(target_node.old_cluster_state.nodes_ids) == 1
                        else
                        (target_node.old_node_state.leader_node_id is not None
                         and implication(target_node.old_node_state.role_kind
                                         is RoleKind.LEADER,
                                         target_node.old_cluster_state.stable)
                         and (source_node.new_node_state.id
                              in target_node.old_cluster_state.nodes_ids))
                )
                and implication(
                        (source_node.old_node_state.id
                         == target_node.old_node_state.id)
                        if (target_node.old_cluster_state.nodes_ids
                            == [target_node.old_node_state.id])
                        else
                        ((target_node.old_node_state.role_kind
                          is RoleKind.LEADER)
                         and target_node.old_cluster_state.stable
                         and (source_node.new_node_state.id
                              in target_node.old_cluster_state.nodes_ids)),
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
                and node.new_node_state.role_kind is RoleKind.LEADER
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
                   and implication(node.old_node_state.role_kind
                                   is RoleKind.LEADER,
                                   error is None)
                   for node, error in zip(nodes, errors))

    @rule(target=running_nodes,
          nodes=consumes(shutdown_nodes))
    def restart_nodes(self, nodes: List[RaftClusterNode]
                      ) -> List[RaftClusterNode]:
        succeeded = list(self._executor.map(RaftClusterNode.restart, nodes))
        nodes = [node for node, success in zip(nodes, succeeded) if success]
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
