from asyncio import get_event_loop
from collections import (Counter,
                         defaultdict)
from functools import partial
from operator import eq
from typing import (Any,
                    Dict,
                    List,
                    Sequence,
                    Tuple)
from weakref import WeakValueDictionary

from hypothesis.stateful import (Bundle,
                                 RuleBasedStateMachine,
                                 consumes,
                                 invariant,
                                 multiple,
                                 precondition,
                                 rule)
from hypothesis.strategies import DataObject

from consensual.core.raft.role import RoleKind
from consensual.raft import Processor
from . import strategies
from .raft_cluster_node import (RaftClusterNode,
                                is_resetted_node)
from .raft_communication import RaftCommunication
from .utils import (MAX_NODES_COUNT,
                    equivalence,
                    implication,
                    transpose)


class RaftNetwork(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.loop = get_event_loop()
        self._raw_nodes = WeakValueDictionary()
        self.communication = RaftCommunication(self._raw_nodes)
        self._active_nodes: List[RaftClusterNode] = []
        self.deactivated_nodes: List[RaftClusterNode] = []

    @property
    def active_nodes(self) -> Sequence[RaftClusterNode]:
        return self._active_nodes

    @active_nodes.setter
    def active_nodes(self, value: Sequence[RaftClusterNode]) -> None:
        self._raw_nodes.clear()
        self._raw_nodes.update({node.url.authority: node.raw
                                for node in value})
        self._active_nodes = value

    @invariant()
    def commit_length_monotonicity(self) -> None:
        assert all((node.old_node_state.commit_length
                    <= node.new_node_state.commit_length)
                   or (node.new_node_state.commit_length == 0
                       and is_resetted_node(node))
                   for node in self.active_nodes)

    @invariant()
    def leader_append_only(self) -> None:
        assert all(implication(node.new_node_state.role_kind
                               is RoleKind.LEADER,
                               (len(node.new_node_state.log)
                                >= len(node.old_node_state.log))
                               and all(map(eq, node.new_node_state.log,
                                           node.old_node_state.log)))
                   for node in self.active_nodes)

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
                   for node in self.active_nodes)

    @invariant()
    def log_matching(self) -> None:
        same_records = defaultdict(list)
        for node in self.active_nodes:
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
                                   for node in self.active_nodes]
        nodes_internal_commands = [[record.command
                                    for record in node.new_node_state.log
                                    if record.command.internal]
                                   for node in self.active_nodes]
        assert all(
                len(node_state.processed_external_commands)
                + len(node_state.processed_internal_commands)
                <= node_state.commit_length
                for node in self.active_nodes
                for node_state in [node.new_node_state,
                                   node.old_node_state]
        ), [
            (len(node_state.processed_external_commands),
             len(node_state.processed_internal_commands),
             node_state.commit_length)
            for node in self.active_nodes
            for node_state in [node.new_node_state, node.old_node_state]
        ]
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
                in zip(self.active_nodes, nodes_external_commands,
                       nodes_internal_commands)
        )

    @invariant()
    def election_safety(self) -> None:
        clusters_leaders_counts = defaultdict(Counter)
        for node in self.active_nodes:
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
    def roles_completeness(self) -> None:
        assert all(equivalence(node_state.leader_node_id == node_state.id,
                               node_state.role_kind is RoleKind.LEADER)
                   for node in self.active_nodes
                   for node_state in [node.new_node_state,
                                      node.old_node_state])

    @invariant()
    def term_monotonicity(self) -> None:
        assert all(implication(not is_resetted_node(node),
                               node.new_node_state.term
                               >= node.old_node_state.term)
                   for node in self.active_nodes)

    running_nodes = Bundle('running_nodes')
    shut_down_nodes = Bundle('shut_down_nodes')

    @rule(source_node=running_nodes,
          target_node=running_nodes)
    def add_nodes(self,
                  target_node: RaftClusterNode,
                  source_node: RaftClusterNode) -> None:
        target_node.loop.run_until_complete(self._attach_node(target_node,
                                                              source_node))

    def is_not_full(self) -> bool:
        return (len(self.active_nodes)
                + len(self.deactivated_nodes)) < MAX_NODES_COUNT

    @precondition(is_not_full)
    @rule(target=running_nodes,
          heartbeat=strategies.heartbeats,
          nodes_parameters=strategies.running_nodes_parameters_lists)
    def create_nodes(self,
                     heartbeat: float,
                     nodes_parameters: List[Tuple[str, Sequence[int],
                                                  Dict[str, Processor], int]]
                     ) -> List[RaftClusterNode]:
        max_new_nodes_count = MAX_NODES_COUNT - len(self.active_nodes)
        nodes_parameters = nodes_parameters[:max_new_nodes_count]
        nodes = list(map(partial(RaftClusterNode,
                                 communication=self.communication,
                                 heartbeat=heartbeat),
                         *transpose(nodes_parameters)))
        succeeded = [node.start() for node in nodes]
        nodes = [node for node, success in zip(nodes, succeeded) if success]
        self.active_nodes += nodes
        return multiple(*nodes)

    @rule(node=running_nodes)
    def detach_node(self, node: RaftClusterNode) -> None:
        node.loop.run_until_complete(self._detach(node))

    @rule(source_node=running_nodes,
          target_node=running_nodes)
    def detach_nodes(self,
                     source_node: RaftClusterNode,
                     target_node: RaftClusterNode) -> None:
        target_node.loop.run_until_complete(self._detach_nodes(source_node,
                                                               target_node))

    @rule(data=strategies.data_objects,
          node=running_nodes)
    def log(self, data: DataObject, node: RaftClusterNode) -> None:
        arguments = data.draw(strategies.to_log_arguments_lists(node))
        for action, parameters in arguments:
            node.loop.run_until_complete(self._enqueue(node, action,
                                                       parameters))

    @rule(target=running_nodes,
          node=consumes(shut_down_nodes))
    def restart_node(self, node: RaftClusterNode) -> RaftClusterNode:
        if node.restart():
            self.active_nodes += [node]
            self.deactivated_nodes = [candidate
                                      for candidate in self.deactivated_nodes
                                      if candidate is not node]
            return node
        return multiple()

    @rule(target=shut_down_nodes,
          node=consumes(running_nodes))
    def shutdown_nodes(self, node: RaftClusterNode) -> RaftClusterNode:
        node.stop()
        self.active_nodes = [candidate
                             for candidate in self.active_nodes
                             if candidate is not node]
        self.deactivated_nodes.append(node)
        return node

    @rule(node=running_nodes)
    def solo_nodes(self, node: RaftClusterNode) -> None:
        node.loop.run_until_complete(self._solo(node))

    def teardown(self) -> None:
        for node in self.active_nodes:
            node.stop()

    async def _attach_node(self,
                           target_node: RaftClusterNode,
                           source_node: RaftClusterNode) -> None:
        error = await target_node.attach_nodes(source_node)
        assert implication(
                error is None,
                target_node.old_node_state.leader_node_id is not None
                and (source_node.old_node_state.id
                     not in target_node.old_cluster_state.nodes_ids)
                and implication(target_node.old_node_state.role_kind
                                is RoleKind.LEADER,
                                target_node.old_cluster_state.stable)
        )

    async def _detach(self, node: RaftClusterNode) -> None:
        error = await node.detach()
        assert (implication(error is None,
                            len(node.old_cluster_state.nodes_ids) == 1
                            or
                            (node.old_node_state.leader_node_id is not None
                             and implication(node.old_node_state.role_kind
                                             is RoleKind.LEADER,
                                             node.old_cluster_state.stable)))
                and implication((node.old_cluster_state.nodes_ids
                                 == [node.old_node_state.id])
                                or ((node.old_node_state.role_kind
                                     is RoleKind.LEADER)
                                    and node.old_cluster_state.stable),
                                error is None))

    async def _detach_nodes(self,
                            source_node: RaftClusterNode,
                            target_node: RaftClusterNode) -> None:
        error = await target_node.detach_nodes(source_node)
        assert (implication(error is None,
                            (source_node.old_node_state.id
                             == target_node.old_node_state.id)
                            if (len(target_node.old_cluster_state.nodes_ids)
                                == 1)
                            else
                            (target_node.old_node_state.leader_node_id
                             is not None
                             and
                             implication(target_node.old_node_state.role_kind
                                         is RoleKind.LEADER,
                                         target_node.old_cluster_state.stable)
                             and (source_node.new_node_state.id
                                  in target_node.old_cluster_state.nodes_ids)))
                and
                implication((source_node.old_node_state.id
                             == target_node.old_node_state.id)
                            if (target_node.old_cluster_state.nodes_ids
                                == [target_node.old_node_state.id])
                            else
                            ((target_node.old_node_state.role_kind
                              is RoleKind.LEADER)
                             and target_node.old_cluster_state.stable
                             and (source_node.new_node_state.id
                                  in target_node.old_cluster_state.nodes_ids)),
                            error is None))

    async def _enqueue(self,
                       node: RaftClusterNode,
                       action: str,
                       parameters: Any) -> None:
        error = await node.enqueue(action, parameters)
        assert (implication(error is None,
                            node.old_node_state.leader_node_id is not None
                            and ((node.old_node_state.role_kind
                                  is RoleKind.LEADER)
                                 or (node.old_node_state.id
                                     in node.old_cluster_state.nodes_ids)))
                and implication(node.old_node_state.role_kind
                                is RoleKind.LEADER,
                                error is None))

    async def _solo(self, node: RaftClusterNode) -> None:
        error = await node.solo()
        assert error is None
        assert (node.new_cluster_state.id
                and node.new_node_state.id in node.new_cluster_state.nodes_ids
                and len(node.new_cluster_state.nodes_ids) == 1
                and node.new_node_state.role_kind is RoleKind.LEADER)


TestCluster = RaftNetwork.TestCase
