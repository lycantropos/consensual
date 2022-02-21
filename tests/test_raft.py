from asyncio import (Future,
                     get_event_loop,
                     wait)
from collections import (Counter,
                         defaultdict)
from functools import partial
from operator import eq
from typing import (Any,
                    Awaitable,
                    Dict,
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
from .raft_communication import RaftCommunication
from .utils import (MAX_RUNNING_NODES_COUNT,
                    implication,
                    transpose)


class RaftNetwork(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.loop = get_event_loop()
        self._raw_nodes = {}
        self.communication = RaftCommunication(self._raw_nodes)
        self._nodes: List[RaftClusterNode] = []

    @property
    def nodes(self) -> Sequence[RaftClusterNode]:
        return self._nodes

    @nodes.setter
    def nodes(self, value: Sequence[RaftClusterNode]) -> None:
        self._raw_nodes.clear()
        self._raw_nodes.update({node.url.authority: node.raw
                                for node in value})
        self._nodes = value

    @invariant()
    def commit_length_monotonicity(self) -> None:
        assert all((node.old_node_state.commit_length
                    <= node.new_node_state.commit_length)
                   or (node.new_node_state.commit_length == 0
                       and is_resetted_node(node))
                   for node in self.nodes)

    @invariant()
    def leader_append_only(self) -> None:
        assert all(implication(node.new_node_state.role_kind
                               is RoleKind.LEADER,
                               (len(node.new_node_state.log)
                                >= len(node.old_node_state.log))
                               and all(map(eq, node.new_node_state.log,
                                           node.old_node_state.log)))
                   for node in self.nodes)

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
                   for node in self.nodes)

    @invariant()
    def log_matching(self) -> None:
        same_records = defaultdict(list)
        for node in self.nodes:
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
                                   for node in self.nodes]
        nodes_internal_commands = [[record.command
                                    for record in node.new_node_state.log
                                    if record.command.internal]
                                   for node in self.nodes]
        assert all(len(node.new_node_state.processed_external_commands)
                   + len(node.new_node_state.processed_internal_commands)
                   <= node.new_node_state.commit_length
                   for node in self.nodes)
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
                in zip(self.nodes, nodes_external_commands,
                       nodes_internal_commands)
        )

    @invariant()
    def election_safety(self) -> None:
        clusters_leaders_counts = defaultdict(Counter)
        for node in self.nodes:
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
                   for node in self.nodes)

    running_nodes = Bundle('running_nodes')
    shut_down_nodes = Bundle('shut_down_nodes')

    @rule(source_nodes=running_nodes,
          target_nodes=running_nodes)
    def add_nodes(self,
                  target_nodes: List[RaftClusterNode],
                  source_nodes: List[RaftClusterNode]) -> None:
        source_nodes = source_nodes[:len(target_nodes)]
        self.loop.run_until_complete(self._wait(
                [self._attach_node(target_node, source_node)
                 for target_node, source_node in zip(target_nodes,
                                                     source_nodes)]
        ))

    def is_not_full(self) -> bool:
        return len(self.nodes) < MAX_RUNNING_NODES_COUNT

    @precondition(is_not_full)
    @rule(target=running_nodes,
          heartbeat=strategies.heartbeats,
          nodes_parameters=strategies.running_nodes_parameters_lists)
    def create_nodes(self,
                     heartbeat: float,
                     nodes_parameters: List[Tuple[str, Sequence[int],
                                                  Dict[str, Processor], int]]
                     ) -> List[RaftClusterNode]:
        max_new_nodes_count = MAX_RUNNING_NODES_COUNT - len(self.nodes)
        nodes_parameters = nodes_parameters[:max_new_nodes_count]
        nodes = list(map(partial(RaftClusterNode,
                                 communication=self.communication,
                                 heartbeat=heartbeat,
                                 loop=self.loop),
                         range(len(nodes_parameters)),
                         *transpose(nodes_parameters)))
        succeeded = [node.start() for node in nodes]
        nodes = [node for node, success in zip(nodes, succeeded) if success]
        self.nodes += nodes
        return nodes

    @rule(nodes=running_nodes)
    def detach_nodes(self, nodes: List[RaftClusterNode]) -> None:
        self.loop.run_until_complete(self._wait([self._detach(node)
                                                 for node in nodes]))

    @rule(source_nodes=running_nodes,
          target_nodes=running_nodes)
    def detach_many_nodes(self,
                          source_nodes: List[RaftClusterNode],
                          target_nodes: List[RaftClusterNode]) -> None:
        self.loop.run_until_complete(self._wait(
                [self._detach_nodes(source_node, target_node)
                 for source_node, target_node in zip(source_nodes,
                                                     target_nodes)]
        ))

    @rule(data=strategies.data_objects,
          nodes=running_nodes)
    def log(self, data: DataObject, nodes: List[RaftClusterNode]) -> None:
        nodes_with_arguments = data.draw(
                strategies.to_nodes_with_log_arguments_lists(nodes)
        )
        self.loop.run_until_complete(self._wait(
                [self._enqueue(node, action, parameters)
                 for node, action, parameters in nodes_with_arguments]
        ))

    @rule(target=running_nodes,
          nodes=consumes(shut_down_nodes))
    def restart_nodes(self, nodes: List[RaftClusterNode]
                      ) -> List[RaftClusterNode]:
        succeeded = [node.restart() for node in nodes]
        nodes = [node for node, success in zip(nodes, succeeded) if success]
        self.nodes += nodes
        return nodes

    @rule(target=shut_down_nodes,
          nodes=consumes(running_nodes))
    def shutdown_nodes(self, nodes: List[RaftClusterNode]
                       ) -> List[RaftClusterNode]:
        for node in nodes:
            node.stop()
        shut_down_nodes = frozenset(nodes)
        self.nodes = [node
                      for node in self.nodes
                      if node not in shut_down_nodes]
        return nodes

    @rule(nodes=running_nodes)
    def solo_nodes(self, nodes: List[RaftClusterNode]) -> None:
        self.loop.run_until_complete(self._wait([self._solo(node)
                                                 for node in nodes]))

    def teardown(self) -> None:
        for node in self.nodes:
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
                            and (node.old_node_state.id
                                 in node.old_cluster_state.nodes_ids))
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

    async def _wait(self, awaitables: List[Awaitable]
                    ) -> Tuple[List[Future], List[Future]]:
        return (await wait([self.loop.create_task(awaitable)
                            for awaitable in awaitables])
                if awaitables
                else ([], []))


TestCluster = RaftNetwork.TestCase
