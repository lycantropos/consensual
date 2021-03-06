import logging.config
import sys
from asyncio import (AbstractEventLoop,
                     new_event_loop)
from contextlib import contextmanager
from functools import partial
from random import Random
from typing import (Any,
                    Dict,
                    List,
                    Mapping,
                    Optional)

from reprit.base import generate_repr
from yarl import URL

from consensual.core.raft.command import Command
from consensual.raft import (Node,
                             Processor)
from .raft_cluster_state import RaftClusterState
from .raft_communication import RaftCommunication
from .raft_node_state import RaftNodeState


class WrappedNode(Node):
    __slots__ = 'processed_external_commands', 'processed_internal_commands'

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.processed_external_commands = []
        self.processed_internal_commands = []

    def _trigger_commands(self, commands: List[Command]) -> None:
        super()._trigger_commands(
                [Command(action=command.action,
                         internal=command.internal,
                         parameters=(command.parameters,
                                     self.processed_external_commands
                                     if command.external
                                     else self.processed_internal_commands))
                 for command in commands]
        )

    def _process_commands(self,
                          commands: List[Command],
                          processors: Mapping[str, Processor]) -> None:
        assert all(command.external is commands[0].external
                   for command in commands)
        original_commands = [Command(action=command.action,
                                     internal=command.internal,
                                     parameters=command.parameters[0])
                             for command in commands]
        super()._process_commands(original_commands, processors)
        for command, original_command in zip(commands, original_commands):
            command.parameters[1].append(original_command)

    def _reset(self) -> None:
        self.processed_external_commands, self.processed_internal_commands = (
            [], []
        )
        super()._reset()


class RaftClusterNode:
    def __init__(self,
                 url: URL,
                 processors: Dict[str, Processor],
                 random_seed: int,
                 *,
                 communication: RaftCommunication,
                 heartbeat: float) -> None:
        self.heartbeat, self.processors, self.random_seed, self.url = (
            heartbeat, processors, random_seed, url
        )
        self._communication = communication
        self._random_latency_generator = partial(
                Random(self.random_seed).uniform, 0, self.heartbeat
        )
        self._loop = new_event_loop()
        self._logger = to_logger(self.url.authority)
        self.raw = self._to_raw()
        self._receiver = self._communication.to_receiver(self.raw)
        self._old_cluster_state: Optional[RaftClusterState] = None
        self._old_node_state: Optional[RaftNodeState] = None
        self._new_cluster_state: Optional[RaftClusterState] = None
        self._new_node_state: Optional[RaftNodeState] = None

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: 'RaftClusterNode') -> bool:
        return (self.url == other.url
                if isinstance(other, RaftClusterNode)
                else NotImplemented)

    def __hash__(self) -> int:
        return hash(self.url)

    @property
    def cluster_state(self) -> RaftClusterState:
        raw = self.raw
        return RaftClusterState(raw._cluster.id,
                                heartbeat=raw._cluster.heartbeat,
                                nodes_ids=list(raw._cluster.nodes_ids),
                                stable=raw._cluster.stable)

    @property
    def communication(self) -> RaftCommunication:
        return self._communication

    @property
    def loop(self) -> AbstractEventLoop:
        return self._loop

    @property
    def new_cluster_state(self) -> RaftClusterState:
        return self._new_cluster_state

    @property
    def new_node_state(self) -> RaftNodeState:
        return self._new_node_state

    @property
    def node_state(self) -> RaftNodeState:
        raw = self.raw
        return RaftNodeState(raw._id,
                             commit_length=raw._commit_length,
                             leader_node_id=raw._role.leader_node_id,
                             log=list(raw._history.log),
                             processed_external_commands
                             =list(raw.processed_external_commands),
                             processed_internal_commands
                             =list(raw.processed_internal_commands),
                             role_kind=raw._role.kind,
                             term=raw._role.term)

    @property
    def old_cluster_state(self) -> RaftClusterState:
        return self._old_cluster_state

    @property
    def old_node_state(self) -> RaftNodeState:
        return self._old_node_state

    async def attach_nodes(self,
                           node: 'RaftClusterNode',
                           *rest: 'RaftClusterNode') -> Optional[str]:
        with self._update_states():
            return await self.raw.attach_nodes([node.url
                                                for node in [node, *rest]])

    async def detach(self) -> Optional[str]:
        with self._update_states():
            return await self.raw.detach()

    async def detach_nodes(self,
                           node: 'RaftClusterNode',
                           *rest: 'RaftClusterNode') -> Optional[str]:
        with self._update_states():
            return await self.raw.detach_nodes([node.url
                                                for node in [node, *rest]])

    async def enqueue(self, action: str, parameters: Any) -> None:
        with self._update_states():
            return await self.raw.enqueue(action, parameters)

    async def solo(self) -> Optional[str]:
        with self._update_states():
            return await self.raw.solo()

    def restart(self) -> bool:
        assert self.loop is None
        assert self.raw is None
        assert self._receiver is None
        self._loop = new_event_loop()
        self.raw = self._to_raw()
        self._receiver = self._communication.to_receiver(self.raw)
        return self.start()

    def start(self) -> bool:
        with self._update_states():
            try:
                self._receiver.start()
            except OSError:
                return False
        return True

    def stop(self) -> None:
        self._loop.close()
        self._loop = None
        self._new_cluster_state = self._old_cluster_state = None
        self._new_node_state = self._old_node_state = None
        self._receiver.stop()
        self._receiver = None
        self.raw = None

    def _to_raw(self) -> WrappedNode:
        return WrappedNode.from_url(
                self.url,
                logger=self._logger,
                loop=self._loop,
                processors=self.processors,
                heartbeat=self.heartbeat,
                sender=self._communication.to_sender(
                        [self.url],
                        random_latency_generator=self._random_latency_generator
                )
        )

    @contextmanager
    def _update_states(self) -> None:
        node_state_before, cluster_state_before = (
            self.node_state, self.cluster_state
        )
        try:
            yield
        finally:
            self._old_node_state, self._new_node_state = (
                node_state_before, self.node_state
            )
            self._old_cluster_state, self._new_cluster_state = (
                cluster_state_before, self.cluster_state
            )


class FilterRecordsWithGreaterLevel:
    def __init__(self, max_level: int) -> None:
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.max_level


def is_resetted_node(node: RaftClusterNode) -> bool:
    return (not node.old_cluster_state.id
            and not node.new_cluster_state.id
            and not node.new_node_state.log
            and node.new_node_state.term == 0)


def to_logger(name: str,
              *,
              version: int = 1) -> logging.Logger:
    console_formatter = {'format': '[%(levelname)-8s %(name)s] %(msg)s'}
    formatters = {'console': console_formatter}
    stderr_handler = {'class': 'logging.StreamHandler',
                      'formatter': 'console',
                      'level': logging.WARNING,
                      'stream': sys.stderr.reconfigure(encoding='utf-8')}
    stdout_handler = {'class': 'logging.StreamHandler',
                      'filters': ['stdout'],
                      'formatter': 'console',
                      'level': logging.DEBUG,
                      'stream': sys.stdout.reconfigure(encoding='utf-8')}
    handlers = {'stderr': stderr_handler,
                'stdout': stdout_handler}
    loggers = {name: {'level': logging.INFO,
                      'handlers': ['stderr', 'stdout']}}
    config = {'disable_existing_loggers': False,
              'filters': {'stdout': {'()': FilterRecordsWithGreaterLevel,
                                     'max_level': logging.WARNING}},
              'formatters': formatters,
              'handlers': handlers,
              'loggers': loggers,
              'version': version}
    logging.config.dictConfig(config)
    return logging.getLogger(name)
