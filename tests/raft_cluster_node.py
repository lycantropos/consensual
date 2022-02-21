import logging.config
import sys
from asyncio import AbstractEventLoop
from contextlib import contextmanager
from typing import (Any,
                    Dict,
                    List,
                    Mapping,
                    Optional)

from reprit.base import generate_repr
from yarl import URL

from consensual.core.raft.command import Command
from consensual.raft import (Node,
                             Processor,
                             Sender)
from .raft_cluster_state import RaftClusterState
from .raft_communication import RaftCommunication
from .raft_node_state import RaftNodeState


class RaftClusterNode:
    def __init__(self,
                 index: int,
                 url: URL,
                 processors: Dict[str, Processor],
                 random_seed: int,
                 *,
                 communication: RaftCommunication,
                 loop: AbstractEventLoop,
                 heartbeat: float) -> None:
        (
            self.index, self.heartbeat, self.processors, self.random_seed,
            self.url
        ) = index, heartbeat, processors, random_seed, url
        self._communication = communication
        self._loop = loop
        self.raw = WrappedNode.from_url(self.url,
                                        logger=to_logger(self.url.authority),
                                        loop=self._loop,
                                        processors=self.processors,
                                        sender=self._to_sender())
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
        node = self.raw
        return RaftClusterState(node._cluster_state.id,
                                heartbeat=node._cluster_state.heartbeat,
                                nodes_ids=list(node._cluster_state.nodes_ids),
                                stable=node._cluster_state.stable)

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
        node = self.raw
        return RaftNodeState(
                node._id,
                commit_length=node._commit_length,
                leader_node_id=node._role.leader_node_id,
                log=list(node._history.log),
                processed_external_commands
                =list(node.processed_external_commands),
                processed_internal_commands
                =list(node.processed_internal_commands),
                role_kind=node._role.kind,
                term=node._role.term
        )

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
        assert self.raw is None
        assert self._receiver is None
        self.raw = WrappedNode.from_url(self.url,
                                        logger=to_logger(self.url.authority),
                                        loop=self._loop,
                                        processors=self.processors,
                                        sender=self._to_sender())
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
        self._new_cluster_state = self._old_cluster_state = None
        self._new_node_state = self._old_node_state = None
        self._receiver = None
        self.raw = None

    def _to_sender(self) -> Sender:
        return self._communication.to_sender([self.url])
        return SenderWithSimulatedLatency([self.url],
                                          max_delay=2 * self.heartbeat,
                                          random_seed=self.random_seed)

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


class WrappedNode(Node):
    __slots__ = 'processed_external_commands', 'processed_internal_commands'

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.processed_external_commands = []
        self.processed_internal_commands = []

    def _process_commands(self,
                          commands: List[Command],
                          processors: Mapping[str, Processor]) -> None:
        assert all(command.external is commands[0].external
                   for command in commands)
        processed_commands = ((self.processed_external_commands
                               if commands[0].external
                               else self.processed_internal_commands)
                              if commands
                              else None)
        super()._process_commands(commands, processors)
        if not commands:
            return
        processed_commands += commands

    def _reset(self) -> None:
        super()._reset()
        self.processed_external_commands = []
        self.processed_internal_commands = []


def to_logger(name: str,
              *,
              version: int = 1) -> logging.Logger:
    console_formatter = {'format': '[%(levelname)-8s %(name)s] %(msg)s'}
    formatters = {'console': console_formatter}
    stderr_handler_config = {
        'class': 'logging.StreamHandler',
        'level': logging.WARNING,
        'formatter': 'console',
        'stream': sys.stderr,
    }
    stdout_handler_config = {
        'class': 'logging.StreamHandler',
        'level': logging.DEBUG,
        'formatter': 'console',
        'stream': sys.stdout,
        'filters': ['stdout']
    }
    handlers = {'stdout': stdout_handler_config,
                'stderr': stderr_handler_config}
    loggers = {name: {'level': logging.DEBUG,
                      'handlers': ('stderr', 'stdout')}}
    config = {'formatters': formatters,
              'handlers': handlers,
              'loggers': loggers,
              'version': version,
              'filters': {
                  'stdout': {
                      '()': FilterRecordsWithGreaterLevel,
                      'max_level': logging.WARNING,
                  }
              }}
    logging.config.dictConfig(config)
    return logging.getLogger(name)
