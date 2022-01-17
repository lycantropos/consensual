import asyncio
import dataclasses
import enum
import logging.config
import random
import traceback
from asyncio import get_event_loop
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType
from typing import (Any,
                    Callable,
                    Collection,
                    Dict,
                    Generic,
                    List,
                    Mapping,
                    MutableMapping,
                    MutableSet,
                    NoReturn,
                    Optional,
                    TypeVar,
                    Union)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

from aiohttp import (ClientError,
                     ClientSession,
                     hdrs,
                     web,
                     web_ws)
from reprit import seekers
from reprit.base import generate_repr
from yarl import URL

START_CONFIGURATION_UPDATE_COMMAND_PATH = '0'
END_CONFIGURATION_UPDATE_COMMAND_PATH = '1'
assert (START_CONFIGURATION_UPDATE_COMMAND_PATH
        and not START_CONFIGURATION_UPDATE_COMMAND_PATH.startswith('/'))
assert (END_CONFIGURATION_UPDATE_COMMAND_PATH
        and not END_CONFIGURATION_UPDATE_COMMAND_PATH.startswith('/'))

Time = Union[float, int]
NodeId = str
_T = TypeVar('_T')
Processor = Callable[['Node', _T], None]
Term = int


class ClusterConfiguration:
    def __init__(self,
                 nodes_urls: Mapping[NodeId, URL],
                 *,
                 active_nodes_ids: Optional[Collection[NodeId]] = None,
                 heartbeat: Time = 5) -> None:
        self._heartbeat = heartbeat
        self._nodes_urls = nodes_urls
        self._active_nodes_ids = (set(nodes_urls.keys())
                                  if active_nodes_ids is None
                                  else set(active_nodes_ids))

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: Any) -> Any:
        return ((self.heartbeat == other.heartbeat
                 and self.nodes_urls == other.nodes_urls
                 and self.active_nodes_ids == other.active_nodes_ids)
                if isinstance(other, ClusterConfiguration)
                else NotImplemented)

    @property
    def active_nodes_ids(self) -> Collection[NodeId]:
        return self._active_nodes_ids

    @property
    def heartbeat(self) -> Time:
        return self._heartbeat

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.nodes_urls.keys()

    @property
    def nodes_urls(self) -> Mapping[NodeId, URL]:
        return self._nodes_urls

    @classmethod
    def from_json(cls,
                  *,
                  nodes_urls: Dict[NodeId, str],
                  **kwargs: Any) -> 'ClusterConfiguration':
        return cls(nodes_urls
                   ={node_id: URL(raw_node_url)
                     for node_id, raw_node_url in nodes_urls.items()},
                   **kwargs)

    def as_json(self) -> Dict[str, Any]:
        return {
            'nodes_urls': {node_id: str(node_url)
                           for node_id, node_url in self.nodes_urls.items()},
            'active_nodes_ids': list(self.active_nodes_ids),
            'heartbeat': self.heartbeat,
        }

    def activate(self, node_id: NodeId) -> None:
        assert node_id in self.nodes_ids
        assert node_id not in self.active_nodes_ids
        self._active_nodes_ids.add(node_id)

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return len(nodes_ids) >= ceil_division(len(self.nodes_ids) + 1, 2)


class TransitionalClusterConfiguration:
    def __init__(self, old: ClusterConfiguration, new: ClusterConfiguration):
        self.new, self.old = new, old

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: Any) -> Any:
        return (self.old == other.old and self.new == other.new
                if isinstance(other, TransitionalClusterConfiguration)
                else NotImplemented)

    @property
    def nodes_urls(self) -> Mapping[NodeId, URL]:
        return {**self.old.nodes_urls, **self.new.nodes_urls}

    @property
    def heartbeat(self) -> Time:
        return self.new.heartbeat

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.old.nodes_urls.keys() | self.new.nodes_urls.keys()

    @property
    def active_nodes_ids(self) -> Collection[NodeId]:
        return set(self.old.active_nodes_ids) | set(self.new.active_nodes_ids)

    @classmethod
    def from_json(cls,
                  *,
                  old: Dict[str, Any],
                  new: Dict[str, Any]) -> 'TransitionalClusterConfiguration':
        return cls(old=ClusterConfiguration.from_json(**old),
                   new=ClusterConfiguration.from_json(**new))

    def as_json(self) -> Dict[str, Any]:
        return {'old': self.old.as_json(), 'new': self.new.as_json()}

    def activate(self, node_id: NodeId) -> None:
        self.new.activate(node_id)

    def has_majority(self, nodes_ids: Collection[NodeId]) -> bool:
        return (self.old.has_majority(nodes_ids)
                and self.new.has_majority(nodes_ids))


AnyClusterConfiguration = Union[ClusterConfiguration,
                                TransitionalClusterConfiguration]


@dataclasses.dataclass(frozen=True)
class Command(Generic[_T]):
    path: str
    parameters: Any


@dataclasses.dataclass(frozen=True)
class Record:
    command: Command
    term: Term

    @classmethod
    def from_json(cls,
                  *,
                  command: Dict[str, Any],
                  term: Term) -> 'Record':
        return cls(command=Command(**command),
                   term=term)


class CallPath(enum.IntEnum):
    LOG = 0
    SYNC = 1
    UPDATE = 2
    VOTE = 3


class Role(enum.IntEnum):
    CANDIDATE = 0
    FOLLOWER = 1
    LEADER = 2


class Call(Protocol):
    def as_json(self) -> Dict[str, Any]:
        return {}


@dataclasses.dataclass(frozen=True)
class LogCall:
    command: Command

    @classmethod
    def from_json(cls, command: Dict[str, Any]) -> 'LogCall':
        return cls(Command(**command))

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class LogReply:
    error: Optional[str]

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class SyncCall:
    node_id: NodeId
    term: Term
    prefix_length: int
    prefix_term: Term
    commit_length: int
    suffix: List[Record]

    @classmethod
    def from_json(cls,
                  *,
                  suffix: List[Dict[str, Any]],
                  **kwargs: Any) -> 'SyncCall':
        return cls(suffix=[Record.from_json(**raw_record)
                           for raw_record in suffix],
                   **kwargs)

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class SyncReply:
    node_id: NodeId
    term: Term
    accepted_length: int
    successful: bool

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class UpdateCall:
    configuration: ClusterConfiguration

    def as_json(self) -> Dict[str, Any]:
        return {'configuration': self.configuration.as_json()}

    @classmethod
    def from_json(cls, configuration: Dict[str, Any]) -> 'UpdateCall':
        return cls(ClusterConfiguration.from_json(**configuration))


@dataclasses.dataclass(frozen=True)
class UpdateReply:
    error: Optional[str]

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class VoteCall:
    node_id: NodeId
    term: Term
    log_length: int
    log_term: Term

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class VoteReply:
    node_id: NodeId
    term: Term
    supports: bool

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


class _Result(Protocol[_T]):
    value: _T


class _Ok:
    def __init__(self, value: _T) -> None:
        self.value = value

    __repr__ = generate_repr(__init__)


class _Error:
    def __init__(self, exception: Exception) -> None:
        self.exception = exception

    __repr__ = generate_repr(__init__)

    @property
    def value(self) -> NoReturn:
        raise self.exception


class Communication:
    HTTP_METHOD = hdrs.METH_PATCH
    assert HTTP_METHOD is not hdrs.METH_POST

    def __init__(self, configuration: AnyClusterConfiguration) -> None:
        self._configuration = configuration
        self._loop = asyncio.get_event_loop()
        self._calls = {node_id: asyncio.Queue()
                       for node_id in self.configuration.nodes_ids}
        self._latencies: Dict[NodeId, deque] = {
            node_id: deque([0],
                           maxlen=10)
            for node_id in self.configuration.nodes_ids}
        self._results = {node_id: {path: asyncio.Queue() for path in CallPath}
                         for node_id in self.configuration.nodes_ids}
        self._session = ClientSession(loop=self._loop)
        self._senders = {node_id: self._loop.create_task(self._sender(node_id))
                         for node_id in self.configuration.nodes_ids}

    __repr__ = generate_repr(__init__)

    @property
    def configuration(self) -> AnyClusterConfiguration:
        return self._configuration

    @configuration.setter
    def configuration(self, value: AnyClusterConfiguration) -> None:
        new_nodes_ids, old_nodes_ids = (set(value.nodes_ids),
                                        set(self._configuration.nodes_ids))
        for removed_node_id in old_nodes_ids - new_nodes_ids:
            self._senders.pop(removed_node_id).cancel()
            del (self._calls[removed_node_id],
                 self._latencies[removed_node_id],
                 self._results[removed_node_id])
        self._configuration = value
        for added_node_id in new_nodes_ids - old_nodes_ids:
            self._calls[added_node_id] = asyncio.Queue()
            self._latencies[added_node_id] = deque([0],
                                                   maxlen=10)
            self._results[added_node_id] = {path: asyncio.Queue()
                                            for path in CallPath}
            self._senders[added_node_id] = self._loop.create_task(
                    self._sender(added_node_id))

    async def send(self, to: NodeId, path: CallPath, call: Call) -> Any:
        self._calls[to].put_nowait((path, call))
        result = await self._results[to][path].get()
        return result.value

    async def _sender(self, to: NodeId) -> None:
        url = self.configuration.nodes_urls[to]
        calls, results, latencies = (self._calls[to], self._results[to],
                                     self._latencies[to])
        path, call = await calls.get()
        to_time = self._loop.time
        while True:
            try:
                async with self._session.ws_connect(
                        url,
                        method=self.HTTP_METHOD,
                        timeout=self.configuration.heartbeat,
                        heartbeat=self.configuration.heartbeat) as connection:
                    call_start = to_time()
                    await connection.send_json({'path': path,
                                                'data': call.as_json()})
                    async for reply in connection:
                        reply: web_ws.WSMessage
                        reply_end = to_time()
                        latency = reply_end - call_start
                        latencies.append(latency)
                        results[path].put_nowait(_Ok(reply.json()))
                        path, call = await calls.get()
                        call_start = to_time()
                        await connection.send_json({'path': path,
                                                    'data': call.as_json()})
            except (ClientError, OSError) as exception:
                results[path].put_nowait(_Error(exception))
                path, call = await calls.get()

    def to_expected_broadcast_time(self) -> float:
        return sum(max(latencies) for latencies in self._latencies.values())


class NodeState:
    def __init__(self,
                 nodes_ids: Collection[NodeId],
                 *,
                 log: Optional[List[Record]] = None,
                 supported_node_id: Optional[NodeId] = None,
                 term: Term = 0) -> None:
        self._nodes_ids = nodes_ids
        self._accepted_lengths = {node_id: 0 for node_id in self.nodes_ids}
        self._commit_length = 0
        self._leader_node_id = None
        self._log = [] if log is None else log
        self._role = Role.FOLLOWER
        self._sent_lengths = {node_id: 0 for node_id in self.nodes_ids}
        self._supported_node_id = supported_node_id
        self._supporters_nodes_ids = set()
        self._term = term

    @property
    def accepted_lengths(self) -> MutableMapping[NodeId, int]:
        assert self.role is Role.LEADER
        return self._accepted_lengths

    @property
    def commit_length(self) -> int:
        return self._commit_length

    @commit_length.setter
    def commit_length(self, value: int) -> None:
        assert value >= 0
        assert self.commit_length < value
        self._commit_length = value

    @property
    def leader_node_id(self) -> Optional[NodeId]:
        return self._leader_node_id

    @leader_node_id.setter
    def leader_node_id(self, value: Optional[NodeId]) -> None:
        assert value is None or value in self.nodes_ids
        self._leader_node_id = value

    @property
    def log(self) -> List[Record]:
        return self._log

    @property
    def log_term(self) -> Term:
        return self.log[-1].term if self.log else 0

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self._nodes_ids

    @nodes_ids.setter
    def nodes_ids(self, value: Collection[NodeId]) -> None:
        new_nodes_ids, old_nodes_ids = set(value), set(self.nodes_ids)
        for removed_node_id in old_nodes_ids - new_nodes_ids:
            del (self._accepted_lengths[removed_node_id],
                 self._sent_lengths[removed_node_id])
        added_nodes_ids = new_nodes_ids - old_nodes_ids
        self._accepted_lengths.update({node_id: 0
                                       for node_id in added_nodes_ids})
        self._sent_lengths.update({node_id: 0 for node_id in added_nodes_ids})
        self._nodes_ids = value

    @property
    def role(self) -> Role:
        return self._role

    @role.setter
    def role(self, value: Role) -> None:
        assert value in Role
        self._role = value

    @property
    def sent_lengths(self) -> MutableMapping[NodeId, int]:
        assert self.role is Role.LEADER
        return self._sent_lengths

    @property
    def supported_node_id(self) -> Optional[NodeId]:
        return self._supported_node_id

    @supported_node_id.setter
    def supported_node_id(self, value: Optional[NodeId]) -> None:
        assert value is None or value in self.nodes_ids
        self._supported_node_id = value

    @property
    def supporters_nodes_ids(self) -> MutableSet[NodeId]:
        return self._supporters_nodes_ids

    @property
    def term(self) -> Term:
        return self._term

    @term.setter
    def term(self, value: Term) -> None:
        assert value > self.term
        self._term = value


class Node:
    __slots__ = ('_app', '_commands_executor', '_communication',
                 '_configuration', '_election_duration', '_election_task',
                 '_id', '_last_heartbeat_time', '_logger', '_loop',
                 '_patch_routes', '_processors', '_reelection_lag',
                 '_reelection_task', '_state', '_sync_task')

    def __init__(self,
                 id_: NodeId,
                 state: NodeState,
                 configuration: ClusterConfiguration,
                 *,
                 logger: Optional[logging.Logger] = None,
                 processors: Dict[str, Processor]) -> None:
        self._id = id_
        self._configuration = configuration
        self._state = state
        self._last_heartbeat_time = -self.configuration.heartbeat
        self._logger = logging.getLogger() if logger is None else logger
        self._loop = get_event_loop()
        self._app = web.Application()
        self._communication = Communication(self.configuration)
        self._commands_executor = ThreadPoolExecutor(max_workers=1)
        self._election_duration = 0
        self._election_task = self._loop.create_future()
        self._reelection_lag = 0
        self._reelection_task = self._loop.create_future()
        self._sync_task = self._loop.create_future()
        self._patch_routes = {
            CallPath.LOG: (LogCall.from_json, self._process_log_call),
            CallPath.SYNC: (SyncCall.from_json, self._process_sync_call),
            CallPath.UPDATE: (UpdateCall.from_json, self._process_update_call),
            CallPath.VOTE: (VoteCall, self._process_vote_call),
        }
        self._app.router.add_delete('/', self._handle_delete)
        self._app.router.add_post('/', self._handle_post)
        self._app.router.add_route(self._communication.HTTP_METHOD, '/',
                                   self._handle_communication)
        self._processors = processors
        for path in self.processors.keys():
            self._app.router.add_post(path, self._handle_record)
        self._processors[START_CONFIGURATION_UPDATE_COMMAND_PATH] = (
            Node._start_configuration_update)
        self._processors[END_CONFIGURATION_UPDATE_COMMAND_PATH] = (
            Node._end_configuration_update)

    __repr__ = generate_repr(__init__,
                             field_seeker=seekers.complex_)

    @property
    def configuration(self) -> AnyClusterConfiguration:
        return self._configuration

    @property
    def id(self) -> NodeId:
        return self._id

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def processors(self) -> Mapping[str, Processor]:
        return MappingProxyType(self._processors)

    @property
    def state(self) -> NodeState:
        return self._state

    def run(self) -> None:
        self._start_reelection_timer()
        url = self.configuration.nodes_urls[self.id]
        web.run_app(self._app,
                    host=url.host,
                    port=url.port,
                    loop=self._loop)

    @property
    def _passive(self) -> bool:
        return self.id not in self.configuration.active_nodes_ids

    async def _agitate_voter(self, node_id: NodeId) -> None:
        reply = await self._call_vote(node_id)
        await self._process_vote_reply(reply)

    async def _handle_communication(self, request: web.Request
                                    ) -> web_ws.WebSocketResponse:
        websocket = web_ws.WebSocketResponse()
        await websocket.prepare(request)
        routes = self._patch_routes
        async for message in websocket:
            message: web_ws.WSMessage
            raw_call = message.json()
            call_path = CallPath(raw_call['path'])
            if self._passive and call_path is not CallPath.SYNC:
                raise web.HTTPBadRequest()
            call_cls, processor = routes[call_path]
            call = call_cls(**raw_call['data'])
            reply = await processor(call)
            await websocket.send_json(reply.as_json())
        return websocket

    async def _handle_delete(self, request: web.Request) -> web.Response:
        if self._passive:
            raise web.HTTPBadRequest()
        self.logger.debug(f'{self.id} gets removed')
        rest_nodes_urls = dict(self.configuration.nodes_urls)
        del rest_nodes_urls[self.id]
        call = UpdateCall(ClusterConfiguration(
                rest_nodes_urls,
                heartbeat=self.configuration.heartbeat))
        reply = await self._process_update_call(call)
        return web.json_response(reply.as_json())

    async def _handle_post(self, request: web.Request) -> web.Response:
        if self._passive:
            raise web.HTTPBadRequest()
        raw_nodes_urls_to_add = await request.json()
        nodes_urls_to_add = {
            node_id: URL(raw_node_url)
            for node_id, raw_node_url in raw_nodes_urls_to_add.items()}
        nodes_ids_to_add = nodes_urls_to_add.keys()
        assert not nodes_ids_to_add & set(self.configuration.nodes_ids)
        self.logger.debug(f'{self.id} adds {nodes_urls_to_add} '
                          f'to {self.configuration.nodes_urls}')
        call = UpdateCall(ClusterConfiguration(
                {**self.configuration.nodes_urls, **nodes_urls_to_add},
                heartbeat=self.configuration.heartbeat))
        reply = await self._process_update_call(call)
        return web.json_response(reply.as_json())

    async def _handle_record(self, request: web.Request) -> web.Response:
        if self._passive:
            raise web.HTTPBadRequest()
        parameters = await request.json()
        reply = await self._process_log_call(
                LogCall(Command(path=request.path,
                                parameters=parameters)))
        return web.json_response(reply.as_json())

    async def _call_sync(self, node_id: NodeId) -> SyncReply:
        prefix_length = self.state.sent_lengths[node_id]
        call = SyncCall(node_id=self.id,
                        term=self.state.term,
                        prefix_length=prefix_length,
                        prefix_term=(self.state.log[prefix_length - 1].term
                                     if prefix_length
                                     else 0),
                        commit_length=self.state.commit_length,
                        suffix=self.state.log[prefix_length:])
        try:
            raw_reply = await self._send_call(node_id, CallPath.SYNC, call)
        except (ClientError, OSError):
            return SyncReply(node_id=node_id,
                             term=self.state.term,
                             accepted_length=0,
                             successful=False)
        else:
            return SyncReply(**raw_reply)

    async def _call_vote(self, node_id: NodeId) -> VoteReply:
        call = VoteCall(node_id=self.id,
                        term=self.state.term,
                        log_length=len(self.state.log),
                        log_term=self.state.log_term)
        try:
            raw_reply = await self._send_call(node_id, CallPath.VOTE, call)
        except (ClientError, OSError):
            return VoteReply(node_id=node_id,
                             term=self.state.term,
                             supports=False)
        else:
            return VoteReply(**raw_reply)

    async def _process_log_call(self, call: LogCall) -> LogReply:
        assert self.state.leader_node_id is not None
        if self.state.role is Role.LEADER:
            self.state.log.append(Record(call.command, self.state.term))
            await self._sync_followers_once()
            assert self.state.accepted_lengths[self.id] == len(self.state.log)
            return LogReply(error=None)
        else:
            assert self.state.role is Role.FOLLOWER
            try:
                raw_reply = await self._send_call(self.state.leader_node_id,
                                                  CallPath.LOG, call)
            except (ClientError, OSError) as exception:
                return LogReply(error=format_exception(exception))
            else:
                return LogReply(**raw_reply)

    async def _process_sync_call(self, call: SyncCall) -> SyncReply:
        self.logger.debug(f'{self.id} processes {call}')
        self._last_heartbeat_time = self._to_time()
        self._restart_reelection_timer()
        if call.term > self.state.term:
            update_term(self.state, call.term)
            self._cancel_election_timer()
        if call.term == self.state.term and call.node_id != self.id:
            self.state.leader_node_id = call.node_id
            self.state.role = Role.FOLLOWER
        if (call.term == self.state.term
                and (len(self.state.log) >= call.prefix_length
                     and (call.prefix_length == 0
                          or (self.state.log[call.prefix_length - 1].term
                              == call.prefix_term)))):
            self._append_records(call.prefix_length, call.suffix)
            if call.commit_length > self.state.commit_length:
                self._commit(self.state.log[self.state.commit_length
                                            :call.commit_length])
            if self._passive:
                self.configuration.activate(self.id)
            return SyncReply(node_id=self.id,
                             term=self.state.term,
                             accepted_length=(call.prefix_length
                                              + len(call.suffix)),
                             successful=True)
        else:
            return SyncReply(node_id=self.id,
                             term=self.state.term,
                             accepted_length=0,
                             successful=False)

    async def _process_sync_reply(self, reply: SyncReply) -> None:
        self.logger.debug(f'{self.id} processes {reply}')
        if reply.term == self.state.term and self.state.role is Role.LEADER:
            if (reply.successful
                    and (reply.accepted_length
                         >= self.state.accepted_lengths[reply.node_id])):
                if reply.node_id not in self.configuration.active_nodes_ids:
                    self.configuration.activate(reply.node_id)
                self.state.accepted_lengths[reply.node_id] = (
                    reply.accepted_length)
                self.state.sent_lengths[reply.node_id] = reply.accepted_length
                self._try_commit()
            elif self.state.sent_lengths[reply.node_id] > 0:
                self.state.sent_lengths[reply.node_id] = (
                        self.state.sent_lengths[reply.node_id] - 1)
                await self._sync_follower(reply.node_id)
        elif reply.term > self.state.term:
            update_term(self.state, reply.term)
            self.state.role = Role.FOLLOWER
            self._cancel_election_timer()

    async def _process_update_call(self, call: UpdateCall) -> UpdateReply:
        assert self.state.leader_node_id is not None
        self.logger.debug(f'{self.id} processes {call}')
        if self.state.role is not Role.LEADER:
            raw_reply = await self._send_call(self.state.leader_node_id,
                                              CallPath.UPDATE, call)
            return UpdateReply(**raw_reply)
        if self.configuration == call.configuration:
            reply = UpdateReply(error='got the same configuration')
        else:
            transitional_configuration = TransitionalClusterConfiguration(
                    self.configuration, call.configuration)
            self.state.log.append(Record(
                    Command(path=START_CONFIGURATION_UPDATE_COMMAND_PATH,
                            parameters=transitional_configuration.as_json()),
                    self.state.term))
            self._update_configuration(transitional_configuration)
            await self._sync_followers_once()
            reply = UpdateReply(error=None)
        return reply

    async def _process_vote_call(self, call: VoteCall) -> VoteReply:
        self.logger.debug(f'{self.id} processes {call}')
        leader_may_be_alive = (self._to_time() - self._last_heartbeat_time
                               < self.configuration.heartbeat)
        if leader_may_be_alive:
            self.logger.debug(f'{self.id} leader {self.state.leader_node_id} '
                              f'can be alive, '
                              f'skipping voting for {call.node_id}')
            return VoteReply(node_id=self.id,
                             term=self.state.term,
                             supports=False)
        if call.term > self.state.term:
            update_term(self.state, call.term)
            self.state.role = Role.FOLLOWER
        if (call.term == self.state.term
                and ((call.log_term, call.log_length)
                     >= (self.state.log_term, len(self.state.log)))
                and (self.state.supported_node_id is None
                     or self.state.supported_node_id == call.node_id)):
            self.state.supported_node_id = call.node_id
            return VoteReply(node_id=self.id,
                             term=self.state.term,
                             supports=True)
        else:
            return VoteReply(node_id=self.id,
                             term=self.state.term,
                             supports=False)

    async def _process_vote_reply(self, reply: VoteReply) -> None:
        self.logger.debug(f'{self.id} processes {reply}')
        if (self.state.role is Role.CANDIDATE
                and reply.term == self.state.term
                and reply.supports):
            self.state.supporters_nodes_ids.add(reply.node_id)
            if self.configuration.has_majority(
                    self.state.supporters_nodes_ids):
                self._lead()
        elif reply.term > self.state.term:
            update_term(self.state, reply.term)
            self.state.role = Role.FOLLOWER
            self._cancel_election_timer()

    async def _run_election(self) -> None:
        self.logger.debug(f'{self.id} runs election '
                          f'for term {self.state.term + 1}')
        update_term(self.state, self.state.term + 1)
        self.state.role = Role.CANDIDATE
        self.state.supporters_nodes_ids.clear()
        start = self._to_time()
        try:
            await asyncio.wait_for(
                    asyncio.gather(*[self._agitate_voter(node_id)
                                     for node_id
                                     in self.configuration.active_nodes_ids]),
                    self._election_duration)
        finally:
            duration = self._to_time() - start
            self.logger.debug(f'{self.id} election for term {self.state.term} '
                              f'took {duration}s, '
                              f'timeout: {self._election_duration}, '
                              f'role: {self.state.role.name}, '
                              f'supporters count: '
                              f'{len(self.state.supporters_nodes_ids)}'
                              f'/{len(self.configuration.active_nodes_ids)}')
            await asyncio.sleep(self._election_duration - duration)

    async def _send_call(self,
                         to: NodeId,
                         path: CallPath,
                         call: Call) -> Dict[str, Any]:
        result = await self._communication.send(to, path, call)
        return result

    async def _sync_follower(self, node_id: NodeId) -> None:
        reply = await self._call_sync(node_id)
        await self._process_sync_reply(reply)

    async def _sync_followers(self) -> None:
        self.logger.info(f'{self.id} syncs followers')
        while self.state.role is Role.LEADER:
            start = self._to_time()
            await self._sync_followers_once()
            duration = self._to_time() - start
            self.logger.debug(f'{self.id} followers\' sync took {duration}s')
            await asyncio.sleep(
                    self.configuration.heartbeat - duration
                    - self._communication.to_expected_broadcast_time())

    async def _sync_followers_once(self) -> None:
        await asyncio.gather(*[self._sync_follower(node_id)
                               for node_id in self.configuration.nodes_ids])

    def _append_records(self,
                        prefix_length: int,
                        suffix: List[Record]) -> None:
        log = self.state.log
        if suffix and len(log) > prefix_length:
            index = min(len(log), prefix_length + len(suffix)) - 1
            if log[index].term != suffix[index - prefix_length].term:
                del log[prefix_length:]
        if prefix_length + len(suffix) > len(log):
            new_records = suffix[len(log) - prefix_length:]
            for record in reversed(new_records):
                command = record.command
                if command.path == START_CONFIGURATION_UPDATE_COMMAND_PATH:
                    configuration = TransitionalClusterConfiguration.from_json(
                            **command.parameters)
                    self._update_configuration(configuration)
                    break
                elif command.path == END_CONFIGURATION_UPDATE_COMMAND_PATH:
                    configuration = ClusterConfiguration.from_json(
                            **command.parameters)
                    self._update_configuration(configuration)
                    break
            log.extend(new_records)

    def _cancel_election_timer(self) -> None:
        self.logger.debug(f'{self.id} cancels election timer')
        self._election_task.cancel()

    def _cancel_reelection_timer(self) -> None:
        self._reelection_task.cancel()

    def _commit(self, records: List[Record]) -> None:
        assert records
        self._process_records(records)
        self.state.commit_length += len(records)

    def _election_timer_callback(self, future: asyncio.Future) -> None:
        if future.cancelled():
            self.logger.debug(f'{self.id} cancelled election')
            return
        exception = future.exception()
        if exception is None:
            assert future.result() is None
            self._start_election_timer()
        elif isinstance(exception, asyncio.TimeoutError):
            future.cancel()
            self.logger.debug(f'{self.id} timed out election')
            self._start_election_timer()
        else:
            raise exception

    def _end_configuration_update(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self.id} ends configuration update')
        assert isinstance(self.configuration, ClusterConfiguration)
        assert (self.configuration == ClusterConfiguration.from_json(
                **parameters))
        if self.id not in self.configuration.nodes_ids:
            self.state.role = Role.FOLLOWER
            self._update_configuration(ClusterConfiguration(
                    {self.id: self.configuration.nodes_urls[self.id]},
                    active_nodes_ids=set(),
                    heartbeat=self.configuration.heartbeat))

    def _lead(self) -> None:
        self.logger.info(f'{self.id} is leader of term {self.state.term}')
        self.state.leader_node_id = self.id
        self.state.role = Role.LEADER
        for node_id in self.configuration.nodes_ids:
            self.state.sent_lengths[node_id] = len(self.state.log)
            self.state.accepted_lengths[node_id] = 0
        self._cancel_election_timer()
        self._start_sync_timer()

    def _process_records(self, records: List[Record]) -> None:
        self._loop.run_in_executor(self._commands_executor,
                                   self._process_commands,
                                   [record.command for record in records])

    def _process_commands(self, commands: List[Command]) -> None:
        for command in commands:
            self.processors[command.path](self, command.parameters)

    def _restart_election_timer(self) -> None:
        self.logger.debug(f'{self.id} restarts election timer '
                          f'after {self._reelection_lag}')
        if self._passive:
            self.logger.debug(f'{self.id} is passive, not running election')
            return
        self._cancel_election_timer()
        self._start_election_timer()

    def _restart_reelection_timer(self) -> None:
        self._cancel_reelection_timer()
        self._start_reelection_timer()

    def _start_configuration_update(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self.id} starts configuration update')
        assert isinstance(self.configuration, TransitionalClusterConfiguration)
        if self.state.role is Role.LEADER:
            configuration = TransitionalClusterConfiguration.from_json(
                    **parameters)
            assert configuration == self.configuration
            self.state.log.append(Record(
                    Command(path=END_CONFIGURATION_UPDATE_COMMAND_PATH,
                            parameters=configuration.new.as_json()),
                    self.state.term))
            self._update_configuration(configuration.new)

    def _start_election_timer(self) -> None:
        self._election_duration = self._to_new_duration()
        self._election_task = self._loop.create_task(self._run_election())
        self._election_task.add_done_callback(self._election_timer_callback)

    def _start_reelection_timer(self) -> None:
        self._reelection_lag = self._to_new_duration()
        self._reelection_task = self._loop.call_later(
                self._reelection_lag, self._restart_election_timer)

    def _start_sync_timer(self) -> None:
        self._sync_task = self._loop.create_task(self._sync_followers())

    def _to_new_duration(self) -> Time:
        broadcast_time = self._communication.to_expected_broadcast_time()
        assert 100 * broadcast_time < self.configuration.heartbeat
        return (self.configuration.heartbeat
                + random.uniform(broadcast_time, self.configuration.heartbeat))

    def _to_time(self) -> Time:
        return self._loop.time()

    def _try_commit(self) -> None:
        state = self.state
        while (state.commit_length < len(state.log)
               and self.configuration.has_majority(
                        to_nodes_ids_that_accepted_more_records(state))):
            self._commit([self.state.log[self.state.commit_length]])

    def _update_configuration(self,
                              configuration: AnyClusterConfiguration) -> None:
        self.logger.debug(f'{self.id} changes configuration '
                          f'from {self.configuration} to {configuration}')
        self._configuration = configuration
        self._communication.configuration = configuration
        self.state.nodes_ids = configuration.nodes_ids


def to_nodes_ids_that_accepted_more_records(state: NodeState
                                            ) -> Collection[NodeId]:
    return [node_id
            for node_id, length in state.accepted_lengths.items()
            if length > state.commit_length]


def update_term(state: NodeState, value: Term) -> None:
    state.leader_node_id = None
    state.supported_node_id = None
    state.term = value


def ceil_division(dividend: int, divisor: int) -> int:
    return -((-dividend) // divisor)


def format_exception(value: Exception) -> str:
    return traceback.format_exception(type(value), value, value.__traceback__)
