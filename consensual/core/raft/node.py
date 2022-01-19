import asyncio
import dataclasses
import enum
import logging.config
import random
from asyncio import get_event_loop
from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType
from typing import (Any,
                    Dict,
                    List,
                    Mapping,
                    Optional)

from aiohttp import (ClientError,
                     web,
                     web_ws)
from reprit import seekers
from reprit.base import generate_repr
from yarl import URL

from .cluster_configuration import (AnyClusterConfiguration,
                                    StableClusterConfiguration,
                                    TransitionalClusterConfiguration)
from .communication import (Communication,
                            update_communication_configuration)
from .event import Event
from .hints import (NodeId,
                    Processor,
                    Protocol,
                    Term,
                    Time)
from .node_state import (NodeState,
                         Role,
                         state_to_nodes_ids_that_accepted_more_records,
                         update_state_nodes_ids,
                         update_state_term)
from .record import Record
from .utils import format_exception

START_CONFIGURATION_UPDATE_ACTION = '0'
END_CONFIGURATION_UPDATE_ACTION = '1'
assert (START_CONFIGURATION_UPDATE_ACTION
        and not START_CONFIGURATION_UPDATE_ACTION.startswith('/'))
assert (END_CONFIGURATION_UPDATE_ACTION
        and not END_CONFIGURATION_UPDATE_ACTION.startswith('/'))


class Call(Protocol):
    def as_json(self) -> Dict[str, Any]:
        return {}


class CallPath(enum.IntEnum):
    LOG = 0
    SYNC = 1
    UPDATE = 2
    VOTE = 3


@dataclasses.dataclass(frozen=True)
class LogCall:
    event: Event

    @classmethod
    def from_json(cls, event: Dict[str, Any]) -> 'LogCall':
        return cls(Event(**event))

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
    configuration: StableClusterConfiguration

    def as_json(self) -> Dict[str, Any]:
        return {'configuration': self.configuration.as_json()}

    @classmethod
    def from_json(cls, configuration: Dict[str, Any]) -> 'UpdateCall':
        return cls(StableClusterConfiguration.from_json(**configuration))


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


class Node:
    __slots__ = ('_app', '_events_executor', '_communication',
                 '_configuration', '_election_duration', '_election_task',
                 '_id', '_last_heartbeat_time', '_logger', '_loop',
                 '_patch_routes', '_processors', '_reelection_lag',
                 '_reelection_task', '_state', '_sync_task')

    def __init__(self,
                 id_: NodeId,
                 state: NodeState,
                 configuration: AnyClusterConfiguration,
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
        self._events_executor = ThreadPoolExecutor(max_workers=1)
        self._communication: Communication[CallPath] = Communication(
                self.configuration, list(CallPath))
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
        self._processors[START_CONFIGURATION_UPDATE_ACTION] = (
            Node._start_configuration_update)
        self._processors[END_CONFIGURATION_UPDATE_ACTION] = (
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
            call_cls, processor = routes[call_path]
            call = call_cls(**raw_call['message'])
            reply = await processor(call)
            await websocket.send_json(reply.as_json())
        return websocket

    async def _handle_delete(self, request: web.Request) -> web.Response:
        self.logger.debug(f'{self.id} gets removed')
        rest_nodes_urls = dict(self.configuration.nodes_urls)
        del rest_nodes_urls[self.id]
        call = UpdateCall(StableClusterConfiguration(
                rest_nodes_urls,
                heartbeat=self.configuration.heartbeat))
        reply = await self._process_update_call(call)
        return web.json_response(reply.as_json())

    async def _handle_post(self, request: web.Request) -> web.Response:
        raw_nodes_urls_to_add = await request.json()
        nodes_urls_to_add = {
            node_id: URL(raw_node_url)
            for node_id, raw_node_url in raw_nodes_urls_to_add.items()}
        nodes_ids_to_add = nodes_urls_to_add.keys()
        existing_nodes_ids = (nodes_ids_to_add
                              & set(self.configuration.nodes_ids))
        if existing_nodes_ids:
            raise web.HTTPBadRequest(
                    reason=('nodes {nodes_ids} already exist'
                            .format(nodes_ids=', '.join(existing_nodes_ids))))
        self.logger.debug('{id} adds {nodes_ids}'
                          .format(id=self.id,
                                  nodes_ids=', '.join(nodes_urls_to_add)))
        call = UpdateCall(StableClusterConfiguration(
                {**self.configuration.nodes_urls, **nodes_urls_to_add},
                heartbeat=self.configuration.heartbeat))
        reply = await self._process_update_call(call)
        return web.json_response(reply.as_json())

    async def _handle_record(self, request: web.Request) -> web.Response:
        parameters = await request.json()
        reply = await self._process_log_call(
                LogCall(Event(action=request.path,
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
        self.logger.debug(f'{self.id} processes {call}')
        if self.state.leader_node_id is None:
            return LogReply(error=f'{self.id} has no leader')
        elif self.state.role is Role.LEADER:
            self.state.log.append(Record(call.event, self.state.term))
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
            update_state_term(self.state, call.term)
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
                self.state.accepted_lengths[reply.node_id] = (
                    reply.accepted_length)
                self.state.sent_lengths[reply.node_id] = reply.accepted_length
                self._try_commit()
            elif self.state.sent_lengths[reply.node_id] > 0:
                self.state.sent_lengths[reply.node_id] = (
                        self.state.sent_lengths[reply.node_id] - 1)
                await self._sync_follower(reply.node_id)
        elif reply.term > self.state.term:
            update_state_term(self.state, reply.term)
            self.state.role = Role.FOLLOWER
            self._cancel_election_timer()

    async def _process_update_call(self, call: UpdateCall) -> UpdateReply:
        self.logger.debug(f'{self.id} processes {call}')
        if self.state.leader_node_id is None:
            return UpdateReply(error=f'{self.id} has no leader')
        elif self.state.role is not Role.LEADER:
            try:
                raw_reply = await self._send_call(self.state.leader_node_id,
                                                  CallPath.UPDATE, call)
            except (ClientError, OSError) as exception:
                return UpdateReply(error=format_exception(exception))
            else:
                return UpdateReply(**raw_reply)
        transitional_configuration = TransitionalClusterConfiguration(
                self.configuration, call.configuration)
        self.state.log.append(Record(
                Event(action=START_CONFIGURATION_UPDATE_ACTION,
                      parameters=transitional_configuration.as_json()),
                self.state.term))
        self._update_configuration(transitional_configuration)
        await self._sync_followers_once()
        return UpdateReply(error=None)

    async def _process_vote_call(self, call: VoteCall) -> VoteReply:
        self.logger.debug(f'{self.id} processes {call}')
        if call.node_id not in self.configuration.nodes_ids:
            self.logger.debug(f'{self.id} skips voting for {call.node_id} '
                              f'because it is not in configuration')
            return VoteReply(node_id=self.id,
                             term=self.state.term,
                             supports=False)
        elif (self.state.leader_node_id is not None
              and (self._to_time() - self._last_heartbeat_time
                   < self.configuration.heartbeat)):
            self.logger.debug(f'{self.id} skips voting for {call.node_id} '
                              f'because leader {self.state.leader_node_id} '
                              f'can be alive')
            return VoteReply(node_id=self.id,
                             term=self.state.term,
                             supports=False)
        if call.term > self.state.term:
            update_state_term(self.state, call.term)
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
            update_state_term(self.state, reply.term)
            self.state.role = Role.FOLLOWER
            self._cancel_election_timer()

    async def _run_election(self) -> None:
        self.logger.debug(f'{self.id} runs election '
                          f'for term {self.state.term + 1}')
        update_state_term(self.state, self.state.term + 1)
        self.state.role = Role.CANDIDATE
        self.state.supporters_nodes_ids.clear()
        start = self._to_time()
        try:
            await asyncio.wait_for(
                    asyncio.gather(*[self._agitate_voter(node_id)
                                     for node_id
                                     in self.configuration.nodes_ids]),
                    self._election_duration)
        finally:
            duration = self._to_time() - start
            self.logger.debug(f'{self.id} election for term {self.state.term} '
                              f'took {duration}s, '
                              f'timeout: {self._election_duration}, '
                              f'role: {self.state.role.name}, '
                              f'supporters count: '
                              f'{len(self.state.supporters_nodes_ids)}')
            await asyncio.sleep(self._election_duration - duration)

    async def _send_call(self,
                         to: NodeId,
                         path: CallPath,
                         call: Call) -> Dict[str, Any]:
        result = await self._communication.send(to, path, call.as_json())
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
                event = record.event
                if event.action == START_CONFIGURATION_UPDATE_ACTION:
                    configuration = TransitionalClusterConfiguration.from_json(
                            **event.parameters)
                    self._update_configuration(configuration)
                    break
                elif event.action == END_CONFIGURATION_UPDATE_ACTION:
                    configuration = StableClusterConfiguration.from_json(
                            **event.parameters)
                    self._update_configuration(configuration)
                    break
            log.extend(new_records)

    def _cancel_election_timer(self) -> None:
        self.logger.debug(f'{self.id} cancels election timer')
        self._election_task.cancel()

    def _cancel_reelection_timer(self) -> None:
        self._reelection_task.cancel()

    def _cancel_sync_timer(self) -> None:
        self._sync_task.cancel()

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
        assert isinstance(self.configuration, StableClusterConfiguration)
        assert (self.configuration == StableClusterConfiguration.from_json(
                **parameters))
        if self.id not in self.configuration.nodes_ids:
            self.state.role = Role.FOLLOWER

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
        self._loop.run_in_executor(self._events_executor, self._process_events,
                                   [record.event for record in records])

    def _process_events(self, events: List[Event]) -> None:
        for event in events:
            self.processors[event.action](self, event.parameters)

    def _restart_election_timer(self) -> None:
        self.logger.debug(f'{self.id} restarts election timer '
                          f'after {self._reelection_lag}')
        self._cancel_election_timer()
        self._start_election_timer()

    def _restart_reelection_timer(self) -> None:
        self._cancel_reelection_timer()
        self._start_reelection_timer()

    def _restart_sync_timer(self) -> None:
        self._cancel_sync_timer()
        self._start_sync_timer()

    def _start_configuration_update(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self.id} starts configuration update')
        assert isinstance(self.configuration, TransitionalClusterConfiguration)
        if self.state.role is Role.LEADER:
            configuration = TransitionalClusterConfiguration.from_json(
                    **parameters)
            assert configuration == self.configuration
            self.state.log.append(Record(
                    Event(action=END_CONFIGURATION_UPDATE_ACTION,
                          parameters=configuration.new.as_json()),
                    self.state.term))
            self._update_configuration(configuration.new)
            self._restart_sync_timer()

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
        heartbeat = self.configuration.heartbeat
        assert (
                broadcast_time < heartbeat
        ), (
            f'broadcast time = {broadcast_time} >= {heartbeat} = heartbeat'
        )
        return heartbeat + random.uniform(broadcast_time, heartbeat)

    def _to_time(self) -> Time:
        return self._loop.time()

    def _try_commit(self) -> None:
        state = self.state
        while (state.commit_length < len(state.log)
               and self.configuration.has_majority(
                        state_to_nodes_ids_that_accepted_more_records(state))):
            self._commit([state.log[state.commit_length]])

    def _update_configuration(self,
                              configuration: AnyClusterConfiguration) -> None:
        self.logger.debug(f'{self.id} changes configuration\n'
                          f'from {sorted(self.configuration.nodes_ids)}\n'
                          f'to {sorted(configuration.nodes_ids)}')
        self._configuration = configuration
        update_communication_configuration(self._communication, configuration)
        update_state_nodes_ids(self.state, configuration.nodes_ids)
