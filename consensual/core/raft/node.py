import asyncio
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

from .cluster_state import (AnyClusterState,
                            StableClusterState,
                            TransitionalClusterState)
from .command import Command
from .communication import (Communication,
                            update_communication_cluster_state)
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

START_CLUSTER_STATE_UPDATE_ACTION = '0'
END_CLUSTER_STATE_UPDATE_ACTION = '1'
assert (START_CLUSTER_STATE_UPDATE_ACTION
        and not START_CLUSTER_STATE_UPDATE_ACTION.startswith('/'))
assert (END_CLUSTER_STATE_UPDATE_ACTION
        and not END_CLUSTER_STATE_UPDATE_ACTION.startswith('/'))


class Call(Protocol):
    def as_json(self) -> Dict[str, Any]:
        return {}


class CallPath(enum.IntEnum):
    LOG = 0
    SYNC = 1
    UPDATE = 2
    VOTE = 3


class LogCall:
    __slots__ = '_command',

    def __new__(cls, command: Command) -> 'LogCall':
        self = super().__new__(cls)
        self._command = command
        return self

    __repr__ = generate_repr(__new__)

    @property
    def command(self) -> Command:
        return self._command

    @classmethod
    def from_json(cls, command: Dict[str, Any]) -> 'LogCall':
        return cls(Command(**command))

    def as_json(self) -> Dict[str, Any]:
        return {'command': self.command.as_json()}


class LogReply:
    __slots__ = '_error',

    def __new__(cls, error: Optional[str]) -> 'LogReply':
        self = super().__new__(cls)
        self._error = error
        return self

    __repr__ = generate_repr(__new__)

    @property
    def error(self) -> Optional[str]:
        return self._error

    from_json = classmethod(__new__)

    def as_json(self) -> Dict[str, Any]:
        return {'error': self.error}


class SyncCall:
    __slots__ = ('_commit_length', '_node_id', '_prefix_length',
                 '_prefix_term', '_suffix', '_term')

    def __new__(cls,
                *,
                commit_length: int,
                node_id: NodeId,
                prefix_length: int,
                prefix_term: Term,
                suffix: List[Record],
                term: Term) -> 'SyncCall':
        self = super().__new__(cls)
        (
            self._commit_length, self._node_id, self._prefix_length,
            self._prefix_term, self._suffix, self._term
        ) = commit_length, node_id, prefix_length, prefix_term, suffix, term
        return self

    __repr__ = generate_repr(__new__)

    @property
    def commit_length(self) -> int:
        return self._commit_length

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def prefix_length(self) -> int:
        return self._prefix_length

    @property
    def prefix_term(self) -> Term:
        return self._prefix_term

    @property
    def suffix(self) -> List[Record]:
        return self._suffix

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  commit_length: int,
                  node_id: NodeId,
                  prefix_length: int,
                  prefix_term: Term,
                  suffix: List[Dict[str, Any]],
                  term: Term) -> 'SyncCall':
        return cls(commit_length=commit_length,
                   node_id=node_id,
                   prefix_length=prefix_length,
                   prefix_term=prefix_term,
                   suffix=[Record.from_json(**record) for record in suffix],
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'commit_length': self.commit_length,
                'node_id': self.node_id,
                'prefix_length': self.prefix_length,
                'prefix_term': self.prefix_term,
                'suffix': [record.as_json() for record in self.suffix],
                'term': self.term}


class SyncReply:
    __slots__ = '_accepted_length', '_node_id', '_successful', '_term'

    def __new__(cls,
                *,
                accepted_length: int,
                node_id: NodeId,
                successful: bool,
                term: Term) -> 'SyncReply':
        self = super().__new__(cls)
        self._accepted_length, self._node_id, self._successful, self._term = (
            accepted_length, node_id, successful, term
        )
        return self

    __repr__ = generate_repr(__new__)

    @property
    def accepted_length(self) -> int:
        return self._accepted_length

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def successful(self) -> bool:
        return self._successful

    @property
    def term(self) -> Term:
        return self._term

    from_json = classmethod(__new__)

    def as_json(self) -> Dict[str, Any]:
        return {'accepted_length': self.accepted_length,
                'node_id': self.node_id,
                'successful': self.successful,
                'term': self.term}


class UpdateCall:
    __slots__ = '_cluster_state',

    def __new__(cls, cluster_state: StableClusterState) -> 'UpdateCall':
        self = super().__new__(cls)
        self._cluster_state = cluster_state
        return self

    __repr__ = generate_repr(__new__)

    @property
    def cluster_state(self) -> StableClusterState:
        return self._cluster_state

    @classmethod
    def from_json(cls, cluster_state: Dict[str, Any]) -> 'UpdateCall':
        return cls(StableClusterState.from_json(**cluster_state))

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_state': self.cluster_state.as_json()}


class UpdateReply:
    __slots__ = '_error',

    def __new__(cls, error: Optional[str]) -> 'UpdateReply':
        self = super().__new__(cls)
        self._error = error
        return self

    __repr__ = generate_repr(__new__)

    @property
    def error(self) -> Optional[str]:
        return self._error

    from_json = classmethod(__new__)

    def as_json(self) -> Dict[str, Any]:
        return {'error': self.error}


class VoteCall:
    __slots__ = '_log_length', '_log_term', '_node_id', '_term'

    def __new__(cls,
                *,
                log_length: int,
                log_term: Term,
                node_id: NodeId,
                term: Term) -> 'VoteCall':
        self = super().__new__(cls)
        self._log_length, self._log_term, self._node_id, self._term = (
            log_length, log_term, node_id, term
        )
        return self

    __repr__ = generate_repr(__new__)

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def term(self) -> Term:
        return self._term

    @property
    def log_length(self) -> int:
        return self._log_length

    @property
    def log_term(self) -> Term:
        return self._log_term

    from_json = classmethod(__new__)

    def as_json(self) -> Dict[str, Any]:
        return {'log_length': self.log_length,
                'log_term': self.log_term,
                'node_id': self.node_id,
                'term': self.term}


class VoteReply:
    __slots__ = '_node_id', '_supports', '_term'

    def __new__(cls,
                *,
                node_id: NodeId,
                supports: bool,
                term: Term) -> 'VoteReply':
        self = super().__new__(cls)
        self._node_id = node_id
        self._supports = supports
        self._term = term
        return self

    __repr__ = generate_repr(__new__)

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def supports(self) -> bool:
        return self._supports

    @property
    def term(self) -> Term:
        return self._term

    from_json = classmethod(__new__)

    def as_json(self) -> Dict[str, Any]:
        return {'node_id': self.node_id,
                'supports': self.supports,
                'term': self.term}


class Node:
    __slots__ = ('_app', '_cluster_state', '_commands_executor',
                 '_communication', '_election_duration', '_election_task',
                 '_id', '_last_heartbeat_time', '_logger', '_loop',
                 '_patch_routes', '_processors', '_reelection_lag',
                 '_reelection_task', '_state', '_sync_task')

    def __init__(self,
                 id_: NodeId,
                 state: NodeState,
                 cluster_state: AnyClusterState,
                 *,
                 logger: Optional[logging.Logger] = None,
                 processors: Dict[str, Processor]) -> None:
        self._id = id_
        self._cluster_state = cluster_state
        self._state = state
        self._last_heartbeat_time = -self.cluster_state.heartbeat
        self._logger = logging.getLogger() if logger is None else logger
        self._loop = get_event_loop()
        self._app = web.Application()
        self._commands_executor = ThreadPoolExecutor(max_workers=1)
        self._communication: Communication[CallPath] = Communication(
                self.cluster_state, list(CallPath))
        self._election_duration = 0
        self._election_task = self._loop.create_future()
        self._reelection_lag = 0
        self._reelection_task = self._loop.create_future()
        self._sync_task = self._loop.create_future()
        self._patch_routes = {
            CallPath.LOG: (LogCall.from_json, self._process_log_call),
            CallPath.SYNC: (SyncCall.from_json, self._process_sync_call),
            CallPath.UPDATE: (UpdateCall.from_json, self._process_update_call),
            CallPath.VOTE: (VoteCall.from_json, self._process_vote_call),
        }
        self._app.router.add_delete('/', self._handle_delete)
        self._app.router.add_post('/', self._handle_post)
        self._app.router.add_route(self._communication.HTTP_METHOD, '/',
                                   self._handle_communication)
        self._processors = processors
        for path in self.processors.keys():
            self._app.router.add_post(path, self._handle_record)
        self._processors[START_CLUSTER_STATE_UPDATE_ACTION] = (
            Node._start_cluster_state_update)
        self._processors[END_CLUSTER_STATE_UPDATE_ACTION] = (
            Node._end_cluster_state_update)

    __repr__ = generate_repr(__init__,
                             field_seeker=seekers.complex_)

    @property
    def cluster_state(self) -> AnyClusterState:
        return self._cluster_state

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
        url = self.cluster_state.nodes_urls[self.id]
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
            call_from_json, processor = routes[call_path]
            call = call_from_json(**raw_call['message'])
            reply = await processor(call)
            await websocket.send_json(reply.as_json())
        return websocket

    async def _handle_delete(self, request: web.Request) -> web.Response:
        self.logger.debug(f'{self.id} gets removed')
        rest_nodes_urls = dict(self.cluster_state.nodes_urls)
        del rest_nodes_urls[self.id]
        call = UpdateCall(
                StableClusterState(nodes_urls=rest_nodes_urls,
                                   heartbeat=self.cluster_state.heartbeat))
        reply = await self._process_update_call(call)
        return web.json_response(reply.as_json())

    async def _handle_post(self, request: web.Request) -> web.Response:
        raw_nodes_urls_to_add = await request.json()
        nodes_urls_to_add = {
            node_id: URL(raw_node_url)
            for node_id, raw_node_url in raw_nodes_urls_to_add.items()}
        nodes_ids_to_add = nodes_urls_to_add.keys()
        existing_nodes_ids = (nodes_ids_to_add
                              & set(self.cluster_state.nodes_ids))
        if existing_nodes_ids:
            raise web.HTTPBadRequest(
                    reason=('nodes {nodes_ids} already exist'
                            .format(nodes_ids=', '.join(existing_nodes_ids))))
        self.logger.debug('{id} adds {nodes_ids}'
                          .format(id=self.id,
                                  nodes_ids=', '.join(nodes_urls_to_add)))
        call = UpdateCall(StableClusterState(
                nodes_urls={**self.cluster_state.nodes_urls,
                            **nodes_urls_to_add},
                heartbeat=self.cluster_state.heartbeat))
        reply = await self._process_update_call(call)
        return web.json_response(reply.as_json())

    async def _handle_record(self, request: web.Request) -> web.Response:
        parameters = await request.json()
        reply = await self._process_log_call(
                LogCall(Command(action=request.path,
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
            return SyncReply.from_json(**raw_reply)

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
            return VoteReply.from_json(**raw_reply)

    async def _process_log_call(self, call: LogCall) -> LogReply:
        self.logger.debug(f'{self.id} processes {call}')
        if self.state.leader_node_id is None:
            return LogReply(error=f'{self.id} has no leader')
        elif self.state.role is Role.LEADER:
            self.state.log.append(Record(command=call.command,
                                         term=self.state.term))
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
                return LogReply.from_json(**raw_reply)

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
                return UpdateReply.from_json(**raw_reply)
        next_cluster_state = TransitionalClusterState(old=self.cluster_state,
                                                      new=call.cluster_state)
        self.state.log.append(Record(
                command=Command(action=START_CLUSTER_STATE_UPDATE_ACTION,
                                parameters=next_cluster_state.as_json()),
                term=self.state.term))
        self._update_cluster_state(next_cluster_state)
        await self._sync_followers_once()
        return UpdateReply(error=None)

    async def _process_vote_call(self, call: VoteCall) -> VoteReply:
        self.logger.debug(f'{self.id} processes {call}')
        if call.node_id not in self.cluster_state.nodes_ids:
            self.logger.debug(f'{self.id} skips voting for {call.node_id} '
                              f'because it is not in cluster state')
            return VoteReply(node_id=self.id,
                             term=self.state.term,
                             supports=False)
        elif (self.state.leader_node_id is not None
              and (self._to_time() - self._last_heartbeat_time
                   < self.cluster_state.heartbeat)):
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
            if self.cluster_state.has_majority(
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
                                     in self.cluster_state.nodes_ids]),
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
                    self.cluster_state.heartbeat - duration
                    - self._communication.to_expected_broadcast_time())

    async def _sync_followers_once(self) -> None:
        await asyncio.gather(*[self._sync_follower(node_id)
                               for node_id in self.cluster_state.nodes_ids])

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
                if command.action == START_CLUSTER_STATE_UPDATE_ACTION:
                    cluster_state = TransitionalClusterState.from_json(
                            **command.parameters)
                    self._update_cluster_state(cluster_state)
                    break
                elif command.action == END_CLUSTER_STATE_UPDATE_ACTION:
                    cluster_state = StableClusterState.from_json(
                            **command.parameters)
                    self._update_cluster_state(cluster_state)
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

    def _end_cluster_state_update(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self.id} ends cluster state update')
        assert isinstance(self.cluster_state, StableClusterState)
        assert (self.cluster_state == StableClusterState.from_json(
                **parameters))
        if self.id not in self.cluster_state.nodes_ids:
            self.state.role = Role.FOLLOWER
            self._cancel_election_timer()
            self._cancel_reelection_timer()
            self._cancel_sync_timer()

    def _lead(self) -> None:
        self.logger.info(f'{self.id} is leader of term {self.state.term}')
        self.state.leader_node_id = self.id
        self.state.role = Role.LEADER
        for node_id in self.cluster_state.nodes_ids:
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
            self.processors[command.action](self, command.parameters)

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

    def _start_cluster_state_update(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self.id} starts cluster state update')
        assert isinstance(self.cluster_state, TransitionalClusterState)
        if self.state.role is Role.LEADER:
            cluster_state = TransitionalClusterState.from_json(**parameters)
            assert cluster_state == self.cluster_state
            self.state.log.append(Record(
                    command=Command(action=END_CLUSTER_STATE_UPDATE_ACTION,
                                    parameters=cluster_state.new.as_json()),
                    term=self.state.term))
            self._update_cluster_state(cluster_state.new)
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
        heartbeat = self.cluster_state.heartbeat
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
               and self.cluster_state.has_majority(
                        state_to_nodes_ids_that_accepted_more_records(state))):
            self._commit([state.log[state.commit_length]])

    def _update_cluster_state(self,
                              cluster_state: AnyClusterState) -> None:
        self.logger.debug(f'{self.id} changes cluster state\n'
                          f'from {sorted(self.cluster_state.nodes_ids)}\n'
                          f'to {sorted(cluster_state.nodes_ids)}')
        self._cluster_state = cluster_state
        update_communication_cluster_state(self._communication, cluster_state)
        update_state_nodes_ids(self.state, cluster_state.nodes_ids)
