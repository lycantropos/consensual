import asyncio
import enum
import json
import logging.config
import random
import uuid
from asyncio import (AbstractEventLoop,
                     get_event_loop,
                     new_event_loop,
                     set_event_loop)
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
                            ClusterId,
                            RawClusterId,
                            StableClusterState,
                            TransitionalClusterState)
from .command import Command
from .communication import (Communication,
                            update_communication_registry)
from .hints import (Processor,
                    Protocol,
                    Term,
                    Time)
from .node_state import (NodeId,
                         NodeState,
                         Role,
                         append_record,
                         append_records,
                         state_to_nodes_ids_that_accepted_more_records,
                         update_state_nodes_ids,
                         update_state_term)
from .record import Record
from .utils import (format_exception,
                    host_to_ip_address,
                    subtract_mapping,
                    unite_mappings)

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
    __slots__ = ('_cluster_id', '_commit_length', '_node_id', '_prefix_length',
                 '_prefix_term', '_suffix', '_term')

    def __new__(cls,
                *,
                cluster_id: ClusterId,
                commit_length: int,
                node_id: NodeId,
                prefix_length: int,
                prefix_term: Term,
                suffix: List[Record],
                term: Term) -> 'SyncCall':
        self = super().__new__(cls)
        (
            self._cluster_id, self._commit_length, self._node_id,
            self._prefix_length, self._prefix_term, self._suffix, self._term
        ) = (
            cluster_id, commit_length, node_id, prefix_length, prefix_term,
            suffix, term,
        )
        return self

    __repr__ = generate_repr(__new__)

    @property
    def cluster_id(self) -> ClusterId:
        return self._cluster_id

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
                  cluster_id: RawClusterId,
                  commit_length: int,
                  node_id: NodeId,
                  prefix_length: int,
                  prefix_term: Term,
                  suffix: List[Dict[str, Any]],
                  term: Term) -> 'SyncCall':
        return cls(cluster_id=ClusterId.from_json(cluster_id),
                   commit_length=commit_length,
                   node_id=node_id,
                   prefix_length=prefix_length,
                   prefix_term=prefix_term,
                   suffix=[Record.from_json(**record) for record in suffix],
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_id': self.cluster_id.as_json(),
                'commit_length': self.commit_length,
                'node_id': self.node_id,
                'prefix_length': self.prefix_length,
                'prefix_term': self.prefix_term,
                'suffix': [record.as_json() for record in self.suffix],
                'term': self.term}


class SyncReply:
    __slots__ = ('_accepted_length', '_cluster_id', '_node_id', '_successful',
                 '_term')

    def __new__(cls,
                *,
                accepted_length: int,
                cluster_id: ClusterId,
                node_id: NodeId,
                successful: bool,
                term: Term) -> 'SyncReply':
        self = super().__new__(cls)
        (
            self._accepted_length, self._cluster_id, self._node_id,
            self._successful, self._term
        ) = accepted_length, cluster_id, node_id, successful, term
        return self

    __repr__ = generate_repr(__new__)

    @property
    def accepted_length(self) -> int:
        return self._accepted_length

    @property
    def cluster_id(self) -> ClusterId:
        return self._cluster_id

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def successful(self) -> bool:
        return self._successful

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  accepted_length: int,
                  cluster_id: RawClusterId,
                  node_id: NodeId,
                  successful: bool,
                  term: Term) -> 'SyncReply':
        return cls(accepted_length=accepted_length,
                   cluster_id=ClusterId.from_json(cluster_id),
                   node_id=node_id,
                   successful=successful,
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'accepted_length': self.accepted_length,
                'cluster_id': self.cluster_id.as_json(),
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
    __slots__ = '_cluster_id', '_log_length', '_log_term', '_node_id', '_term'

    def __new__(cls,
                *,
                cluster_id: ClusterId,
                log_length: int,
                log_term: Term,
                node_id: NodeId,
                term: Term) -> 'VoteCall':
        self = super().__new__(cls)
        (
            self._cluster_id, self._log_length, self._log_term, self._node_id,
            self._term
        ) = cluster_id, log_length, log_term, node_id, term
        return self

    __repr__ = generate_repr(__new__)

    @property
    def cluster_id(self) -> ClusterId:
        return self._cluster_id

    @property
    def log_length(self) -> int:
        return self._log_length

    @property
    def log_term(self) -> Term:
        return self._log_term

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  cluster_id: RawClusterId,
                  log_length: int,
                  log_term: Term,
                  node_id: NodeId,
                  term: Term) -> 'VoteCall':
        return cls(cluster_id=ClusterId.from_json(cluster_id),
                   log_length=log_length,
                   log_term=log_term,
                   node_id=node_id,
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_id': self.cluster_id.as_json(),
                'log_length': self.log_length,
                'log_term': self.log_term,
                'node_id': self.node_id,
                'term': self.term}


class VoteReply:
    __slots__ = '_cluster_id', '_node_id', '_supports', '_term'

    def __new__(cls,
                *,
                cluster_id: ClusterId,
                node_id: NodeId,
                supports: bool,
                term: Term) -> 'VoteReply':
        self = super().__new__(cls)
        self._cluster_id, self._node_id, self._supports, self._term = (
            cluster_id, node_id, supports, term,
        )
        return self

    __repr__ = generate_repr(__new__)

    @property
    def cluster_id(self) -> ClusterId:
        return self._cluster_id

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def supports(self) -> bool:
        return self._supports

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  cluster_id: RawClusterId,
                  node_id: NodeId,
                  supports: bool,
                  term: Term) -> 'VoteReply':
        return cls(cluster_id=ClusterId.from_json(cluster_id),
                   node_id=node_id,
                   supports=supports,
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_id': self.cluster_id.as_json(),
                'node_id': self.node_id,
                'supports': self.supports,
                'term': self.term}


class Node:
    __slots__ = ('_app', '_cluster_state', '_commands_executor',
                 '_communication', '_election_duration', '_election_task',
                 '_last_heartbeat_time', '_logger', '_loop', '_patch_routes',
                 '_processors', '_reelection_lag', '_reelection_task',
                 '_state', '_sync_task', '_url')

    @classmethod
    def from_url(cls,
                 url: URL,
                 *,
                 heartbeat: Time = 5,
                 logger: Optional[logging.Logger] = None,
                 processors: Optional[Mapping[str, Processor]] = None
                 ) -> 'Node':
        id_ = node_url_to_id(url)
        return cls(NodeState(id_),
                   StableClusterState(ClusterId(),
                                      heartbeat=heartbeat,
                                      nodes_urls={id_: url}),
                   logger=logger,
                   processors=processors)

    def __init__(self,
                 _state: NodeState,
                 _cluster_state: AnyClusterState,
                 *,
                 logger: Optional[logging.Logger] = None,
                 processors: Optional[Mapping[str, Processor]] = None) -> None:
        self._cluster_state, self._state = _cluster_state, _state
        self._url = self._cluster_state.nodes_urls[self._state.id]
        self._logger = logging.getLogger() if logger is None else logger
        self._processors = {} if processors is None else dict(processors)
        self._loop = safe_get_event_loop()
        self._app = web.Application()
        self._commands_executor = ThreadPoolExecutor(max_workers=1)
        self._communication: Communication[NodeId, CallPath] = Communication(
                heartbeat=self._cluster_state.heartbeat,
                paths=list(CallPath),
                registry=self._cluster_state.nodes_urls)
        self._election_duration = 0
        self._election_task = self._loop.create_future()
        self._last_heartbeat_time = -self._cluster_state.heartbeat
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
        self._app.router.add_get('/cluster', self._handle_get_cluster)
        self._app.router.add_get('/node', self._handle_get_node)
        self._app.router.add_post('/', self._handle_post)
        self._app.router.add_route(self._communication.HTTP_METHOD, '/',
                                   self._handle_communication)
        for path in self.processors.keys():
            self._app.router.add_post(path, self._handle_record)
        self._processors[START_CLUSTER_STATE_UPDATE_ACTION] = (
            Node._start_cluster_state_update)
        self._processors[END_CLUSTER_STATE_UPDATE_ACTION] = (
            Node._end_cluster_state_update)

    __repr__ = generate_repr(from_url,
                             field_seeker=seekers.complex_)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def processors(self) -> Mapping[str, Processor]:
        return MappingProxyType(self._processors)

    @property
    def url(self) -> URL:
        return self._url

    def run(self) -> None:
        url = self.url
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
        text = await request.text()
        if text:
            raw_nodes_urls = json.loads(text)
            assert isinstance(raw_nodes_urls, list)
            nodes_urls = [URL(raw_url) for raw_url in raw_nodes_urls]
            nodes_urls_to_delete = {node_url_to_id(node_url): node_url
                                    for node_url in nodes_urls}
            nonexistent_nodes_ids = (nodes_urls_to_delete.keys()
                                     - set(self._cluster_state.nodes_ids))
            if nonexistent_nodes_ids:
                reply = UpdateReply(
                        error
                        =('nonexistent node(s) found: {nodes_ids}'
                          .format(nodes_ids=', '.join(nonexistent_nodes_ids))))
                return web.json_response(reply.as_json())
            self.logger.debug(
                    ('{id} initializes removal of {nodes_ids}'
                     .format(id=self._state.id,
                             nodes_ids=', '.join(nodes_urls_to_delete))))
            call = UpdateCall(StableClusterState(
                    generate_cluster_id(),
                    heartbeat=self._cluster_state.heartbeat,
                    nodes_urls=subtract_mapping(self._cluster_state.nodes_urls,
                                                nodes_urls_to_delete)))
            reply = await self._process_update_call(call)
            return web.json_response(reply.as_json())
        else:
            self.logger.debug(f'{self._state.id} gets removed')
            rest_nodes_urls = dict(self._cluster_state.nodes_urls)
            del rest_nodes_urls[self._state.id]
            call = UpdateCall(
                    StableClusterState(generate_cluster_id(),
                                       heartbeat=self._cluster_state.heartbeat,
                                       nodes_urls=rest_nodes_urls))
            reply = await self._process_update_call(call)
            return web.json_response(reply.as_json())

    async def _handle_get_cluster(self, request: web.Request) -> web.Response:
        cluster_state_json = {
            'id': self._cluster_state.id.as_json(),
            'heartbeat': self._cluster_state.heartbeat,
            'nodes_ids': list(self._cluster_state.nodes_ids),
            'stable': self._cluster_state.stable,
        }
        return web.json_response(cluster_state_json)

    async def _handle_get_node(self, request: web.Request) -> web.Response:
        node_state_json = {
            'id': self._state.id,
            'commit_length': self._state.commit_length,
            'leader_node_id': self._state.leader_node_id,
            'log': [record.as_json() for record in self._state.log],
            'role': self._state.role,
            'supported_node_id': self._state.supported_node_id,
            'supporters_nodes_ids': list(self._state.supporters_nodes_ids),
            'term': self._state.term,
        }
        return web.json_response(node_state_json)

    async def _handle_post(self, request: web.Request) -> web.Response:
        text = await request.text()
        if text:
            raw_urls = json.loads(text)
            assert isinstance(raw_urls, list)
            urls = [URL(raw_url) for raw_url in raw_urls]
            nodes_urls_to_add = {node_url_to_id(node_url): node_url
                                 for node_url in urls}
            existing_nodes_ids = (nodes_urls_to_add.keys()
                                  & set(self._cluster_state.nodes_ids))
            if existing_nodes_ids:
                reply = UpdateReply(
                        error
                        =('already existing node(s) found: {nodes_ids}'
                          .format(nodes_ids=', '.join(existing_nodes_ids))))
                return web.json_response(reply.as_json())
            self.logger.debug('{id} initializes adding of {nodes_ids}'
                              .format(id=self._state.id,
                                      nodes_ids=', '.join(nodes_urls_to_add)))
            call = UpdateCall(StableClusterState(
                    generate_cluster_id(),
                    heartbeat=self._cluster_state.heartbeat,
                    nodes_urls=unite_mappings(self._cluster_state.nodes_urls,
                                              nodes_urls_to_add)))
            reply = await self._process_update_call(call)
            return web.json_response(reply.as_json())
        else:
            self._initialize()
            return web.HTTPOk()

    async def _handle_record(self, request: web.Request) -> web.Response:
        parameters = await request.json()
        reply = await self._process_log_call(
                LogCall(Command(action=request.path,
                                parameters=parameters)))
        return web.json_response(reply.as_json())

    async def _call_sync(self, node_id: NodeId) -> SyncReply:
        prefix_length = self._state.sent_lengths[node_id]
        call = SyncCall(cluster_id=self._cluster_state.id,
                        commit_length=self._state.commit_length,
                        node_id=self._state.id,
                        prefix_length=prefix_length,
                        prefix_term=(self._state.log[prefix_length - 1].term
                                     if prefix_length
                                     else 0),
                        suffix=self._state.log[prefix_length:],
                        term=self._state.term)
        try:
            raw_reply = await self._send_call(node_id, CallPath.SYNC, call)
        except (ClientError, OSError):
            return SyncReply(accepted_length=0,
                             cluster_id=ClusterId(),
                             node_id=node_id,
                             term=self._state.term,
                             successful=False)
        else:
            return SyncReply.from_json(**raw_reply)

    async def _call_vote(self, node_id: NodeId) -> VoteReply:
        call = VoteCall(cluster_id=self._cluster_state.id,
                        node_id=self._state.id,
                        term=self._state.term,
                        log_length=len(self._state.log),
                        log_term=self._state.log_term)
        try:
            raw_reply = await self._send_call(node_id, CallPath.VOTE, call)
        except (ClientError, OSError):
            return VoteReply(cluster_id=ClusterId(),
                             node_id=node_id,
                             supports=False,
                             term=self._state.term)
        else:
            return VoteReply.from_json(**raw_reply)

    async def _process_log_call(self, call: LogCall) -> LogReply:
        self.logger.debug(f'{self._state.id} processes {call}')
        if self._state.leader_node_id is None:
            return LogReply(error=f'{self._state.id} has no leader')
        elif self._state.role is Role.LEADER:
            append_record(self._state,
                          Record(command=call.command,
                                 term=self._state.term))
            await self._sync_followers_once()
            assert self._state.accepted_lengths[self._state.id] == len(
                    self._state.log)
            return LogReply(error=None)
        else:
            assert self._state.role is Role.FOLLOWER
            try:
                raw_reply = await asyncio.wait_for(
                        self._send_call(self._state.leader_node_id,
                                        CallPath.LOG, call),
                        self._reelection_lag
                        - (self._to_time() - self._last_heartbeat_time))
            except (asyncio.TimeoutError, ClientError, OSError) as exception:
                return LogReply(error=format_exception(exception))
            else:
                return LogReply.from_json(**raw_reply)

    async def _process_sync_call(self, call: SyncCall) -> SyncReply:
        self.logger.debug(f'{self._state.id} processes {call}')
        if (self._cluster_state.id
                and not self._cluster_state.id.agrees_with(call.cluster_id)):
            return SyncReply(accepted_length=0,
                             cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             successful=False)
        self._last_heartbeat_time = self._to_time()
        self._restart_reelection_timer()
        if call.term > self._state.term:
            update_state_term(self._state, call.term)
            self._follow(call.node_id)
        elif (call.term == self._state.term
              and self._state.leader_node_id is None):
            self._follow(call.node_id)
        if (call.term == self._state.term
                and (len(self._state.log) >= call.prefix_length
                     and (call.prefix_length == 0
                          or (self._state.log[call.prefix_length - 1].term
                              == call.prefix_term)))):
            self._append_records(call.prefix_length, call.suffix)
            if call.commit_length > self._state.commit_length:
                self._commit(self._state.log[self._state.commit_length
                                             :call.commit_length])
            return SyncReply(accepted_length=(call.prefix_length
                                              + len(call.suffix)),
                             cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             successful=True)
        else:
            return SyncReply(accepted_length=0,
                             cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             successful=False)

    async def _process_sync_reply(self, reply: SyncReply) -> None:
        self.logger.debug(f'{self._state.id} processes {reply}')
        if not self._cluster_state.id.agrees_with(reply.cluster_id):
            pass
        elif (reply.term == self._state.term
              and self._state.role is Role.LEADER):
            if (reply.successful
                    and (reply.accepted_length
                         >= self._state.accepted_lengths[reply.node_id])):
                self._state.accepted_lengths[reply.node_id] = (
                    reply.accepted_length
                )
                self._state.sent_lengths[reply.node_id] = reply.accepted_length
                self._try_commit()
            elif self._state.sent_lengths[reply.node_id] > 0:
                self._state.sent_lengths[reply.node_id] = (
                        self._state.sent_lengths[reply.node_id] - 1)
                await self._sync_follower(reply.node_id)
        elif reply.term > self._state.term:
            update_state_term(self._state, reply.term)
            self._state.role = Role.FOLLOWER
            self._cancel_election_timer()

    async def _process_update_call(self, call: UpdateCall) -> UpdateReply:
        self.logger.debug(f'{self._state.id} processes {call}')
        if self._state.leader_node_id is None:
            return UpdateReply(error=f'{self._state.id} has no leader')
        elif self._state.role is not Role.LEADER:
            try:
                raw_reply = await asyncio.wait_for(
                        self._send_call(self._state.leader_node_id,
                                        CallPath.UPDATE, call),
                        self._reelection_lag
                        - (self._to_time() - self._last_heartbeat_time))
            except (asyncio.TimeoutError, ClientError, OSError) as exception:
                return UpdateReply(error=format_exception(exception))
            else:
                return UpdateReply.from_json(**raw_reply)
        if not self._cluster_state.stable:
            return UpdateReply(error='Cluster is currently '
                                     'in transitional state.')
        if (len(self._cluster_state.nodes_ids) == 1
                and not call.cluster_state.nodes_ids):
            self._delete()
            return UpdateReply(error=None)
        next_cluster_state = TransitionalClusterState(old=self._cluster_state,
                                                      new=call.cluster_state)
        command = Command(action=START_CLUSTER_STATE_UPDATE_ACTION,
                          parameters=next_cluster_state.as_json())
        append_record(self._state,
                      Record(command=command,
                             term=self._state.term))
        self._update_cluster_state(next_cluster_state)
        await self._sync_followers_once()
        return UpdateReply(error=None)

    async def _process_vote_call(self, call: VoteCall) -> VoteReply:
        self.logger.debug(f'{self._state.id} processes {call}')
        if not self._cluster_state.id.agrees_with(call.cluster_id):
            self.logger.debug(f'{self._state.id} skips voting '
                              f'for {call.node_id} '
                              f'because it has conflicting cluster id')
            return VoteReply(cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             supports=False)
        elif call.node_id not in self._cluster_state.nodes_ids:
            self.logger.debug(f'{self._state.id} skips voting '
                              f'for {call.node_id} '
                              f'because it is not in cluster state')
            return VoteReply(cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             supports=False)
        elif (self._state.leader_node_id is not None
              and (self._to_time() - self._last_heartbeat_time
                   < self._cluster_state.heartbeat)):
            self.logger.debug(
                    f'{self._state.id} skips voting for {call.node_id} '
                    f'because leader {self._state.leader_node_id} '
                    f'can be alive')
            return VoteReply(cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             supports=False)
        if call.term > self._state.term:
            update_state_term(self._state, call.term)
            self._state.role = Role.FOLLOWER
        if (call.term == self._state.term
                and ((call.log_term, call.log_length)
                     >= (self._state.log_term, len(self._state.log)))
                and (self._state.supported_node_id is None
                     or self._state.supported_node_id == call.node_id)):
            self._state.supported_node_id = call.node_id
            return VoteReply(cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             supports=True)
        else:
            return VoteReply(cluster_id=self._cluster_state.id,
                             node_id=self._state.id,
                             term=self._state.term,
                             supports=False)

    async def _process_vote_reply(self, reply: VoteReply) -> None:
        self.logger.debug(f'{self._state.id} processes {reply}')
        if self._state.role is not Role.CANDIDATE:
            pass
        elif not self._cluster_state.id.agrees_with(reply.cluster_id):
            pass
        elif (not self._cluster_state.stable
              and self._cluster_state.new.id == reply.cluster_id
              and self._state.id not in self._cluster_state.new.nodes_ids):
            assert not reply.supports
            self._state.rejectors_nodes_ids.add(reply.node_id)
            if self._cluster_state.new.has_majority(
                    self._state.rejectors_nodes_ids):
                self._delete()
        elif reply.term == self._state.term and reply.supports:
            self._state.supporters_nodes_ids.add(reply.node_id)
            if self._cluster_state.has_majority(
                    self._state.supporters_nodes_ids):
                self._lead()
        elif reply.term > self._state.term:
            update_state_term(self._state, reply.term)
            self._state.role = Role.FOLLOWER
            self._cancel_election_timer()

    async def _run_election(self) -> None:
        self.logger.debug(f'{self._state.id} runs election '
                          f'for term {self._state.term + 1}')
        update_state_term(self._state, self._state.term + 1)
        self._state.role = Role.CANDIDATE
        self._state.rejectors_nodes_ids.clear()
        self._state.supporters_nodes_ids.clear()
        start = self._to_time()
        try:
            await asyncio.wait_for(
                    asyncio.gather(*[self._agitate_voter(node_id)
                                     for node_id
                                     in self._cluster_state.nodes_ids]),
                    self._election_duration)
        finally:
            duration = self._to_time() - start
            self.logger.debug(
                    f'{self._state.id} election for term {self._state.term} '
                    f'took {duration}s, '
                    f'timeout: {self._election_duration}, '
                    f'role: {self._state.role.name}, '
                    f'supporters count: '
                    f'{len(self._state.supporters_nodes_ids)}')
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
        self.logger.info(f'{self._state.id} syncs followers')
        while self._state.role is Role.LEADER:
            start = self._to_time()
            await self._sync_followers_once()
            duration = self._to_time() - start
            self.logger.debug(
                    f'{self._state.id} followers\' sync took {duration}s')
            await asyncio.sleep(
                    self._cluster_state.heartbeat - duration
                    - self._communication.to_expected_broadcast_time())

    async def _sync_followers_once(self) -> None:
        await asyncio.gather(*[self._sync_follower(node_id)
                               for node_id in self._cluster_state.nodes_ids])

    def _append_records(self,
                        prefix_length: int,
                        suffix: List[Record]) -> None:
        log = self._state.log
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
            append_records(self._state, new_records)

    def _cancel_election_timer(self) -> None:
        self.logger.debug(f'{self._state.id} cancels election timer')
        self._election_task.cancel()

    def _cancel_reelection_timer(self) -> None:
        self._reelection_task.cancel()

    def _cancel_sync_timer(self) -> None:
        self._sync_task.cancel()

    def _commit(self, records: List[Record]) -> None:
        assert records
        self._process_records(records)
        self._state.commit_length += len(records)

    def _delete(self) -> None:
        self.logger.debug(f'{self._state.id} gets deleted')
        self._cancel_election_timer()
        self._cancel_reelection_timer()
        self._cancel_sync_timer()
        self._update_cluster_state(StableClusterState(
                ClusterId(),
                heartbeat=self._cluster_state.heartbeat,
                nodes_urls={self._state.id: self.url},
        ))
        self._state.leader_node_id = None
        self._state.role = Role.FOLLOWER

    def _election_timer_callback(self, future: asyncio.Future) -> None:
        if future.cancelled():
            self.logger.debug(f'{self._state.id} has election been cancelled')
            return
        exception = future.exception()
        if exception is None:
            assert future.result() is None
            self._start_election_timer()
        elif isinstance(exception, asyncio.TimeoutError):
            future.cancel()
            self.logger.debug(f'{self._state.id} has election been timed out')
            self._start_election_timer()
        else:
            raise exception

    def _end_cluster_state_update(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self._state.id} ends cluster state update')
        assert isinstance(self._cluster_state, StableClusterState)
        assert (self._cluster_state
                == StableClusterState.from_json(**parameters))
        if self._state.id not in self._cluster_state.nodes_ids:
            self._delete()

    def _follow(self, leader_node_id: NodeId) -> None:
        self.logger.info(f'{self._state.id} follows {leader_node_id} '
                         f'in term {self._state.term}')
        self._state.leader_node_id = leader_node_id
        self._state.role = Role.FOLLOWER
        self._cancel_election_timer()

    def _initialize(self) -> None:
        self.logger.debug(f'{self._state.id} gets initialized')
        self._update_cluster_state(StableClusterState(
                generate_cluster_id(),
                heartbeat=self._cluster_state.heartbeat,
                nodes_urls={self._state.id: self.url},
        ))
        self._lead()

    def _lead(self) -> None:
        self.logger.info(
                f'{self._state.id} is leader of term {self._state.term}')
        self._state.leader_node_id = self._state.id
        self._state.role = Role.LEADER
        for node_id in self._cluster_state.nodes_ids:
            self._state.sent_lengths[node_id] = len(self._state.log)
            self._state.accepted_lengths[node_id] = 0
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
        self.logger.debug(f'{self._state.id} restarts election timer '
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
        self.logger.debug(f'{self._state.id} starts cluster state update')
        assert isinstance(self._cluster_state, TransitionalClusterState)
        if self._state.role is Role.LEADER:
            cluster_state = TransitionalClusterState.from_json(**parameters)
            assert cluster_state == self._cluster_state
            command = Command(action=END_CLUSTER_STATE_UPDATE_ACTION,
                              parameters=cluster_state.new.as_json())
            append_record(self._state,
                          Record(command=command,
                                 term=self._state.term))
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
        heartbeat = self._cluster_state.heartbeat
        assert (
                broadcast_time < heartbeat
        ), (
            f'broadcast time = {broadcast_time} >= {heartbeat} = heartbeat'
        )
        return heartbeat + random.uniform(broadcast_time, heartbeat)

    def _to_time(self) -> Time:
        return self._loop.time()

    def _try_commit(self) -> None:
        state = self._state
        while (state.commit_length < len(state.log)
               and self._cluster_state.has_majority(
                        state_to_nodes_ids_that_accepted_more_records(state))):
            self._commit([state.log[state.commit_length]])

    def _update_cluster_state(self, cluster_state: AnyClusterState) -> None:
        self._cluster_state = cluster_state
        update_communication_registry(self._communication,
                                      cluster_state.nodes_urls)
        update_state_nodes_ids(self._state, cluster_state.nodes_ids)


def safe_get_event_loop() -> AbstractEventLoop:
    try:
        result = get_event_loop()
    except RuntimeError:
        result = new_event_loop()
        set_event_loop(result)
    return result


def generate_cluster_id() -> ClusterId:
    return ClusterId(uuid.uuid4().hex)


def node_url_to_id(url: URL) -> NodeId:
    return f'{host_to_ip_address(url.host)}:{url.port}'
