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
                    Awaitable,
                    Callable,
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

from .cluster_id import (ClusterId,
                         RawClusterId)
from .cluster_state import (ClusterState,
                            DisjointClusterState,
                            JointClusterState)
from .command import Command
from .communication import (Communication,
                            update_communication_registry)
from .hints import (NodeId,
                    Processor,
                    Protocol,
                    Term,
                    Time)
from .node_state import (NodeState,
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

SEPARATE_CLUSTERS_ACTION = '0'
STABILIZE_CLUSTER_ACTION = '1'
assert (SEPARATE_CLUSTERS_ACTION
        and not SEPARATE_CLUSTERS_ACTION.startswith('/'))
assert (STABILIZE_CLUSTER_ACTION
        and not STABILIZE_CLUSTER_ACTION.startswith('/'))


class Call(Protocol):
    def as_json(self) -> Dict[str, Any]:
        return {}


class CallPath(enum.IntEnum):
    LOG = 0
    SYNC = 1
    UPDATE = 2
    VOTE = 3


class LogCall:
    __slots__ = '_command', '_node_id'

    def __new__(cls, *, command: Command, node_id: NodeId) -> 'LogCall':
        self = super().__new__(cls)
        self._command, self._node_id = command, node_id
        return self

    __repr__ = generate_repr(__new__)

    @property
    def command(self) -> Command:
        return self._command

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @classmethod
    def from_json(cls,
                  *,
                  command: Dict[str, Any],
                  node_id: NodeId) -> 'LogCall':
        return cls(command=Command.from_json(**command),
                   node_id=node_id)

    def as_json(self) -> Dict[str, Any]:
        return {'command': self.command.as_json(),
                'node_id': self.node_id}


class LogStatus(enum.IntEnum):
    REJECTED = enum.auto()
    SUCCEED = enum.auto()
    UNAVAILABLE = enum.auto()
    UNGOVERNABLE = enum.auto()


class LogReply:
    __slots__ = '_status',

    def __new__(cls, status: LogStatus) -> 'LogReply':
        self = super().__new__(cls)
        self._status = status
        return self

    __repr__ = generate_repr(__new__)

    @property
    def status(self) -> LogStatus:
        return self._status

    @classmethod
    def from_json(cls, status: int) -> 'LogReply':
        return cls(LogStatus(status))

    def as_json(self) -> Dict[str, Any]:
        return {'status': int(self.status)}


class SyncCall:
    __slots__ = ('_cluster_id', '_commit_length', '_node_id',
                 '_prefix_cluster_id', '_prefix_length', '_prefix_term',
                 '_suffix', '_term')

    def __new__(cls,
                *,
                cluster_id: ClusterId,
                commit_length: int,
                node_id: NodeId,
                prefix_cluster_id: ClusterId,
                prefix_length: int,
                prefix_term: Term,
                suffix: List[Record],
                term: Term) -> 'SyncCall':
        self = super().__new__(cls)
        (
            self._cluster_id, self._commit_length, self._node_id,
            self._prefix_cluster_id, self._prefix_length, self._prefix_term,
            self._suffix, self._term
        ) = (
            cluster_id, commit_length, node_id, prefix_cluster_id,
            prefix_length, prefix_term, suffix, term,
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
    def prefix_cluster_id(self) -> ClusterId:
        return self._prefix_cluster_id

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
                  prefix_cluster_id: RawClusterId,
                  prefix_length: int,
                  prefix_term: Term,
                  suffix: List[Dict[str, Any]],
                  term: Term) -> 'SyncCall':
        return cls(cluster_id=ClusterId.from_json(cluster_id),
                   commit_length=commit_length,
                   node_id=node_id,
                   prefix_cluster_id=ClusterId.from_json(prefix_cluster_id),
                   prefix_length=prefix_length,
                   prefix_term=prefix_term,
                   suffix=[Record.from_json(**record) for record in suffix],
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_id': self.cluster_id.as_json(),
                'commit_length': self.commit_length,
                'node_id': self.node_id,
                'prefix_cluster_id': self.prefix_cluster_id.as_json(),
                'prefix_length': self.prefix_length,
                'prefix_term': self.prefix_term,
                'suffix': [record.as_json() for record in self.suffix],
                'term': self.term}


class SyncStatus(enum.IntEnum):
    CONFLICT = enum.auto()
    FAILURE = enum.auto()
    SUCCESS = enum.auto()
    UNAVAILABLE = enum.auto()


class SyncReply:
    __slots__ = '_accepted_length', '_node_id', '_status', '_term'

    def __new__(cls,
                *,
                accepted_length: int,
                node_id: NodeId,
                status: SyncStatus,
                term: Term) -> 'SyncReply':
        self = super().__new__(cls)
        self._accepted_length, self._node_id, self._status, self._term = (
            accepted_length, node_id, status, term
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
    def status(self) -> SyncStatus:
        return self._status

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  accepted_length: int,
                  node_id: NodeId,
                  status: int,
                  term: Term) -> 'SyncReply':
        return cls(accepted_length=accepted_length,
                   node_id=node_id,
                   status=SyncStatus(status),
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'accepted_length': self.accepted_length,
                'node_id': self.node_id,
                'status': int(self.status),
                'term': self.term}


class UpdateCall:
    __slots__ = '_cluster_state', '_node_id'

    def __new__(cls,
                *,
                cluster_state: DisjointClusterState,
                node_id: NodeId) -> 'UpdateCall':
        self = super().__new__(cls)
        self._cluster_state, self._node_id = cluster_state, node_id
        return self

    __repr__ = generate_repr(__new__)

    @property
    def cluster_state(self) -> DisjointClusterState:
        return self._cluster_state

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @classmethod
    def from_json(cls,
                  *,
                  cluster_state: Dict[str, Any],
                  node_id: NodeId) -> 'UpdateCall':
        return cls(cluster_state=DisjointClusterState.from_json(
                **cluster_state),
                node_id=node_id)

    def as_json(self) -> Dict[str, Any]:
        return {'cluster_state': self.cluster_state.as_json(),
                'node_id': self.node_id}


class UpdateStatus(enum.IntEnum):
    REJECTED = enum.auto()
    SUCCEED = enum.auto()
    UNAVAILABLE = enum.auto()
    UNGOVERNABLE = enum.auto()
    UNSTABLE = enum.auto()


class UpdateReply:
    __slots__ = '_status',

    def __new__(cls, status: UpdateStatus) -> 'UpdateReply':
        self = super().__new__(cls)
        self._status = status
        return self

    __repr__ = generate_repr(__new__)

    @property
    def status(self) -> UpdateStatus:
        return self._status

    @classmethod
    def from_json(cls, status: int) -> 'UpdateReply':
        return cls(UpdateStatus(status))

    def as_json(self) -> Dict[str, Any]:
        return {'status': int(self.status)}


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
                  log_length: int,
                  log_term: Term,
                  node_id: NodeId,
                  term: Term) -> 'VoteCall':
        return cls(log_length=log_length,
                   log_term=log_term,
                   node_id=node_id,
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'log_length': self.log_length,
                'log_term': self.log_term,
                'node_id': self.node_id,
                'term': self.term}


class VoteStatus(enum.IntEnum):
    CONFLICTS = enum.auto()
    IGNORES = enum.auto()
    OPPOSES = enum.auto()
    REJECTS = enum.auto()
    SUPPORTS = enum.auto()
    UNAVAILABLE = enum.auto()


class VoteReply:
    __slots__ = '_node_id', '_status', '_term'

    def __new__(cls,
                *,
                node_id: NodeId,
                status: VoteStatus,
                term: Term) -> 'VoteReply':
        self = super().__new__(cls)
        self._node_id, self._status, self._term = node_id, status, term
        return self

    __repr__ = generate_repr(__new__)

    @property
    def node_id(self) -> NodeId:
        return self._node_id

    @property
    def status(self) -> VoteStatus:
        return self._status

    @property
    def term(self) -> Term:
        return self._term

    @classmethod
    def from_json(cls,
                  *,
                  node_id: NodeId,
                  status: int,
                  term: Term) -> 'VoteReply':
        return cls(node_id=node_id,
                   status=VoteStatus(status),
                   term=term)

    def as_json(self) -> Dict[str, Any]:
        return {'node_id': self.node_id,
                'status': int(self.status),
                'term': self.term}


class Node:
    __slots__ = ('_app', '_cluster_state', '_commands_executor',
                 '_commands_processors', '_communication',
                 '_election_duration', '_election_task',
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
                   DisjointClusterState(ClusterId(),
                                        heartbeat=heartbeat,
                                        nodes_urls={id_: url},
                                        stable=False),
                   logger=logger,
                   processors=processors)

    def __init__(self,
                 _state: NodeState,
                 _cluster_state: ClusterState,
                 *,
                 logger: Optional[logging.Logger] = None,
                 processors: Optional[Mapping[str, Processor]] = None) -> None:
        self._cluster_state, self._state = _cluster_state, _state
        self._url = self._cluster_state.nodes_urls[self._state.id]
        self._logger = logging.getLogger() if logger is None else logger
        self._processors = {} if processors is None else processors
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

        @web.middleware
        async def error_middleware(
                request: web.Request,
                handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
        ) -> web.StreamResponse:
            try:
                result = await handler(request)
            except web.HTTPException:
                raise
            except Exception:
                logger.exception('Something unexpected happened:')
                raise
            else:
                return result

        self._app.middlewares.append(error_middleware)
        self._app.router.add_delete('/', self._handle_delete)
        self._app.router.add_post('/', self._handle_post)
        self._app.router.add_route(self._communication.HTTP_METHOD, '/',
                                   self._handle_communication)
        for path in self.processors.keys():
            self._app.router.add_post(path, self._handle_record)
        self._commands_processors: Mapping[str, Processor] = MappingProxyType({
            **self.processors,
            SEPARATE_CLUSTERS_ACTION: Node._separate_clusters,
            STABILIZE_CLUSTER_ACTION: Node._stabilize_cluster
        })

    __repr__ = generate_repr(__init__,
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
                result = {
                    'error':
                        ('nonexistent node(s) found: {nodes_ids}'
                         .format(nodes_ids=', '.join(nonexistent_nodes_ids)))
                }
                return web.json_response(result)
            self.logger.debug(
                    ('{id} initializes removal of {nodes_ids}'
                     .format(id=self._state.id,
                             nodes_ids=', '.join(nodes_urls_to_delete))))
            rest_nodes_urls = subtract_mapping(self._cluster_state.nodes_urls,
                                               nodes_urls_to_delete)
        else:
            self.logger.debug(f'{self._state.id} gets removed')
            rest_nodes_urls = dict(self._cluster_state.nodes_urls)
            del rest_nodes_urls[self._state.id]
        call = UpdateCall(
                cluster_state=DisjointClusterState(
                        generate_cluster_id(),
                        heartbeat=self._cluster_state.heartbeat,
                        nodes_urls=rest_nodes_urls,
                        stable=False
                ),
                node_id=self._state.id
        )
        reply = await self._process_update_call(call)
        result = {'error': update_status_to_error_message(reply.status)}
        return web.json_response(result)

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
                result = {
                    'error':
                        ('already existing node(s) found: {nodes_ids}'
                         .format(nodes_ids=', '.join(existing_nodes_ids)))
                }
                return web.json_response(result)
            self.logger.debug('{id} initializes adding of {nodes_ids}'
                              .format(id=self._state.id,
                                      nodes_ids=', '.join(nodes_urls_to_add)))
            call = UpdateCall(
                    cluster_state=DisjointClusterState(
                            generate_cluster_id(),
                            heartbeat=self._cluster_state.heartbeat,
                            nodes_urls=unite_mappings(
                                    self._cluster_state.nodes_urls,
                                    nodes_urls_to_add
                            ),
                            stable=False
                    ),
                    node_id=self._state.id
            )
            reply = await self._process_update_call(call)
            result = {'error': update_status_to_error_message(reply.status)}
            return web.json_response(result)
        else:
            self._solo()
            return web.json_response({'error': None})

    async def _handle_record(self, request: web.Request) -> web.Response:
        parameters = await request.json()
        call = LogCall(command=Command(action=request.path,
                                       parameters=parameters),
                       node_id=self._state.id)
        reply = await self._process_log_call(call)
        result = {'error': log_status_to_error_message(reply.status)}
        return web.json_response(result)

    async def _call_sync(self, node_id: NodeId) -> SyncReply:
        prefix_length = self._state.sent_lengths[node_id]
        call = SyncCall(cluster_id=self._cluster_state.id,
                        commit_length=self._state.commit_length,
                        node_id=self._state.id,
                        prefix_cluster_id
                        =(self._state.log[prefix_length - 1].cluster_id
                          if prefix_length
                          else ClusterId()),
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
                             node_id=node_id,
                             status=SyncStatus.UNAVAILABLE,
                             term=self._state.term)
        else:
            return SyncReply.from_json(**raw_reply)

    async def _call_vote(self, node_id: NodeId) -> VoteReply:
        call = VoteCall(node_id=self._state.id,
                        term=self._state.term,
                        log_length=len(self._state.log),
                        log_term=self._state.log_term)
        try:
            raw_reply = await self._send_call(node_id, CallPath.VOTE, call)
        except (ClientError, OSError):
            return VoteReply(node_id=node_id,
                             status=VoteStatus.UNAVAILABLE,
                             term=self._state.term)
        else:
            return VoteReply.from_json(**raw_reply)

    async def _process_log_call(self, call: LogCall) -> LogReply:
        self.logger.debug(f'{self._state.id} processes {call}')
        if self._state.leader_node_id is None:
            return LogReply(status=LogStatus.UNGOVERNABLE)
        elif self._state.role is not Role.LEADER:
            assert self._state.role is Role.FOLLOWER
            try:
                raw_reply = await asyncio.wait_for(
                        self._send_call(self._state.leader_node_id,
                                        CallPath.LOG, call),
                        self._reelection_lag
                        - (self._to_time() - self._last_heartbeat_time))
            except (asyncio.TimeoutError, ClientError, OSError):
                return LogReply(status=LogStatus.UNAVAILABLE)
            else:
                return LogReply.from_json(**raw_reply)
        elif call.node_id not in self._cluster_state.nodes_ids:
            return LogReply(status=LogStatus.REJECTED)
        else:
            append_record(self._state,
                          Record(cluster_id=self._cluster_state.id,
                                 command=call.command,
                                 term=self._state.term))
            await self._sync_followers_once()
            assert self._state.accepted_lengths[self._state.id] == len(
                    self._state.log)
            return LogReply(status=LogStatus.SUCCEED)

    async def _process_sync_call(self, call: SyncCall) -> SyncReply:
        self.logger.debug(f'{self._state.id} processes {call}')
        clusters_agree = (
                len(self._state.log) >= call.prefix_length
                and ((not self._cluster_state.id
                      or self._cluster_state.id.agrees_with(call.cluster_id))
                     if call.prefix_length == 0
                     else (self._state.log[call.prefix_length - 1].cluster_id
                           == call.prefix_cluster_id))
        )
        if not clusters_agree:
            return SyncReply(accepted_length=0,
                             node_id=self._state.id,
                             status=SyncStatus.CONFLICT,
                             term=self._state.term)
        self._last_heartbeat_time = self._to_time()
        self._restart_reelection_timer()
        if call.term > self._state.term:
            update_state_term(self._state, call.term)
            self._follow(call.node_id)
        elif (call.term == self._state.term
              and self._state.leader_node_id is None):
            self._follow(call.node_id)
        states_agree = (
                call.term == self._state.term
                and (len(self._state.log) >= call.prefix_length
                     and (call.prefix_length == 0
                          or (self._state.log[call.prefix_length - 1].term
                              == call.prefix_term)))
        )
        if states_agree:
            self._append_records(call.prefix_length, call.suffix)
            if call.commit_length > self._state.commit_length:
                self._commit(self._state.log[self._state.commit_length
                                             :call.commit_length])
            return SyncReply(accepted_length=(call.prefix_length
                                              + len(call.suffix)),
                             node_id=self._state.id,
                             status=SyncStatus.SUCCESS,
                             term=self._state.term)
        else:
            return SyncReply(accepted_length=0,
                             node_id=self._state.id,
                             status=SyncStatus.FAILURE,
                             term=self._state.term)

    async def _process_sync_reply(self, reply: SyncReply) -> None:
        self.logger.debug(f'{self._state.id} processes {reply}')
        if self._state.role is not Role.LEADER:
            pass
        elif (reply.status is SyncStatus.CONFLICT
              or reply.status is SyncStatus.UNAVAILABLE):
            pass
        elif reply.term == self._state.term:
            if (reply.status is SyncStatus.SUCCESS
                    and (reply.accepted_length
                         >= self._state.accepted_lengths[reply.node_id])):
                self._state.accepted_lengths[reply.node_id] = (
                    reply.accepted_length
                )
                self._state.sent_lengths[reply.node_id] = reply.accepted_length
                self._try_commit()
            elif self._state.sent_lengths[reply.node_id] > 0:
                assert reply.status is SyncStatus.FAILURE
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
            return UpdateReply(status=UpdateStatus.UNGOVERNABLE)
        elif self._state.role is not Role.LEADER:
            try:
                raw_reply = await asyncio.wait_for(
                        self._send_call(self._state.leader_node_id,
                                        CallPath.UPDATE, call),
                        self._reelection_lag
                        - (self._to_time() - self._last_heartbeat_time))
            except (asyncio.TimeoutError, ClientError, OSError):
                return UpdateReply(status=UpdateStatus.UNAVAILABLE)
            else:
                return UpdateReply.from_json(**raw_reply)
        elif call.node_id not in self._cluster_state.nodes_ids:
            return UpdateReply(status=UpdateStatus.REJECTED)
        elif not self._cluster_state.stable:
            return UpdateReply(status=UpdateStatus.UNSTABLE)
        elif (len(self._cluster_state.nodes_ids) == 1
              and not call.cluster_state.nodes_ids):
            self._detach()
            return UpdateReply(status=UpdateStatus.SUCCEED)
        next_cluster_state = JointClusterState(old=self._cluster_state,
                                               new=call.cluster_state)
        command = Command(action=SEPARATE_CLUSTERS_ACTION,
                          parameters=next_cluster_state.as_json())
        append_record(self._state,
                      Record(cluster_id=self._cluster_state.id,
                             command=command,
                             term=self._state.term))
        self._update_cluster_state(next_cluster_state)
        self._restart_sync_timer()
        return UpdateReply(status=UpdateStatus.SUCCEED)

    async def _process_vote_call(self, call: VoteCall) -> VoteReply:
        self.logger.debug(f'{self._state.id} processes {call}')
        if call.node_id not in self._cluster_state.nodes_ids:
            self.logger.debug(f'{self._state.id} skips voting '
                              f'for {call.node_id} '
                              f'because it is not in cluster state')
            return VoteReply(node_id=self._state.id,
                             status=VoteStatus.REJECTS,
                             term=self._state.term)
        elif (self._state.leader_node_id is not None
              and (self._to_time() - self._last_heartbeat_time
                   < self._cluster_state.heartbeat)):
            self.logger.debug(f'{self._state.id} ignores voting '
                              f'initiated by {call.node_id} '
                              f'because leader {self._state.leader_node_id} '
                              f'can be available')
            return VoteReply(node_id=self._state.id,
                             status=VoteStatus.IGNORES,
                             term=self._state.term)
        if call.term > self._state.term:
            update_state_term(self._state, call.term)
            self._state.role = Role.FOLLOWER
        if (call.term == self._state.term
                and ((call.log_term, call.log_length)
                     >= (self._state.log_term, len(self._state.log)))
                and (self._state.supported_node_id is None
                     or self._state.supported_node_id == call.node_id)):
            self._state.supported_node_id = call.node_id
            return VoteReply(node_id=self._state.id,
                             status=VoteStatus.SUPPORTS,
                             term=self._state.term)
        else:
            return VoteReply(node_id=self._state.id,
                             status=VoteStatus.OPPOSES,
                             term=self._state.term)

    async def _process_vote_reply(self, reply: VoteReply) -> None:
        self.logger.debug(f'{self._state.id} processes {reply}')
        if self._state.role is not Role.CANDIDATE:
            pass
        elif (reply.status is VoteStatus.CONFLICTS
              or reply.status is VoteStatus.IGNORES
              or reply.status is VoteStatus.UNAVAILABLE):
            pass
        elif reply.status is VoteStatus.REJECTS:
            assert (not self._cluster_state.stable
                    and (self._state.id
                         not in self._cluster_state.new.nodes_ids))
            self._state.rejectors_nodes_ids.add(reply.node_id)
            if self._cluster_state.new.has_majority(
                    self._state.rejectors_nodes_ids):
                self._detach()
        elif (reply.term == self._state.term
              and reply.status is VoteStatus.SUPPORTS):
            self._state.supporters_nodes_ids.add(reply.node_id)
            if self._cluster_state.has_majority(
                    self._state.supporters_nodes_ids):
                self._lead()
        elif reply.term > self._state.term:
            assert reply.status is VoteStatus.OPPOSES
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
            if (log[index].term != suffix[index - prefix_length].term
                    or (log[index].cluster_id
                        != suffix[index - prefix_length].cluster_id)):
                del log[prefix_length:]
        if prefix_length + len(suffix) > len(log):
            new_records = suffix[len(log) - prefix_length:]
            for record in reversed(new_records):
                command = record.command
                if command.action == SEPARATE_CLUSTERS_ACTION:
                    cluster_state = JointClusterState.from_json(
                            **command.parameters)
                    self._update_cluster_state(cluster_state)
                    break
                elif command.action == STABILIZE_CLUSTER_ACTION:
                    cluster_state = DisjointClusterState.from_json(
                            **command.parameters
                    )
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

    def _detach(self) -> None:
        self.logger.debug(f'{self._state.id} detaches')
        self._cancel_election_timer()
        self._cancel_reelection_timer()
        self._cancel_sync_timer()
        self._state.leader_node_id = None
        self._state.role = Role.FOLLOWER
        self._update_cluster_state(DisjointClusterState(
                ClusterId(),
                heartbeat=self._cluster_state.heartbeat,
                nodes_urls={self._state.id: self.url},
                stable=False
        ))

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

    def _follow(self, leader_node_id: NodeId) -> None:
        self.logger.info(f'{self._state.id} follows {leader_node_id} '
                         f'in term {self._state.term}')
        self._state.leader_node_id = leader_node_id
        self._state.role = Role.FOLLOWER
        self._cancel_election_timer()

    def _lead(self) -> None:
        self.logger.info(f'{self._state.id} is leader '
                         f'of term {self._state.term}')
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
            self._commands_processors[command.action](self, command.parameters)

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

    def _separate_clusters(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self._state.id} separates clusters')
        assert isinstance(self._cluster_state, JointClusterState)
        if self._state.role is Role.LEADER:
            cluster_state = JointClusterState.from_json(**parameters)
            assert cluster_state == self._cluster_state
            command = Command(action=STABILIZE_CLUSTER_ACTION,
                              parameters=cluster_state.new.as_json())
            append_record(self._state,
                          Record(cluster_id=self._cluster_state.id,
                                 command=command,
                                 term=self._state.term))
            self._update_cluster_state(cluster_state.new)
            self._restart_sync_timer()

    def _solo(self) -> None:
        self.logger.debug(f'{self._state.id} solos')
        self._update_cluster_state(DisjointClusterState(
                generate_cluster_id(),
                heartbeat=self._cluster_state.heartbeat,
                nodes_urls={self._state.id: self.url},
                stable=True
        ))
        self._lead()

    def _stabilize_cluster(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self._state.id} stabilizes cluster')
        assert isinstance(self._cluster_state, DisjointClusterState)
        assert (self._cluster_state
                == DisjointClusterState.from_json(**parameters))
        if self._state.id not in self._cluster_state.nodes_ids:
            self._detach()
        else:
            self._cluster_state = self._cluster_state.stabilize()

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

    def _update_cluster_state(self, cluster_state: ClusterState) -> None:
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


def log_status_to_error_message(status: LogStatus) -> Optional[str]:
    if status is LogStatus.REJECTED:
        return 'node does not belong to cluster'
    elif status is LogStatus.UNAVAILABLE:
        return 'leader is unavailable'
    elif status is LogStatus.UNGOVERNABLE:
        return 'node has no leader'
    else:
        assert status is LogStatus.SUCCEED, f'unknown status: {status!r}'
        return None


def update_status_to_error_message(status: UpdateStatus) -> Optional[str]:
    if status is UpdateStatus.REJECTED:
        return 'node does not belong to cluster'
    elif status is UpdateStatus.UNAVAILABLE:
        return 'leader is unavailable'
    if status is UpdateStatus.UNGOVERNABLE:
        return 'node has no leader'
    elif status is UpdateStatus.UNSTABLE:
        return 'cluster is in unstable mode'
    else:
        assert status is UpdateStatus.SUCCEED, f'unknown status: {status!r}'
        return None
