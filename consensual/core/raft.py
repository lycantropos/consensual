import asyncio
import dataclasses
import enum
import logging.config
import random
import traceback
from asyncio import get_event_loop
from collections import deque
from typing import (Any,
                    Awaitable,
                    Callable,
                    Collection,
                    Dict,
                    Generic,
                    List,
                    Mapping,
                    NoReturn,
                    Optional,
                    TypeVar)

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

MIN_DURATION = 5

NodeId = str
_T1 = TypeVar('_T1')
_T2 = TypeVar('_T2')
Route = Callable[['Node', _T1], Awaitable[_T2]]
Term = int

_T = TypeVar('_T')


@dataclasses.dataclass
class Command(Generic[_T]):
    path: str
    parameters: Any


@dataclasses.dataclass
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
    VOTE = 2


class Role(enum.IntEnum):
    CANDIDATE = 0
    FOLLOWER = 1
    LEADER = 2


class Call(Protocol):
    def as_json(self) -> Dict[str, Any]:
        return {}


@dataclasses.dataclass
class LogCall:
    command: Command

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_json(cls, command: Dict[str, Any]) -> 'LogCall':
        return cls(Command(**command))


@dataclasses.dataclass
class LogReply:
    error: Optional[str]

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class SyncCall:
    node_id: NodeId
    term: Term
    prefix_length: int
    prefix_term: Term
    commit_length: int
    suffix: List[Record]

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_json(cls,
                  *,
                  suffix: List[Dict[str, Any]],
                  **kwargs: Any) -> 'SyncCall':
        return cls(suffix=[Record.from_json(**raw_record)
                           for raw_record in suffix],
                   **kwargs)


@dataclasses.dataclass
class SyncReply:
    node_id: NodeId
    term: Term
    acknowledged_length: int
    successful: bool

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class VoteCall:
    node_id: NodeId
    term: Term
    log_length: int
    log_term: Term

    def as_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
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


class Node:
    __slots__ = ('_acknowledged_lengths', '_app', '_calls', '_commit_length',
                 '_election_duration', '_election_task', '_heartbeat', '_id',
                 '_latencies', '_leader', '_log', '_log_task', '_logger',
                 '_loop', '_reelection_lag', '_reelection_task', '_results',
                 '_role', '_routes', '_senders', '_sent_lengths', '_session',
                 '_sync_task', '_term', '_urls', '_voted_for', '_votes')

    def __init__(self,
                 id_: NodeId,
                 urls: Mapping[NodeId, URL],
                 *,
                 heartbeat: int = MIN_DURATION,
                 log: Optional[List[Record]] = None,
                 logger: Optional[logging.Logger] = None,
                 routes: Dict[str, Route],
                 term: Term = 0,
                 voted_for: Optional[NodeId] = None) -> None:
        self._heartbeat = heartbeat
        self._id = id_
        self._log = [] if log is None else log
        self._logger = logging.getLogger() if logger is None else logger
        self._loop = get_event_loop()
        self._routes = routes
        self._term = term
        self._urls = urls
        self._voted_for = voted_for
        self._acknowledged_lengths = {node_id: 0 for node_id in self.nodes_ids}
        self._app = web.Application()
        self._calls = {node_id: asyncio.Queue() for node_id in self.nodes_ids}
        self._commit_length = 0
        self._election_duration = 0
        self._election_task = self._loop.create_future()
        self._latencies: Dict[NodeId, deque] = {node_id: deque([0],
                                                               maxlen=10)
                                                for node_id in self.nodes_ids}
        self._leader, self._role = None, Role.FOLLOWER
        self._log_task = self._loop.create_future()
        self._log_task.set_result(None)
        self._reelection_lag = 0
        self._reelection_task = self._loop.create_future()
        self._results = {node_id: {path: asyncio.Queue() for path in CallPath}
                         for node_id in self.nodes_ids}
        self._senders = {node_id: self._loop.create_task(self._sender(node_id))
                         for node_id in self.nodes_ids}
        self._sent_lengths = {node_id: 0 for node_id in self.nodes_ids}
        self._session = ClientSession(loop=self._loop)
        self._sync_task = self._loop.create_future()
        self._votes = set()
        self._app.router.add_post('/', self._handle)
        for path in self.routes.keys():
            self._app.router.add_post(path, self._handle_route)

    __repr__ = generate_repr(__init__,
                             field_seeker=seekers.complex_)

    @property
    def heartbeat(self) -> int:
        return self._heartbeat

    @property
    def id(self) -> NodeId:
        return self._id

    @property
    def log(self) -> List[Record]:
        return self._log

    @property
    def log_term(self) -> Term:
        return self.log[-1].term if self.log else 0

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def majority_count(self) -> int:
        return ceil_division(self.nodes_count + 1, 2)

    @property
    def nodes_count(self) -> int:
        return len(self.urls)

    @property
    def nodes_ids(self) -> Collection[NodeId]:
        return self.urls.keys()

    @property
    def routes(self) -> Dict[str, Route]:
        return self._routes

    @property
    def term(self) -> Term:
        return self._term

    @property
    def urls(self) -> Mapping[NodeId, URL]:
        return self._urls

    @property
    def voted_for(self) -> Optional[NodeId]:
        return self._voted_for

    def run(self) -> None:
        self._start_reelection_timer()
        url = self.urls[self.id]
        web.run_app(self._app,
                    host=url.host,
                    port=url.port,
                    loop=self._loop)

    async def _agitate_voter(self, node_id: NodeId) -> None:
        reply = await self._call_vote(node_id)
        await self._process_vote_reply(reply)

    async def _handle(self, request: web.Request) -> web_ws.WebSocketResponse:
        websocket = web_ws.WebSocketResponse()
        await websocket.prepare(request)
        routes = {CallPath.LOG: (LogCall.from_json, self._process_log_call),
                  CallPath.SYNC: (SyncCall.from_json, self._process_sync_call),
                  CallPath.VOTE: (VoteCall, self._process_vote_call)}
        async for message in websocket:
            message: web_ws.WSMessage
            raw_call = message.json()
            call_cls, processor = routes[CallPath(raw_call['path'])]
            call = call_cls(**raw_call['data'])
            reply = await processor(call)
            await websocket.send_json(reply.as_json())
        return websocket

    async def _handle_route(self, request: web.Request) -> web.Response:
        parameters = await request.json()
        reply = await self._process_log_call(
                LogCall(Command(path=request.path,
                                parameters=parameters)))
        return web.json_response(reply.as_json())

    async def _call_sync(self, node_id: NodeId) -> SyncReply:
        prefix_length = self._sent_lengths[node_id]
        call = SyncCall(node_id=self.id,
                        term=self.term,
                        prefix_length=prefix_length,
                        prefix_term=(self.log[prefix_length - 1].term
                                     if prefix_length
                                     else 0),
                        commit_length=self._commit_length,
                        suffix=self.log[prefix_length:])
        try:
            raw_reply = await self._send_call(node_id, CallPath.SYNC, call)
        except (ClientError, OSError):
            return SyncReply(node_id=node_id,
                             term=self.term,
                             acknowledged_length=0,
                             successful=False)
        else:
            return SyncReply(**raw_reply)

    async def _call_vote(self, node_id: NodeId) -> VoteReply:
        call = VoteCall(node_id=self.id,
                        term=self.term,
                        log_length=len(self.log),
                        log_term=self.log_term)
        try:
            raw_reply = await self._send_call(node_id, CallPath.VOTE, call)
        except (ClientError, OSError):
            return VoteReply(node_id=node_id,
                             term=self.term,
                             supports=False)
        else:
            return VoteReply(**raw_reply)

    async def _process_log_call(self, call: LogCall) -> LogReply:
        assert self._leader is not None
        if self._role is Role.LEADER:
            self.log.append(Record(call.command, self.term))
            await self._sync_followers_once()
            assert self._acknowledged_lengths[self.id] == len(self.log)
            return LogReply(error=None)
        else:
            assert self._role is Role.FOLLOWER
            try:
                raw_reply = await self._send_call(self._leader, CallPath.LOG,
                                                  call)
            except (ClientError, OSError) as exception:
                return LogReply(error=format_exception(exception))
            else:
                return LogReply(**raw_reply)

    def _process_records(self, records: List[Record]) -> None:
        def process(_: asyncio.Future) -> None:
            self.logger.debug(f'{self.id} processes {records} '
                              f'with log {self.log}')
            self._log_task = self._loop.create_task(
                    self._process_sequentially(records))

        self._log_task.add_done_callback(process)

    async def _process_sequentially(self, records: List[Record]) -> None:
        for record in records:
            await self.routes[record.command.path](self,
                                                   record.command.parameters)
        self.logger.debug(f'{self.id} finished processing {records} '
                          f'with log {self.log}')

    async def _process_sync_call(self, call: SyncCall) -> SyncReply:
        self.logger.debug(f'{self.id} processes {call}')
        self._restart_reelection_timer()
        if call.term > self.term:
            self._term = call.term
            self._voted_for = None
            self._cancel_election_timer()
        if call.term == self.term and call.node_id != self.id:
            self._leader, self._role = call.node_id, Role.FOLLOWER
        if (call.term == self.term
                and (len(self.log) >= call.prefix_length
                     and (call.prefix_length == 0
                          or (self.log[call.prefix_length - 1].term
                              == call.prefix_term)))):
            self._append_records(call.prefix_length, call.suffix)
            self._update_commit_length(call.commit_length)
            return SyncReply(node_id=self.id,
                             term=self.term,
                             acknowledged_length=(call.prefix_length
                                                  + len(call.suffix)),
                             successful=True)
        else:
            return SyncReply(node_id=self.id,
                             term=self.term,
                             acknowledged_length=0,
                             successful=False)

    async def _process_sync_reply(self, reply: SyncReply) -> None:
        self.logger.debug(f'{self.id} processes {reply}')
        if reply.term == self.term and self._role is Role.LEADER:
            if (reply.successful
                    and (reply.acknowledged_length
                         >= self._acknowledged_lengths[reply.node_id])):
                self._acknowledged_lengths[reply.node_id] = (
                    reply.acknowledged_length)
                self._sent_lengths[reply.node_id] = (
                    reply.acknowledged_length)
                self._commit_records()
            elif self._sent_lengths[reply.node_id] > 0:
                self._sent_lengths[reply.node_id] = (
                        self._sent_lengths[reply.node_id] - 1)
                await self._sync_follower(reply.node_id)
        elif reply.term > self.term:
            self._role = Role.FOLLOWER
            self._term = reply.term
            self._voted_for = None
            self._cancel_election_timer()

    async def _process_vote_call(self, call: VoteCall) -> VoteReply:
        self.logger.debug(f'{self.id} processes {call}')
        if call.term > self.term:
            self._role = Role.FOLLOWER
            self._term = call.term
            self._voted_for = None
        if (call.term == self.term
                and ((call.log_term, call.log_length)
                     >= (self.log_term, len(self.log)))
                and (self.voted_for is None
                     or self.voted_for == call.node_id)):
            self._voted_for = call.node_id
            return VoteReply(node_id=self.id,
                             term=self.term,
                             supports=True)
        else:
            return VoteReply(node_id=self.id,
                             term=self.term,
                             supports=False)

    async def _process_vote_reply(self, reply: VoteReply) -> None:
        self.logger.debug(f'{self.id} processes {reply}')
        if (self._role is Role.CANDIDATE
                and reply.term == self.term
                and reply.supports):
            self._votes.add(reply.node_id)
            if len(self._votes) >= self.majority_count:
                self._lead()
        elif reply.term > self.term:
            self._term = reply.term
            self._role = Role.FOLLOWER
            self._voted_for = None
            self._cancel_election_timer()

    async def _run_election(self) -> None:
        self._role = Role.CANDIDATE
        self._term += 1
        self._voted_for = None
        self._votes.clear()
        self.logger.debug(f'{self.id} runs election for term {self.term}')
        start = self._to_time()
        try:
            await asyncio.wait_for(
                    asyncio.gather(*[self._agitate_voter(node_id)
                                     for node_id in self.nodes_ids]),
                    self._election_duration)
        finally:
            end = self._to_time()
            self.logger.debug(f'{self.id} election for term {self.term} '
                              f'took {end - start}, '
                              f'timeout: {self._election_duration}, '
                              f'role: {self._role.name}')

    async def _send_call(self,
                         to: NodeId,
                         path: CallPath,
                         call: Call) -> Dict[str, Any]:
        self._calls[to].put_nowait((path, call))
        result = await self._results[to][path].get()
        return result.value

    async def _sender(self, to: NodeId) -> None:
        url = self.urls[to]
        calls, results, latencies = (self._calls[to], self._results[to],
                                     self._latencies[to])
        path, call = await calls.get()
        while True:
            try:
                async with self._session.ws_connect(
                        url,
                        method=hdrs.METH_POST,
                        timeout=self.heartbeat,
                        headers={'NodeId': self.id},
                        heartbeat=self.heartbeat) as connection:
                    call_start = self._loop.time()
                    await connection.send_json({'path': path,
                                                'data': call.as_json()})
                    async for reply in connection:
                        reply: web_ws.WSMessage
                        reply_end = self._loop.time()
                        latency = reply_end - call_start
                        latencies.append(latency)
                        results[path].put_nowait(_Ok(reply.json()))
                        path, call = await calls.get()
                        call_start = self._loop.time()
                        await connection.send_json({'path': path,
                                                    'data': call.as_json()})
            except (ClientError, OSError) as exception:
                results[path].put_nowait(_Error(exception))
                path, call = await calls.get()

    async def _sync_follower(self, node_id: NodeId) -> None:
        reply = await self._call_sync(node_id)
        await self._process_sync_reply(reply)

    async def _sync_followers(self) -> None:
        self.logger.info(f'{self.id} syncs followers')
        while self._role is Role.LEADER:
            await self._sync_followers_once()
            await asyncio.sleep(self.heartbeat
                                - self._to_expected_accumulated_latency())

    async def _sync_followers_once(self) -> None:
        await asyncio.gather(*[self._sync_follower(node_id)
                               for node_id in self.nodes_ids])

    def _append_records(self,
                        prefix_length: int,
                        suffix: List[Record]) -> None:
        if suffix and len(self.log) > prefix_length:
            index = min(len(self.log), prefix_length + len(suffix)) - 1
            if self.log[index].term != suffix[index - prefix_length].term:
                self.log[:] = self.log[:prefix_length]
        if prefix_length + len(suffix) > len(self.log):
            self.log.extend(suffix[len(self.log) - prefix_length:])

    def _cancel_election_timer(self) -> None:
        self.logger.debug(f'{self.id} cancels election timer')
        self._election_task.cancel()

    def _cancel_reelection_timer(self) -> None:
        self._reelection_task.cancel()

    def _commit_records(self) -> None:
        while self._commit_length < len(self.log):
            acknowledgements = sum(
                    length > self._commit_length
                    for length in self._acknowledged_lengths.values())
            if acknowledgements >= self.majority_count:
                self._process_records([self.log[self._commit_length]])
                self._commit_length += 1
            else:
                break

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

    def _lead(self) -> None:
        self.logger.info(f'{self.id} is leader of term {self.term}')
        self._leader, self._role = self.id, Role.LEADER
        self._cancel_election_timer()
        for node_id in self.nodes_ids:
            self._sent_lengths[node_id] = len(self.log)
            self._acknowledged_lengths[node_id] = 0
        self._start_sync_timer()

    def _restart_election_timer(self) -> None:
        self.logger.debug(f'{self.id} restarts election timer '
                          f'after {self._reelection_lag}'
                          + (''
                             if self._leader is None
                             else f' with {self._latencies[self._leader]} '
                                  f'leader latencies'))
        self._cancel_election_timer()
        self._start_election_timer()

    def _restart_reelection_timer(self) -> None:
        self._cancel_reelection_timer()
        self._start_reelection_timer()

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

    def _to_expected_accumulated_latency(self) -> float:
        return sum(max(latencies) for latencies in self._latencies.values())

    def _to_expected_latency(self, node_id: NodeId) -> float:
        return max(self._latencies[node_id])

    def _to_new_duration(self) -> float:
        min_timeout = (self._to_expected_accumulated_latency()
                       if self._role is Role.LEADER
                       else (self._leader is not None
                             and self._to_expected_latency(self._leader)))
        return (self.heartbeat
                + random.uniform(min_timeout,
                                 max(self.heartbeat, min_timeout)))

    def _to_time(self) -> float:
        return self._loop.time()

    def _update_commit_length(self, commit_length: int) -> None:
        if commit_length > self._commit_length:
            self._process_records(self.log[self._commit_length:commit_length])
            self._commit_length = commit_length


def ceil_division(dividend: int, divisor: int) -> int:
    return -((-dividend) // divisor)


def format_exception(value: Exception) -> str:
    return traceback.format_exception(type(value), value, value.__traceback__)
