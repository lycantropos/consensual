import logging.config
import random
import sys
import threading
import uuid
from asyncio import (AbstractEventLoop,
                     Future,
                     TimeoutError,
                     gather,
                     get_event_loop,
                     new_event_loop,
                     set_event_loop,
                     sleep,
                     wait_for)
from collections import (defaultdict,
                         deque)
from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType
from typing import (Any,
                    Collection,
                    Dict,
                    List,
                    Mapping,
                    Optional)

from reprit import seekers
from reprit.base import generate_repr
from yarl import URL

from .cluster import (Cluster,
                      DisjointCluster,
                      JointCluster)
from .cluster_id import ClusterId
from .command import Command
from .hints import (NodeId,
                    Processor,
                    Term,
                    Time)
from .history import (History,
                      RegularHistory,
                      SyncHistory,
                      append_record,
                      append_records,
                      to_log_term,
                      to_nodes_ids_that_accepted_more_records)
from .messages import (LogCall,
                       LogReply,
                       LogStatus,
                       MessageKind,
                       SyncCall,
                       SyncReply,
                       SyncStatus,
                       UpdateCall,
                       UpdateReply,
                       UpdateStatus,
                       VoteCall,
                       VoteReply,
                       VoteStatus)
from .record import Record
from .role import (Candidate,
                   Follower,
                   Leader,
                   Role,
                   RoleKind)
from .sender import (ReceiverUnavailable,
                     Sender)
from .utils import (host_to_ip_address,
                    itemize,
                    subtract_mapping,
                    unite_mappings)


class InternalAction:
    SEPARATE_CLUSTERS = '0'
    STABILIZE_CLUSTER = '1'
    assert SEPARATE_CLUSTERS and not SEPARATE_CLUSTERS.startswith('/')
    assert STABILIZE_CLUSTER and not STABILIZE_CLUSTER.startswith('/')


event_loops: Dict[int, AbstractEventLoop] = defaultdict(new_event_loop)


class Node:
    __slots__ = ('_cluster', '_commit_length', '_election_duration',
                 '_election_task', '_external_commands_executor',
                 '_external_commands_loop', '_external_processors', '_history',
                 '_id', '_internal_processors', '_last_heartbeat_time',
                 '_latencies', '_logger', '_loop', '_reelection_lag',
                 '_reelection_task', '_role', '_sender', '_sync_task', '_url',
                 '__weakref__')

    @classmethod
    def from_url(cls,
                 url: URL,
                 *,
                 heartbeat: Time = 5,
                 logger: Optional[logging.Logger] = None,
                 loop: Optional[AbstractEventLoop] = None,
                 processors: Optional[Mapping[str, Processor]] = None,
                 sender: Sender) -> 'Node':
        id_ = node_url_to_id(url)
        return cls(id_, RegularHistory([]), Follower(term=0),
                   DisjointCluster(ClusterId(),
                                   heartbeat=heartbeat,
                                   nodes_urls={id_: url},
                                   stable=False),
                   logger=logging.getLogger() if logger is None else logger,
                   loop=get_event_loop() if loop is None else loop,
                   processors=processors,
                   sender=sender)

    def __init__(self,
                 _id: NodeId,
                 _history: History,
                 _role: Role,
                 _cluster: Cluster,
                 *,
                 logger: logging.Logger,
                 loop: AbstractEventLoop,
                 processors: Mapping[str, Processor],
                 sender: Sender) -> None:
        (
            self._cluster, self._commit_length, self._history, self._id,
            self._role, self._sender
        ) = _cluster, 0, _history, _id, _role, sender
        self._url = self._cluster.nodes_urls[self._id]
        self._latencies = {node_id: deque([0],
                                          maxlen=10)
                           for node_id in self._cluster.nodes_ids}
        self._logger = logger
        self._loop = loop
        self._external_commands_loop = event_loops[threading.get_ident()]
        self._external_commands_executor = to_commands_executor(
                self._external_commands_loop
        )
        self._external_processors = MappingProxyType({}
                                                     if processors is None
                                                     else processors)
        self._internal_processors = MappingProxyType({
            InternalAction.SEPARATE_CLUSTERS: self._separate_clusters,
            InternalAction.STABILIZE_CLUSTER: self._stabilize_cluster
        })
        self._election_duration = 0
        self._election_task = self._loop.create_future()
        self._last_heartbeat_time = -self._cluster.heartbeat
        self._reelection_lag = 0
        self._reelection_task = self._loop.create_future()
        self._sync_task = self._loop.create_future()

    __repr__ = generate_repr(__init__,
                             field_seeker=seekers.complex_)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def loop(self) -> AbstractEventLoop:
        return self._loop

    @property
    def processors(self) -> Mapping[str, Processor]:
        return self._external_processors

    @property
    def sender(self) -> Sender:
        return self._sender

    @property
    def url(self) -> URL:
        return self._url

    async def attach_nodes(self, urls: List[URL]) -> Optional[str]:
        nodes_urls_to_attach = {node_url_to_id(node_url): node_url
                                for node_url in urls}
        existing_nodes_ids = (nodes_urls_to_attach.keys()
                              & set(self._cluster.nodes_ids))
        if existing_nodes_ids:
            return ('already existing node(s) found: '
                    f'{itemize(existing_nodes_ids)}')
        self.logger.info(f'{self._id} initializes '
                         f'attachment of {itemize(nodes_urls_to_attach)}')
        call = UpdateCall(
                cluster=DisjointCluster(generate_cluster_id(),
                                        heartbeat=self._cluster.heartbeat,
                                        nodes_urls=unite_mappings(
                                                self._cluster.nodes_urls,
                                                nodes_urls_to_attach
                                        ),
                                        stable=False),
                node_id=self._id
        )
        reply = await self._receive_update_call(call)
        return update_status_to_error_message(reply.status)

    async def detach(self) -> Optional[str]:
        self.logger.info(f'{self._id} gets detached')
        rest_nodes_urls = dict(self._cluster.nodes_urls)
        rest_nodes_urls.pop(self._id, None)
        call = UpdateCall(
                cluster=DisjointCluster(generate_cluster_id(),
                                        heartbeat=self._cluster.heartbeat,
                                        nodes_urls=rest_nodes_urls,
                                        stable=False),
                node_id=self._id
        )
        reply = await self._receive_update_call(call)
        return update_status_to_error_message(reply.status)

    async def detach_nodes(self, urls: List[URL]) -> Optional[str]:
        nodes_urls_to_detach = {node_url_to_id(node_url): node_url
                                for node_url in urls}
        nonexistent_nodes_ids = (nodes_urls_to_detach.keys()
                                 - set(self._cluster.nodes_ids))
        if nonexistent_nodes_ids:
            return ('nonexistent node(s) found: '
                    f'{itemize(nonexistent_nodes_ids)}')
        self.logger.info(f'{self._id} initializes '
                         f'detachment of {itemize(nodes_urls_to_detach)}')
        rest_nodes_urls = subtract_mapping(self._cluster.nodes_urls,
                                           nodes_urls_to_detach)
        call = UpdateCall(
                cluster=DisjointCluster(generate_cluster_id(),
                                        heartbeat=self._cluster.heartbeat,
                                        nodes_urls=rest_nodes_urls,
                                        stable=False),
                node_id=self._id
        )
        reply = await self._receive_update_call(call)
        return update_status_to_error_message(reply.status)

    async def enqueue(self, action: str, parameters: Any) -> Optional[str]:
        self.logger.info(f'{self._id} enqueues "{action}" '
                         f'with parameters {parameters}')
        assert action in self.processors, action
        call = LogCall(command=Command(action=action,
                                       parameters=parameters,
                                       internal=False),
                       node_id=self._id)
        reply = await self._receive_log_call(call)
        return log_status_to_error_message(reply.status)

    async def receive(self,
                      *,
                      kind: MessageKind,
                      message: Dict[str, Any]) -> Dict[str, Any]:
        if kind is MessageKind.LOG:
            call_from_json, processor = (LogCall.from_json,
                                         self._receive_log_call)
        elif kind is MessageKind.SYNC:
            call_from_json, processor = (SyncCall.from_json,
                                         self._receive_sync_call)
        elif kind is MessageKind.UPDATE:
            call_from_json, processor = (UpdateCall.from_json,
                                         self._receive_update_call)
        else:
            assert kind is MessageKind.VOTE
            call_from_json, processor = (VoteCall.from_json,
                                         self._receive_vote_call)
        call = call_from_json(**message)
        reply = await processor(call)
        return reply.as_json()

    async def solo(self) -> Optional[str]:
        self.logger.info(f'{self._id} solos')
        self._update_cluster(DisjointCluster(generate_cluster_id(),
                                             heartbeat=self._cluster.heartbeat,
                                             nodes_urls={self._id: self.url},
                                             stable=True))
        self._lead()
        return None

    async def _agitate_voter(self, node_id: NodeId) -> None:
        reply = await self._call_vote(node_id)
        await self._process_vote_reply(reply)

    async def _call_sync(self, node_id: NodeId) -> SyncReply:
        assert self._role.kind is RoleKind.LEADER
        try:
            prefix_length = self._history.sent_lengths[node_id]
        except KeyError:
            return SyncReply(accepted_length=0,
                             node_id=node_id,
                             status=SyncStatus.UNAVAILABLE,
                             term=self._role.term)
        call = SyncCall(cluster_id=self._cluster.id,
                        commit_length=self._commit_length,
                        node_id=self._id,
                        prefix_cluster_id
                        =(self._history.log[prefix_length - 1].cluster_id
                          if prefix_length
                          else ClusterId()),
                        prefix_length=prefix_length,
                        prefix_term=(self._history.log[prefix_length - 1].term
                                     if prefix_length
                                     else 0),
                        suffix=list(self._history.log[prefix_length:]),
                        term=self._role.term)
        try:
            return await self._send_sync_call(node_id, call)
        except ReceiverUnavailable:
            return SyncReply(accepted_length=0,
                             node_id=node_id,
                             status=SyncStatus.UNAVAILABLE,
                             term=self._role.term)

    async def _call_vote(self, node_id: NodeId) -> VoteReply:
        assert self._role.kind is RoleKind.CANDIDATE
        call = VoteCall(node_id=self._id,
                        term=self._role.term,
                        log_length=len(self._history.log),
                        log_term=to_log_term(self._history))
        try:
            return await self._send_vote_call(node_id, call)
        except ReceiverUnavailable:
            return VoteReply(node_id=node_id,
                             status=VoteStatus.UNAVAILABLE,
                             term=self._role.term)

    async def _receive_log_call(self, call: LogCall) -> LogReply:
        self.logger.debug(f'{self._id} processes {call}')
        if self._role.leader_node_id is None:
            assert self._role.kind is not RoleKind.LEADER
            return LogReply(status=LogStatus.UNGOVERNABLE)
        elif self._role.kind is not RoleKind.LEADER:
            assert self._role.leader_node_id != self._id
            assert self._role.kind is RoleKind.FOLLOWER
            try:
                return await wait_for(
                        self._send_log_call(self._role.leader_node_id, call),
                        self._reelection_lag
                        - (self._to_time() - self._last_heartbeat_time)
                )
            except (ReceiverUnavailable, TimeoutError):
                return LogReply(status=LogStatus.UNAVAILABLE)
        elif (call.node_id not in self._cluster.nodes_ids
              and call.node_id != self._id):
            return LogReply(status=LogStatus.REJECTED)
        else:
            append_record(self._history,
                          Record(cluster_id=self._cluster.id,
                                 command=call.command,
                                 term=self._role.term))
            self._restart_sync_timer()
            return LogReply(status=LogStatus.SUCCEED)

    async def _receive_sync_call(self, call: SyncCall) -> SyncReply:
        self.logger.debug(f'{self._id} processes {call}')
        clusters_agree = (self._cluster.id.agrees_with(call.cluster_id)
                          if self._cluster.id
                          else not self._history.log)
        if not clusters_agree:
            return SyncReply(accepted_length=0,
                             node_id=self._id,
                             status=SyncStatus.CONFLICT,
                             term=self._role.term)
        self._last_heartbeat_time = self._to_time()
        self._restart_reelection_timer()
        if call.term > self._role.term:
            self._withdraw(call.term)
        if (call.term == self._role.term
                and self._role.leader_node_id is None
                and self._id != call.node_id):
            self._follow(call.node_id)
        states_agree = (
                call.term == self._role.term
                and
                (len(self._history.log) >= call.prefix_length
                 and (call.prefix_length == 0
                      or ((self._history.log[call.prefix_length - 1].cluster_id
                           == call.prefix_cluster_id)
                          and (self._history.log[call.prefix_length - 1].term
                               == call.prefix_term))))
        )
        if states_agree:
            self._append_records(call.prefix_length, call.suffix)
            if call.commit_length > self._commit_length:
                self._commit(self._history.log[self._commit_length
                                               :call.commit_length])
            return SyncReply(accepted_length=(call.prefix_length
                                              + len(call.suffix)),
                             node_id=self._id,
                             status=SyncStatus.SUCCESS,
                             term=self._role.term)
        else:
            return SyncReply(accepted_length=0,
                             node_id=self._id,
                             status=SyncStatus.FAILURE,
                             term=self._role.term)

    async def _receive_sync_reply(self, reply: SyncReply) -> None:
        self.logger.debug(f'{self._id} processes {reply}')
        if self._role.kind is not RoleKind.LEADER:
            pass
        elif (reply.status is SyncStatus.CONFLICT
              or reply.status is SyncStatus.UNAVAILABLE):
            pass
        elif reply.term == self._role.term:
            if (reply.status is SyncStatus.SUCCESS
                    and (reply.accepted_length
                         >= self._history.accepted_lengths[reply.node_id])):
                self._history.accepted_lengths[reply.node_id] = (
                    reply.accepted_length
                )
                self._history.sent_lengths[reply.node_id] = (
                    reply.accepted_length
                )
                self._try_commit()
            elif self._history.sent_lengths[reply.node_id] > 0:
                self._history.sent_lengths[reply.node_id] = (
                        self._history.sent_lengths[reply.node_id] - 1
                )
                await self._sync_follower(reply.node_id)
        elif reply.term > self._role.term:
            self._withdraw(reply.term)
            self._cancel_election_timer()

    async def _receive_update_call(self, call: UpdateCall) -> UpdateReply:
        self.logger.debug(f'{self._id} processes {call}')
        if (not call.cluster.nodes_ids
                and len(self._cluster.nodes_ids) == 1
                and self._id in self._cluster.nodes_ids):
            if self._cluster.id:
                self._detach()
            else:
                self._reset()
            return UpdateReply(status=UpdateStatus.SUCCEED)
        elif self._role.leader_node_id is None:
            return UpdateReply(status=UpdateStatus.UNGOVERNABLE)
        elif self._role.kind is not RoleKind.LEADER:
            try:
                return await wait_for(
                        self._send_update_call(self._role.leader_node_id,
                                               call),
                        self._reelection_lag
                        - (self._to_time() - self._last_heartbeat_time)
                )
            except (ReceiverUnavailable, TimeoutError):
                return UpdateReply(status=UpdateStatus.UNAVAILABLE)
        elif call.node_id not in self._cluster.nodes_ids:
            return UpdateReply(status=UpdateStatus.REJECTED)
        elif not self._cluster.stable:
            return UpdateReply(status=UpdateStatus.UNSTABLE)
        next_cluster = JointCluster(old=self._cluster,
                                    new=call.cluster)
        command = Command(action=InternalAction.SEPARATE_CLUSTERS,
                          parameters=next_cluster.as_json(),
                          internal=True)
        append_record(self._history,
                      Record(cluster_id=self._cluster.id,
                             command=command,
                             term=self._role.term))
        self._update_cluster(next_cluster)
        self._restart_sync_timer()
        return UpdateReply(status=UpdateStatus.SUCCEED)

    async def _receive_vote_call(self, call: VoteCall) -> VoteReply:
        self.logger.debug(f'{self._id} processes {call}')
        if call.node_id not in self._cluster.nodes_ids:
            self.logger.debug(f'{self._id} skips voting '
                              f'for {call.node_id} '
                              f'because it is not in cluster state')
            return VoteReply(node_id=self._id,
                             status=VoteStatus.REJECTS,
                             term=self._role.term)
        elif (self._role.leader_node_id is not None
              and (self._to_time() - self._last_heartbeat_time
                   < self._cluster.heartbeat)):
            assert self._role.kind is not RoleKind.CANDIDATE
            self.logger.debug(f'{self._id} ignores voting '
                              f'initiated by {call.node_id} '
                              f'because leader {self._role.leader_node_id} '
                              f'can be available')
            return VoteReply(node_id=self._id,
                             status=VoteStatus.IGNORES,
                             term=self._role.term)
        if call.term > self._role.term:
            self._withdraw(call.term)
        if (call.term == self._role.term
                and ((call.log_term, call.log_length)
                     >= (to_log_term(self._history), len(self._history.log)))
                and (self._role.supported_node_id is None
                     or self._role.supported_node_id == call.node_id)):
            assert self._role.kind is not RoleKind.LEADER
            self._role = self._role.support(call.node_id)
            return VoteReply(node_id=self._id,
                             status=VoteStatus.SUPPORTS,
                             term=self._role.term)
        else:
            return VoteReply(node_id=self._id,
                             status=VoteStatus.OPPOSES,
                             term=self._role.term)

    async def _process_vote_reply(self, reply: VoteReply) -> None:
        self.logger.debug(f'{self._id} processes {reply}')
        if self._role.kind is not RoleKind.CANDIDATE:
            pass
        elif (reply.status is VoteStatus.CONFLICTS
              or reply.status is VoteStatus.IGNORES
              or reply.status is VoteStatus.UNAVAILABLE):
            pass
        elif reply.status is VoteStatus.REJECTS:
            assert (
                    not self._cluster.stable
                    and self._id not in self._cluster.new.nodes_ids
            ), (
                self._cluster
            )
            self._role = self._role.rejected_by(reply.node_id)
            if self._cluster.new.has_majority(self._role.rejectors_nodes_ids):
                self._detach()
        elif (reply.term == self._role.term
              and reply.status is VoteStatus.SUPPORTS):
            self._role = self._role.supported_by(reply.node_id)
            if self._cluster.has_majority(self._role.supporters_nodes_ids):
                self._lead()
        elif reply.term > self._role.term:
            assert reply.status is VoteStatus.OPPOSES
            self._withdraw(reply.term)
            self._cancel_election_timer()

    async def _run_election(self) -> None:
        self.logger.debug(f'{self._id} runs election '
                          f'for term {self._role.term + 1}')
        start = self._to_time()
        self._nominate()
        try:
            await wait_for(gather(*[self._agitate_voter(node_id)
                                    for node_id in self._cluster.nodes_ids]),
                           self._election_duration)
        finally:
            duration = self._to_time() - start
            self.logger.debug(f'{self._id} election '
                              f'for term {self._role.term} '
                              f'took {duration}s, '
                              f'timeout: {self._election_duration}, '
                              f'role: {self._role.kind.name}')
            await sleep(self._election_duration - duration)

    async def _send_json(self,
                         node_id: NodeId,
                         kind: MessageKind,
                         message: Dict[str, Any]) -> Dict[str, Any]:
        if node_id == self._id:
            return await self.receive(kind=kind,
                                      message=message)
        latencies = self._latencies[node_id]
        node_url = self._cluster.nodes_urls[node_id]
        message_start = self._to_time()
        result = await self._sender.send(kind=kind,
                                         message=message,
                                         url=node_url)
        reply_end = self._to_time()
        latency = reply_end - message_start
        latencies.append(latency)
        return result

    async def _send_log_call(self, node_id: NodeId, call: LogCall) -> LogReply:
        raw_reply = await self._send_json(node_id, MessageKind.LOG,
                                          call.as_json())
        return LogReply.from_json(**raw_reply)

    async def _send_sync_call(self,
                              node_id: NodeId,
                              call: SyncCall) -> SyncReply:
        raw_reply = await self._send_json(node_id, MessageKind.SYNC,
                                          call.as_json())
        return SyncReply.from_json(**raw_reply)

    async def _send_vote_call(self, node_id: NodeId, call: VoteCall
                              ) -> VoteReply:
        raw_reply = await self._send_json(node_id, MessageKind.VOTE,
                                          call.as_json())
        return VoteReply.from_json(**raw_reply)

    async def _send_update_call(self, node_id: NodeId, call: UpdateCall
                                ) -> UpdateReply:
        raw_reply = await self._send_json(node_id, MessageKind.UPDATE,
                                          call.as_json())
        return UpdateReply.from_json(**raw_reply)

    async def _sync_follower(self, node_id: NodeId) -> None:
        if self._role.kind is not RoleKind.LEADER:
            return
        reply = await self._call_sync(node_id)
        await self._receive_sync_reply(reply)

    async def _sync_followers(self) -> None:
        self.logger.debug(f'{self._id} syncs followers')
        while self._role.kind is RoleKind.LEADER:
            start = self._to_time()
            await self._sync_followers_once()
            duration = self._to_time() - start
            self.logger.debug(f'{self._id} followers\' sync took {duration}s')
            await sleep(self._cluster.heartbeat - duration
                        - self._to_expected_broadcast_time())

    async def _sync_followers_once(self) -> None:
        await gather(*[self._sync_follower(node_id)
                       for node_id in self._cluster.nodes_ids])

    def _append_records(self,
                        prefix_length: int,
                        suffix: List[Record]) -> None:
        log = self._history.log
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
                if command.external:
                    continue
                elif command.action == InternalAction.SEPARATE_CLUSTERS:
                    cluster = JointCluster.from_json(**command.parameters)
                    self._update_cluster(cluster)
                    break
                else:
                    assert command.action == InternalAction.STABILIZE_CLUSTER
                    cluster = DisjointCluster.from_json(**command.parameters)
                    self._update_cluster(cluster)
                    break
            append_records(self._history, new_records)

    def _cancel_election_timer(self) -> None:
        self.logger.debug(f'{self._id} cancels election timer')
        self._election_task.cancel()

    def _cancel_reelection_timer(self) -> None:
        self._reelection_task.cancel()

    def _cancel_sync_timer(self) -> None:
        self._sync_task.cancel()

    def _commit(self, records: Collection[Record]) -> None:
        assert records
        self._commit_length += len(records)
        self._trigger_commands([record.command for record in records])

    def _detach(self) -> None:
        self.logger.debug(f'{self._id} detaches')
        self._cancel_election_timer()
        self._cancel_reelection_timer()
        self._cancel_sync_timer()
        self._withdraw(self._role.term)
        self._update_cluster(DisjointCluster(ClusterId(),
                                             heartbeat=self._cluster.heartbeat,
                                             nodes_urls={self._id: self.url},
                                             stable=False))

    def _election_timer_callback(self, future: Future) -> None:
        if future.cancelled():
            self.logger.debug(f'{self._id} has election been cancelled')
            return
        exception = future.exception()
        if exception is None:
            assert future.result() is None
            self._start_election_timer()
        elif isinstance(exception, TimeoutError):
            future.cancel()
            self.logger.debug(f'{self._id} has election been timed out')
            self._start_election_timer()
        else:
            raise exception

    def _follow(self, leader_node_id: NodeId) -> None:
        assert self._id != leader_node_id, leader_node_id
        self.logger.debug(f'{self._id} follows {leader_node_id} '
                          f'in term {self._role.term}')
        self._history = self._history.to_regular()
        self._role = Follower(leader_node_id=leader_node_id,
                              supported_node_id=self._role.supported_node_id,
                              term=self._role.term)
        self._cancel_election_timer()

    def _lead(self) -> None:
        self.logger.debug(f'{self._id} is leader of term {self._role.term}')
        self._history = SyncHistory.from_nodes_ids(self._history.log,
                                                   self._cluster.nodes_ids)
        self._role = Leader(leader_node_id=self._id,
                            supported_node_id=self._role.supported_node_id,
                            term=self._role.term)
        self._cancel_election_timer()
        self._start_sync_timer()

    def _nominate(self) -> None:
        self._history = self._history.to_regular()
        self._role = Candidate(term=self._role.term + 1)

    def _process_commands(self,
                          commands: List[Command],
                          processors: Mapping[str, Processor]) -> None:
        for command in commands:
            try:
                processor = processors[command.action]
            except KeyError:
                self.logger.error(f'{self._id} has no processor '
                                  f'"{command.action}"')
                continue
            try:
                processor(command.parameters)
            except Exception:
                self.logger.exception(f'Failed processing "{command.action}" '
                                      f'with parameters {command.parameters}:')

    def _reset(self) -> None:
        self.logger.debug(f'{self._id} resets')
        assert not self._cluster.id
        self._commit_length = 0
        self._history.log.clear()
        self._withdraw(0)
        force_shutdown_executor(self._external_commands_executor)
        self._external_commands_executor = to_commands_executor(
                self._external_commands_loop
        )

    def _restart_election_timer(self) -> None:
        self.logger.debug(f'{self._id} restarts election timer '
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
        self.logger.debug(f'{self._id} separates clusters')
        if self._role.kind is RoleKind.LEADER:
            cluster = JointCluster.from_json(**parameters)
            if cluster != self._cluster:
                return
            command = Command(action=InternalAction.STABILIZE_CLUSTER,
                              parameters=cluster.new.as_json(),
                              internal=True)
            append_record(self._history,
                          Record(cluster_id=self._cluster.id,
                                 command=command,
                                 term=self._role.term))
            self._update_cluster(cluster.new)
            self._restart_sync_timer()

    def _stabilize_cluster(self, parameters: Dict[str, Any]) -> None:
        self.logger.debug(f'{self._id} stabilizes cluster')
        if self._cluster != DisjointCluster.from_json(**parameters):
            pass
        elif self._id not in self._cluster.nodes_ids:
            assert self._cluster.nodes_ids
            self._detach()
        else:
            self._update_cluster(self._cluster.stabilize())

    def _start_election_timer(self) -> None:
        self._election_duration = self._to_new_duration()
        self._election_task = self._loop.create_task(self._run_election())
        self._election_task.add_done_callback(self._election_timer_callback)

    def _start_reelection_timer(self) -> None:
        self._reelection_lag = self._to_new_duration()
        self._reelection_task = self._loop.call_later(
                self._reelection_lag, self._restart_election_timer
        )

    def _start_sync_timer(self) -> None:
        self._sync_task = self._loop.create_task(self._sync_followers())

    def _to_expected_broadcast_time(self) -> float:
        return sum(max(latencies) for latencies in self._latencies.values())

    def _to_new_duration(self) -> Time:
        broadcast_time = self._to_expected_broadcast_time()
        heartbeat = self._cluster.heartbeat
        assert (
                broadcast_time < heartbeat
        ), (
            f'broadcast time = {broadcast_time} >= {heartbeat} = heartbeat'
        )
        return heartbeat + random.uniform(broadcast_time, heartbeat)

    def _to_time(self) -> Time:
        return self._loop.time()

    def _trigger_commands(self, commands: List[Command]) -> None:
        external_commands = [command
                             for command in commands
                             if command.external]
        internal_commands = [command
                             for command in commands
                             if command.internal]
        self._process_commands(internal_commands, self._internal_processors)
        if external_commands:
            self._loop.run_in_executor(self._external_commands_executor,
                                       self._process_commands,
                                       external_commands,
                                       self._external_processors)

    def _try_commit(self) -> None:
        assert self._role.kind is RoleKind.LEADER
        next_commit_length = self._commit_length
        while (next_commit_length < len(self._history.log)
               and self._cluster.has_majority(
                        to_nodes_ids_that_accepted_more_records(
                                self._history, next_commit_length
                        )
                )):
            next_commit_length += 1
        if next_commit_length > self._commit_length:
            self._commit(self._history.log[self._commit_length
                                           :next_commit_length])

    def _update_cluster(self, cluster: Cluster) -> None:
        self._sender.urls = cluster.nodes_urls.values()
        self._update_history(cluster.nodes_ids)
        self._update_latencies(cluster.nodes_ids)
        self._update_role(cluster.nodes_ids)
        self._cluster = cluster

    def _update_history(self, nodes_ids: Collection[NodeId]) -> None:
        self._history = self._history.with_nodes_ids(
                set(nodes_ids) | {self._id}
                if self._role.kind is RoleKind.LEADER
                else nodes_ids
        )

    def _update_latencies(self, new_nodes_ids: Collection[NodeId]) -> None:
        new_nodes_ids, old_nodes_ids = (set(new_nodes_ids),
                                        set(self._cluster.nodes_ids))
        for removed_node_id in old_nodes_ids - new_nodes_ids:
            del self._latencies[removed_node_id]
        for added_node_id in new_nodes_ids - old_nodes_ids:
            self._latencies[added_node_id] = deque([0],
                                                   maxlen=10)

    def _update_role(self, nodes_ids: Collection[NodeId]) -> None:
        if (self._role.kind is not RoleKind.LEADER
                and self._role.leader_node_id is not None
                and self._role.leader_node_id not in nodes_ids):
            self._role = Follower(
                    supported_node_id=self._role.supported_node_id,
                    term=self._role.term
            )

    def _withdraw(self, term: Term) -> None:
        self._history = self._history.to_regular()
        self._role = Follower(term=term)


def to_commands_executor(loop: AbstractEventLoop) -> ThreadPoolExecutor:
    return ThreadPoolExecutor(initializer=set_event_loop,
                              initargs=(loop,),
                              max_workers=1,
                              thread_name_prefix='commands')


if sys.version_info >= (3, 9):
    def force_shutdown_executor(executor: ThreadPoolExecutor) -> None:
        executor.shutdown(cancel_futures=True,
                          wait=False)
else:
    def force_shutdown_executor(executor: ThreadPoolExecutor) -> None:
        executor.shutdown(wait=False)


def generate_cluster_id() -> ClusterId:
    return ClusterId(uuid.uuid4().hex)


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


def node_url_to_id(url: URL) -> NodeId:
    return f'{host_to_ip_address(url.host)}:{url.port}'


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
