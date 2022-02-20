import asyncio
import atexit
import json
import logging.config
import multiprocessing.synchronize
import sys
import time
from random import Random
from typing import (Any,
                    Awaitable,
                    Callable,
                    Dict,
                    List,
                    Mapping,
                    Optional,
                    Sequence)

import requests
from aiohttp import (hdrs,
                     web)
from reprit.base import generate_repr
from requests import (Response,
                      Session)
from yarl import URL

from consensual.core.raft.command import Command
from consensual.raft import (Node,
                             Processor,
                             aiohttp)
from .raft_cluster_state import RaftClusterState
from .raft_node_state import RaftNodeState
from .utils import MAX_RUNNING_NODES_COUNT


class RaftClusterNode:
    def __init__(self,
                 index: int,
                 url: URL,
                 processors: Dict[str, Processor],
                 random_seed: int,
                 *,
                 heartbeat: float) -> None:
        (
            self.index, self.heartbeat, self.processors, self.random_seed,
            self.url
        ) = index, heartbeat, processors, random_seed, url
        self._url_string = str(url)
        self._event = multiprocessing.Event()
        self._process = multiprocessing.Process(
                target=run_node,
                args=(self.url, self.processors, self.heartbeat,
                      self.random_seed, self._event)
        )
        self._session = Session()
        self._old_cluster_state: Optional[RaftClusterState] = None
        self._old_node_state: Optional[RaftNodeState] = None
        self._new_cluster_state: Optional[RaftClusterState] = None
        self._new_node_state: Optional[RaftNodeState] = None

    __repr__ = generate_repr(__init__)

    @classmethod
    def running_from_one_of_ports(cls,
                                  index: int,
                                  host: str,
                                  ports: Sequence[int],
                                  processors: Dict[str, Processor],
                                  random_seed: int,
                                  *,
                                  heartbeat: float) -> 'RaftClusterNode':
        candidates = list(ports)
        generate_index = Random(random_seed).randrange
        while candidates:
            candidate_index = generate_index(0, len(candidates))
            candidate = candidates[candidate_index]
            url = URL.build(scheme='http',
                            host=host,
                            port=candidate)
            self = cls(index, url, processors, random_seed,
                       heartbeat=heartbeat)
            if self.start():
                break
        else:
            raise RuntimeError(f'all ports from {ports} are occupied')
        self.update_states()
        return self

    def __eq__(self, other: 'RaftClusterNode') -> bool:
        return (self.url == other.url
                if isinstance(other, RaftClusterNode)
                else NotImplemented)

    def __hash__(self) -> int:
        return hash(self.url)

    @property
    def old_cluster_state(self) -> RaftClusterState:
        return self._old_cluster_state

    @property
    def old_node_state(self) -> RaftNodeState:
        return self._old_node_state

    @property
    def new_cluster_state(self) -> RaftClusterState:
        return self._new_cluster_state

    @property
    def new_node_state(self) -> RaftNodeState:
        return self._new_node_state

    def add(self, node: 'RaftClusterNode', *rest: 'RaftClusterNode'
            ) -> Optional[str]:
        response = requests.post(self._url_string,
                                 json=[str(node.url)
                                       for node in [node, *rest]])
        response.raise_for_status()
        response_data = response.json()
        self._update_states(response_data['states'])
        return response_data['result']['error']

    def solo(self) -> None:
        response = requests.post(self._url_string)
        response.raise_for_status()
        response_data = response.json()
        self._update_states(response_data['states'])
        return response_data['result']['error']

    def log(self, path: str, parameters: Any) -> None:
        response = requests.post(str(self.url.with_path(path)),
                                 json=parameters)
        response.raise_for_status()
        response_data = response.json()
        self._update_states(response_data['states'])
        return response_data['result']['error']

    def delete(self, *nodes: 'RaftClusterNode') -> Optional[str]:
        response = requests.delete(self._url_string,
                                   json=([str(node.url) for node in nodes]
                                         if nodes
                                         else None))
        response.raise_for_status()
        response_data = response.json()
        self._update_states(response_data['states'])
        return response_data['result']['error']

    def load_cluster_state(self) -> RaftClusterState:
        result = self._update_cluster_state()
        return result

    def restart(self) -> bool:
        assert self._process is None
        return self.start()

    def start(self) -> bool:
        self._event = multiprocessing.Event()
        self._process = multiprocessing.Process(
                target=run_node,
                args=(self.url, self.processors, self.heartbeat,
                      self.random_seed, self._event))
        self._process.start()
        self._event.wait()
        del self._event
        time.sleep(1)
        if not self._process.is_alive():
            return False
        self.update_states()
        return True

    def stop(self) -> None:
        self._session.close()
        assert self._process is not None
        for _ in range(5):
            if not self._process.is_alive():
                break
            self._process.terminate()
            time.sleep(1)
        else:
            self._process.kill()
            time.sleep(5)
        self._new_cluster_state = self._old_cluster_state = None
        self._new_node_state = self._old_node_state = None
        self._process = None

    def update_states(self) -> None:
        response = self._get('/states')
        response.raise_for_status()
        raw_states = response.json()
        raw_cluster_state = raw_states['cluster']
        cluster_state = RaftClusterState.from_json(raw_cluster_state.pop('id'),
                                                   **raw_cluster_state)
        raw_node_state = raw_states['node']
        node_state = RaftNodeState.from_json(raw_node_state.pop('id'),
                                             **raw_node_state)
        self._new_cluster_state, self._old_cluster_state = (
            cluster_state,
            cluster_state
            if self._new_cluster_state is None
            else self._new_cluster_state
        )
        self._new_node_state, self._old_node_state = (
            node_state,
            node_state
            if self._new_node_state is None
            else self._new_node_state
        )

    def _get(self, path: str) -> Response:
        last_error = None
        for _ in range(10):
            try:
                response = self._session.get(str(self.url.with_path(path)))
            except OSError as error:
                last_error = error
                time.sleep(1)
            else:
                break
        else:
            raise last_error
        return response

    def _update_states(self, raw: Dict[str, Any]) -> None:
        raw_after, raw_before = raw['after'], raw['before']
        raw_cluster_after, raw_cluster_before = (raw_after['cluster'],
                                                 raw_before['cluster'])
        self._old_cluster_state = RaftClusterState.from_json(
                raw_cluster_before.pop('id'), **raw_cluster_before
        )
        self._new_cluster_state = RaftClusterState.from_json(
                raw_cluster_after.pop('id'), **raw_cluster_after
        )
        raw_node_after, raw_node_before = raw_after['node'], raw_before['node']
        self._old_node_state = RaftNodeState.from_json(
                raw_node_before.pop('id'), **raw_node_before
        )
        self._new_node_state = RaftNodeState.from_json(
                raw_node_after.pop('id'), **raw_node_after
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


def run_node(url: URL,
             processors: Dict[str, Processor],
             heartbeat: float,
             random_seed: int,
             event: multiprocessing.synchronize.Event) -> None:
    atexit.register(event.set)
    sender = aiohttp.Sender(heartbeat=heartbeat,
                            urls=[url])
    node = WrappedNode.from_url(url,
                                heartbeat=heartbeat,
                                logger=to_logger(url.authority),
                                processors=processors,
                                sender=sender)
    receiver = aiohttp.Receiver.from_node(node)
    receiver.app.router.add_get('/states', to_states_handler(node))
    receiver.app.middlewares.append(
            to_latency_simulator(max_delay=(heartbeat
                                            / (MAX_RUNNING_NODES_COUNT ** 2)),
                                 random_seed=random_seed)
    )
    receiver.app.middlewares.append(to_states_appender(node))
    url = node.url
    web.run_app(receiver.app,
                host=url.host,
                port=url.port,
                loop=node._loop,
                print=lambda message: event.set() or print(message))


class WrappedNode(Node):
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


Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]
Middleware = Callable[[web.Request, Handler], Awaitable[web.StreamResponse]]


def to_latency_simulator(*,
                         max_delay: float,
                         random_seed: int) -> Middleware:
    @web.middleware
    async def middleware(request: web.Request,
                         handler: Handler,
                         generate_uniform: Callable[[float, float], float]
                         = Random(random_seed).uniform,
                         half_max_delay: float = max_delay / 2
                         ) -> web.StreamResponse:
        await asyncio.sleep(generate_uniform(0, half_max_delay))
        result = await handler(request)
        await asyncio.sleep(generate_uniform(0, half_max_delay))
        return result

    return middleware


def to_states_appender(node: WrappedNode) -> Middleware:
    @web.middleware
    async def middleware(request: web.Request,
                         handler: Handler) -> web.StreamResponse:
        if request.method not in (hdrs.METH_DELETE, hdrs.METH_POST):
            return await handler(request)
        states_before = to_raw_states(node)
        result: web.Response = await handler(request)
        return (web.json_response({'states': {'after': to_raw_states(node),
                                              'before': states_before},
                                   'result': json.loads(result.text)})
                if result.content_type == 'application/json'
                else result)

    return middleware


def to_states_handler(node: WrappedNode) -> Handler:
    async def handler(request: web.Request) -> web.Response:
        return web.json_response(to_raw_states(node))

    return handler


def to_raw_cluster_state(node: Node) -> Dict[str, Any]:
    state = node._cluster_state
    return {'id': state.id.as_json(),
            'heartbeat': state.heartbeat,
            'nodes_ids': list(state.nodes_ids),
            'stable': state.stable}


def to_raw_node_state(node: WrappedNode) -> Dict[str, Any]:
    return {'id': node._id,
            'commit_length': node._commit_length,
            'leader_node_id': node._role.leader_node_id,
            'log': [record.as_json() for record in node._history.log],
            'processed_external_commands': [
                command.as_json()
                for command in node.processed_external_commands
            ],
            'processed_internal_commands': [
                command.as_json()
                for command in node.processed_internal_commands
            ],
            'role_kind': node._role.kind,
            'term': node._role.term}


def to_raw_states(node: WrappedNode) -> Dict[str, Any]:
    return {'cluster': to_raw_cluster_state(node),
            'node': to_raw_node_state(node)}
