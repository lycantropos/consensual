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
                    Optional,
                    Sequence)

import requests
from aiohttp import (hdrs,
                     web)
from reprit.base import generate_repr
from requests import (Response,
                      Session)
from yarl import URL

from consensual.raft import (Node,
                             Processor)
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
        self.index, self.heartbeat, self.processors, self.random_seed, self.url = (
            index, heartbeat, processors, random_seed, url,
        )
        self._url_string = str(url)
        self._event = multiprocessing.Event()
        self._process = multiprocessing.Process(
                target=_run_node,
                args=(self.url, self.processors, self.heartbeat,
                      self.random_seed, self._event))
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

    def initialize(self) -> None:
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
                target=_run_node,
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
        self._old_cluster_state = RaftClusterState.from_json(
                **raw_before['cluster']
        )
        self._new_cluster_state = RaftClusterState.from_json(
                **raw_after['cluster']
        )
        self._old_node_state = RaftNodeState.from_json(**raw_before['node'])
        self._new_node_state = RaftNodeState.from_json(**raw_after['node'])


class FilterRecordsWithGreaterLevel:
    def __init__(self, max_level: int) -> None:
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.max_level


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


def _run_node(url: URL,
              processors: Dict[str, Processor],
              heartbeat: float,
              random_seed: int,
              event: multiprocessing.synchronize.Event) -> None:
    atexit.register(event.set)
    node = Node.from_url(url,
                         heartbeat=heartbeat,
                         logger=to_logger(url.authority),
                         processors=processors)
    node._app.router.add_get('/states', to_states_handler(node))
    node._app.middlewares.append(to_latency_simulator(
            max_delay=heartbeat / (2 * MAX_RUNNING_NODES_COUNT),
            random_seed=random_seed))
    node._app.middlewares.append(to_states_appender(node))
    url = node.url
    web.run_app(node._app,
                host=url.host,
                port=url.port,
                loop=node._loop,
                print=lambda message: event.set() or print(message))


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


def to_states_handler(node: Node) -> Handler:
    async def handler(request: web.Request) -> web.Response:
        cluster_data = {'id': node._cluster_state.id.as_json(),
                        'heartbeat': node._cluster_state.heartbeat,
                        'nodes_ids': list(node._cluster_state.nodes_ids),
                        'stable': node._cluster_state.stable}
        node_data = {
            'id': node._state.id,
            'commit_length': node._state.commit_length,
            'leader_node_id': node._state.leader_node_id,
            'log': [record.as_json() for record in node._state.log],
            'role': node._state.role,
            'supported_node_id': node._state.supported_node_id,
            'supporters_nodes_ids': list(node._state.supporters_nodes_ids),
            'term': node._state.term,
        }
        return web.json_response({'cluster': cluster_data,
                                  'node': node_data})

    return handler


def to_states_appender(node: Node) -> Middleware:
    @web.middleware
    async def middleware(request: web.Request,
                         handler: Handler) -> web.StreamResponse:
        if request.method not in (hdrs.METH_POST,
                                  hdrs.METH_DELETE):
            return await handler(request)
        states_before = to_raw_states(node)
        result: web.Response = await handler(request)
        return (web.json_response({'states': {'after': to_raw_states(node),
                                              'before': states_before},
                                   'result': json.loads(result.text)})
                if result.content_type == 'application/json'
                else result)

    return middleware


def to_raw_cluster_state(node: Node) -> Dict[str, Any]:
    return {'id_': node._cluster_state.id.as_json(),
            'heartbeat': node._cluster_state.heartbeat,
            'nodes_ids': list(node._cluster_state.nodes_ids),
            'stable': node._cluster_state.stable}


def to_raw_node_state(node: Node) -> Dict[str, Any]:
    return {'id_': node._state.id,
            'commit_length': node._state.commit_length,
            'leader_node_id': node._state.leader_node_id,
            'log': [record.as_json() for record in node._state.log],
            'role': node._state.role,
            'supported_node_id': node._state.supported_node_id,
            'supporters_nodes_ids':
                list(node._state.supporters_nodes_ids),
            'term': node._state.term}


def to_raw_states(node: Node) -> Dict[str, Any]:
    return {'cluster': to_raw_cluster_state(node),
            'node': to_raw_node_state(node)}
