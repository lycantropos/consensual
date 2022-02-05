import asyncio
import logging.config
import multiprocessing
import socket
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
from aiohttp import web
from reprit.base import generate_repr
from requests import (Response,
                      Session)
from yarl import URL

from consensual.raft import (ClusterId,
                             Node,
                             Processor,
                             Record,
                             Role)
from .raft_cluster_state import RaftClusterState
from .raft_node_state import RaftNodeState


class RaftClusterNode:
    def __init__(self,
                 url: URL,
                 processors: Dict[str, Processor],
                 random_seed: int,
                 *,
                 heartbeat: float) -> None:
        self.heartbeat, self.processors, self.random_seed, self.url = (
            heartbeat, processors, random_seed, url,
        )
        self._url_string = str(url)
        self._process: Optional[multiprocessing.Process] = None
        self._session = Session()

    __repr__ = generate_repr(__init__)

    @classmethod
    def from_ports_range(cls,
                         host: str,
                         ports: Sequence[int],
                         processors: Dict[str, Processor],
                         random_seed: int,
                         *,
                         heartbeat: float) -> 'RaftClusterNode':
        candidates = list(ports)
        generate_index = Random(random_seed).randrange
        while candidates:
            index = generate_index(0, len(candidates))
            candidate = candidates[index]
            if not is_url_reachable(host, candidate,
                                    timeout=heartbeat):
                port = candidate
                break
            del candidates[index]
        else:
            raise RuntimeError(f'all ports from {ports} are occupied')
        url = URL.build(scheme='http',
                        host=host,
                        port=port)
        return cls(url, processors, random_seed,
                   heartbeat=heartbeat)

    def __eq__(self, other: 'RaftClusterNode') -> bool:
        return (self.url == other.url
                if isinstance(other, RaftClusterNode)
                else NotImplemented)

    def __hash__(self) -> int:
        return hash(self.url)

    def add(self, *nodes: 'RaftClusterNode') -> Optional[str]:
        response = requests.post(self._url_string,
                                 json=[str(node.url) for node in nodes])
        response.raise_for_status()
        return response.json()['error']

    def initialize(self) -> None:
        assert requests.post(self._url_string).ok

    def log(self, path: str, parameters: Any) -> None:
        response = requests.post(str(self.url.with_path(path)),
                                 json=parameters)
        response.raise_for_status()
        return response.json()['error']

    def delete(self) -> Optional[str]:
        response = requests.delete(self._url_string)
        response.raise_for_status()
        return response.json()['error']

    def load_cluster_state(self) -> RaftClusterState:
        response = self._get(str(self.url.with_path('/cluster')))
        response.raise_for_status()
        raw_state = response.json()
        raw_id, heartbeat, nodes_ids, stable = (
            raw_state['id'], raw_state['heartbeat'], raw_state['nodes_ids'],
            raw_state['stable'],
        )
        return RaftClusterState(ClusterId.from_json(raw_id),
                                heartbeat=heartbeat,
                                nodes_ids=nodes_ids,
                                stable=stable)

    def load_node_state(self) -> RaftNodeState:
        response = self._get(str(self.url.with_path('/node')))
        response.raise_for_status()
        raw_state = response.json()
        (
            commit_length, id_, leader_node_id, raw_log, raw_node_role,
            supported_node_id, term,
        ) = (
            raw_state['commit_length'], raw_state['id'],
            raw_state['leader_node_id'], raw_state['log'], raw_state['role'],
            raw_state['supported_node_id'], raw_state['term'],
        )
        log = [Record.from_json(**raw_record) for raw_record in raw_log]
        return RaftNodeState(id_,
                             commit_length=commit_length,
                             leader_node_id=leader_node_id,
                             log=log,
                             role=Role(raw_node_role),
                             supported_node_id=supported_node_id,
                             term=term)

    def start(self) -> None:
        self._process = multiprocessing.Process(
                target=_run_node,
                args=(self.url, self.processors, self.heartbeat,
                      self.random_seed))
        self._process.start()

    def stop(self) -> None:
        self._session.close()
        assert self._process is not None
        while self._process.is_alive():
            self._process.terminate()
            time.sleep(1)
        assert not is_url_reachable(self.url.host, self.url.port,
                                    timeout=self.heartbeat)

    def _get(self, endpoint: str) -> Response:
        last_error = None
        for _ in range(10):
            try:
                response = self._session.get(endpoint)
            except OSError as error:
                last_error = error
                time.sleep(1)
            else:
                break
        else:
            raise last_error
        return response


def is_url_reachable(host: str, port: int,
                     *,
                     timeout: float) -> bool:
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.settimeout(timeout)
    with connection:
        try:
            connection.connect((host, port))
        except OSError:
            return False
        else:
            return True


def to_logger(name: str,
              *,
              version: int = 1) -> logging.Logger:
    console_formatter = {'format': '[%(levelname)-8s %(name)s] %(msg)s'}
    formatters = {'console': console_formatter}
    console_handler_config = {'class': 'logging.StreamHandler',
                              'level': logging.DEBUG,
                              'formatter': 'console',
                              'stream': sys.stdout}
    handlers = {'console': console_handler_config}
    loggers = {name: {'level': logging.DEBUG,
                      'handlers': ('console',)}}
    config = {'formatters': formatters,
              'handlers': handlers,
              'loggers': loggers,
              'version': version}
    logging.config.dictConfig(config)
    return logging.getLogger(name)


def _run_node(url: URL,
              processors: Dict[str, Processor],
              heartbeat: float,
              random_seed: int) -> None:
    node = Node.from_url(url,
                         heartbeat=heartbeat,
                         logger=to_logger(url.authority),
                         processors=processors)
    node._app.middlewares.append(to_latency_simulator(max_delay=heartbeat,
                                                      random_seed=random_seed))
    return node.run()


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
                         step_max_delay: float = 3 * max_delay / 4
                         ) -> web.StreamResponse:
        await asyncio.sleep(generate_uniform(0, step_max_delay))
        result = await handler(request)
        await asyncio.sleep(generate_uniform(0, step_max_delay))
        return result

    return middleware
