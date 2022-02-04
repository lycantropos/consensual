import logging.config
import multiprocessing
import socket
import sys
import time
from typing import (Any,
                    Dict,
                    Optional,
                    Tuple)

import requests
from reprit.base import generate_repr
from requests import (Response,
                      Session)
from yarl import URL

from consensual.raft import (ClusterId,
                             Node,
                             Processor,
                             Record,
                             Role)
from .running_cluster_state import RunningClusterState
from .running_node_state import RunningNodeState


class RunningNode:
    def __init__(self,
                 url: URL,
                 processors: Optional[Dict[str, Processor]],
                 *,
                 heartbeat: float) -> None:
        self.heartbeat, self.processors, self.url = (
            heartbeat, {} if processors is None else processors, url,
        )
        self._url_string = str(url)
        assert not is_url_reachable(self.url)
        self._process: Optional[multiprocessing.Process] = None
        self._session = Session()

    __repr__ = generate_repr(__init__)

    def __eq__(self, other: 'RunningNode') -> bool:
        return (self.url == other.url and self.heartbeat == other.heartbeat
                if isinstance(other, RunningNode)
                else NotImplemented)

    def __hash__(self) -> int:
        return hash((self.url, self.heartbeat))

    def add(self, *nodes: 'RunningNode') -> Optional[str]:
        response = requests.post(self._url_string,
                                 json=[str(node.url) for node in nodes])
        response.raise_for_status()
        return response.json()['error']

    def initialize(self) -> None:
        assert requests.post(self._url_string).ok

    def log(self, path_with_parameters: Tuple[str, Any]) -> None:
        path, parameters = path_with_parameters
        response = requests.post(str(self.url.with_path(path)),
                                 json=parameters)
        response.raise_for_status()
        return response.json()['error']

    def delete(self) -> Optional[str]:
        response = requests.delete(self._url_string)
        response.raise_for_status()
        return response.json()['error']

    def load_cluster_state(self) -> RunningClusterState:
        response = self._get(str(self.url.with_path('/cluster')))
        response.raise_for_status()
        raw_state = response.json()
        raw_id, heartbeat, nodes_ids, stable = (
            raw_state['id'], raw_state['heartbeat'], raw_state['nodes_ids'],
            raw_state['stable'],
        )
        return RunningClusterState(ClusterId.from_json(raw_id),
                                   heartbeat=heartbeat,
                                   nodes_ids=nodes_ids,
                                   stable=stable)

    def load_node_state(self) -> RunningNodeState:
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
        return RunningNodeState(id_,
                                commit_length=commit_length,
                                leader_node_id=leader_node_id,
                                log=log,
                                role=Role(raw_node_role),
                                supported_node_id=supported_node_id,
                                term=term)

    def start(self) -> None:
        self._process = multiprocessing.Process(
                target=_run_node,
                args=(self.url, self.processors, self.heartbeat))
        self._process.start()

    def stop(self) -> None:
        self._session.close()
        assert self._process is not None
        while self._process.is_alive():
            self._process.terminate()
            time.sleep(0.5)
        assert not is_url_reachable(self.url)

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


def is_url_reachable(url: URL) -> bool:
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.settimeout(2)
    with connection:
        try:
            connection.connect((url.host, url.port))
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
              heartbeat: float) -> None:
    node = Node.from_url(url,
                         heartbeat=heartbeat,
                         logger=to_logger(url.authority),
                         processors=processors)
    return node.run()
