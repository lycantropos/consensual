import logging.config
import random
import sys
import time
from typing import (Any,
                    List)

import click
from yarl import URL

from .raft import (Node,
                   NodeId,
                   NodeState,
                   StableClusterState)


def to_logger(name: str,
              *,
              version: int = 1) -> logging.Logger:
    file_formatter = {'format': '[%(levelname)-8s %(asctime)s - %(name)s] '
                                '%(message)s'}
    console_formatter = {'format': '[%(levelname)-8s %(name)s] %(msg)s'}
    formatters = {'console': console_formatter,
                  'file': file_formatter}
    console_handler_config = {'class': 'logging.StreamHandler',
                              'level': logging.DEBUG,
                              'formatter': 'console',
                              'stream': sys.stdout}
    file_handler_config = {'class': 'logging.FileHandler',
                           'level': logging.DEBUG,
                           'formatter': 'file',
                           'filename': f'{name}.log',
                           'mode': 'w'}
    handlers = {'console': console_handler_config,
                'file': file_handler_config}
    loggers = {name: {'level': logging.DEBUG,
                      'handlers': ('console', 'file')}}
    config = {'formatters': formatters,
              'handlers': handlers,
              'loggers': loggers,
              'version': version}
    logging.config.dictConfig(config)
    return logging.getLogger(name)


@click.command()
@click.option('--index',
              required=True,
              type=int)
@click.argument('urls',
                required=True,
                type=URL,
                nargs=-1)
def main(index: int, urls: List[URL]) -> None:
    node_id = url_to_node_id(urls[index])
    nodes_urls = dict(zip(map(url_to_node_id, urls), urls))
    cluster_state = StableClusterState(nodes_urls=nodes_urls)
    Node(node_id, NodeState(cluster_state.nodes_ids), cluster_state,
         logger=to_logger(str(node_id)),
         processors={'/log': process_log}).run()


def process_log(node: Node, parameters: Any) -> None:
    node.logger.debug(f'{node.id} processes {parameters}')
    delay = random.uniform(0, 10)
    time.sleep(delay)
    node.logger.debug(f'{node.id} finished processing {parameters} '
                      f'after {delay}s')


def url_to_node_id(url: URL) -> NodeId:
    return url.authority


if __name__ == '__main__':
    main()
