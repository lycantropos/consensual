import logging.config
import random
import socket
import sys
import time
from typing import Any

import click
from yarl import URL

from .raft import Node


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
@click.option('--host',
              default=socket.gethostbyname(socket.gethostname()),
              type=str)
@click.option('--port',
              required=True,
              type=click.IntRange(0))
def main(host: str, port: int) -> None:
    node_url = URL.build(scheme='http',
                         host=host,
                         port=port)
    Node.from_url(node_url,
                  logger=to_logger(node_url.authority),
                  processors={'/log': process_log}).run()


def process_log(node: Node, parameters: Any) -> None:
    node.logger.debug(f'{node.url} processes {parameters}')
    delay = random.uniform(0, 10)
    time.sleep(delay)
    node.logger.debug(f'{node.url} finished processing {parameters} '
                      f'after {delay}s')


if __name__ == '__main__':
    main()
