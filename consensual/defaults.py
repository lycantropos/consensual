import logging.config
import random
import sys
import time
from typing import Any

from yarl import URL

from consensual.raft import Node


def process_log(node: Node, parameters: Any) -> None:
    node.logger.debug(f'{node.url} processes {parameters}')
    delay = random.uniform(5, 10)
    time.sleep(delay)
    node.logger.debug(f'{node.url} finished processing {parameters} '
                      f'after {delay}s')


processors = {'/log': process_log}


def to_logger(url: URL,
              *,
              version: int = 1) -> logging.Logger:
    name = url.authority
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
