import importlib.util
import socket
from logging import Logger
from types import ModuleType
from typing import (Callable,
                    Mapping,
                    Type)

import click
from yarl import URL

import consensual.raft
from consensual import defaults
from consensual.raft import (Node,
                             Processor)

LoggerFactory = Callable[[URL], Logger]


def _class_to_full_name(cls: Type) -> str:
    return f'{cls.__module__}.{cls.__qualname__}'


@click.command()
@click.option('--host',
              default=socket.gethostbyname(socket.gethostname()),
              type=str)
@click.option('--port',
              required=True,
              type=click.IntRange(0))
@click.option('--processors-path',
              default=f'{defaults.__file__}:processors',
              type=str,
              help='Path to the processors '
                   '(mapping from path to a function accepting '
                   f'`{consensual.raft.__name__}.{Node.__qualname__}` '
                   'instance with parameters from user request body '
                   'and returning nothing) '
                   'in a format `path/to/module.py:processors_name`.')
@click.option('--logger-factory-path',
              default=f'{defaults.__file__}:to_logger',
              type=str,
              help='Path to the logger factory '
                   '(function '
                   f'accepting `{_class_to_full_name(URL)}` instance '
                   f'and returning `{_class_to_full_name(Logger)}` instance) '
                   'in a format `path/to/module.py:logger_factory_name`.')
def main(host: str,
         port: int,
         processors_path: str,
         logger_factory_path: str) -> None:
    node_url = URL.build(scheme='http',
                         host=host,
                         port=port)
    processors_module_path, processors_name = processors_path.rsplit(':', 1)
    logger_factory_module_path, logger_factory_name = (
        logger_factory_path.rsplit(':', 1)
    )
    processors_module = _load_module_from_path(f'{consensual}._processing',
                                               processors_module_path)
    logger_factory_module = _load_module_from_path(f'{consensual}._logging',
                                                   logger_factory_module_path)
    logger_factory: LoggerFactory = getattr(logger_factory_module,
                                            logger_factory_name)
    processors: Mapping[str, Processor] = getattr(processors_module,
                                                  processors_name)
    Node.from_url(node_url,
                  logger=logger_factory(node_url),
                  processors=processors).run()


def _load_module_from_path(name: str, path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


if __name__ == '__main__':
    main()
