consensual
==========

[![](https://dev.azure.com/lycantropos/consensual/_apis/build/status/lycantropos.consensual?branchName=master)](https://dev.azure.com/lycantropos/consensual/_build/latest?definitionId=40&branchName=master "Azure Pipelines")
[![](https://codecov.io/gh/lycantropos/consensual/branch/master/graph/badge.svg)](https://codecov.io/gh/lycantropos/consensual "Codecov")
[![](https://img.shields.io/github/license/lycantropos/consensual.svg)](https://github.com/lycantropos/consensual/blob/master/LICENSE "License")
[![](https://badge.fury.io/py/consensual.svg)](https://badge.fury.io/py/consensual "PyPI")

Summary
-------

`consensual` is a pure-Python library for defining network of nodes
running in a consistent fault-tolerant manner by implementing state-of-the-art
[`Raft` consensus algorithm](https://raft.github.io/).

Currently, next features are implemented & property-based tested
- leader election & log replication
  (described in section 5 of [the article](https://raft.github.io/raft.pdf)),
- cluster membership changes
  (described in section 6 of [the article](https://raft.github.io/raft.pdf)),
  namely consensual addition & removal of nodes,
- "solo mode": non-consensual separation of a node
  resulting in running as a cluster-by-itself,
- separate node state resetting with history deletion.

Next crucial features to implement will be
- persistence,
- log compaction
  (described in section 7 of [the article](https://raft.github.io/raft.pdf)).

---

In what follows `python` is an alias for `python3.7`
or any later version (`python3.8` and so on).

Installation
------------

Install the latest `pip` & `setuptools` packages versions
```bash
python -m pip install --upgrade pip setuptools
```

### User

Download and install the latest stable version from `PyPI` repository
```bash
python -m pip install --upgrade consensual
```

### Developer

Download the latest version from `GitHub` repository
```bash
git clone https://github.com/lycantropos/consensual.git
cd consensual
```

Install dependencies
```bash
python -m pip install -r requirements.txt
```

Install
```bash
python setup.py install
```

Usage
-----

```python
>>> from consensual.raft import Node, communication
>>> from yarl import URL
>>> node_url = URL.build(scheme='http',
...                      host='localhost',
...                      port=6000)
>>> other_node_url = URL.build(scheme='http',
...                            host='localhost',
...                            port=6001)
>>> heartbeat = 0.1
>>> from typing import Any, List, Optional
>>> processed_parameters = []
>>> def dummy_processor(parameters: Any) -> None:
...     processed_parameters.append(parameters)
>>> processors = {'dummy': dummy_processor}
>>> nodes = {}
>>> sender = communication.Sender([node_url], nodes)
>>> other_sender = communication.Sender([other_node_url], nodes)
>>> node = Node.from_url(node_url,
...                      heartbeat=heartbeat,
...                      processors=processors,
...                      sender=sender)
>>> other_node = Node.from_url(other_node_url,
...                            heartbeat=heartbeat,
...                            processors=processors,
...                            sender=other_sender)
>>> receiver = communication.Receiver(node, nodes)
>>> other_receiver = communication.Receiver(other_node, nodes)
>>> receiver.start()
>>> other_receiver.start()
>>> from asyncio import get_event_loop
>>> loop = get_event_loop()
>>> async def run() -> List[Optional[str]]:
...     return [await node.solo(),
...             await node.enqueue('dummy', 42),
...             await node.attach_nodes([other_node.url]),
...             await node.enqueue('dummy', 42),
...             await other_node.detach_nodes([node.url]),
...             await other_node.solo(),
...             await other_node.detach(),
...             await other_node.detach()]
>>> error_messages = loop.run_until_complete(run())
>>> receiver.stop()
>>> other_receiver.stop()
>>> all(error_message is None or isinstance(error_message, str)
...     for error_message in error_messages)
True
>>> all(parameters == 42 for parameters in processed_parameters)
True

```

We can also replace builtin `consensual.raft.communication` communication layer
with another one (like [`consensual_http`](https://pypi.org/project/consensual-http/)
which is built on top of HTTP), usage patterns may change as a result.

Development
-----------

### Bumping version

#### Preparation

Install
[bump2version](https://github.com/c4urself/bump2version#installation).

#### Pre-release

Choose which version number category to bump following [semver
specification](http://semver.org/).

Test bumping version
```bash
bump2version --dry-run --verbose $CATEGORY
```

where `$CATEGORY` is the target version number category name, possible
values are `patch`/`minor`/`major`.

Bump version
```bash
bump2version --verbose $CATEGORY
```

This will set version to `major.minor.patch-alpha`. 

#### Release

Test bumping version
```bash
bump2version --dry-run --verbose release
```

Bump version
```bash
bump2version --verbose release
```

This will set version to `major.minor.patch`.

### Running tests

Install dependencies
```bash
python -m pip install -r requirements-tests.txt
```

Plain
```bash
pytest
```

Inside `Docker` container:
```bash
docker-compose --file docker-compose.yml up
```

`Bash` script:
```bash
./run-tests.sh
```

`PowerShell` script:
```powershell
.\run-tests.ps1
```
