from typing import (Collection,
                    Dict,
                    Iterable,
                    MutableSequence,
                    Union)

from reprit.base import generate_repr

from .hints import (NodeId,
                    Term)
from .record import Record


class RegularHistory:
    __slots__ = '_log',

    def __new__(cls, log: MutableSequence[Record]) -> 'RegularHistory':
        self = super().__new__(cls)
        self._log = log
        return self

    __repr__ = generate_repr(__new__)

    @property
    def log(self) -> MutableSequence[Record]:
        return self._log

    def to_regular(self) -> 'RegularHistory':
        return self

    def with_nodes_ids(self,
                       nodes_ids: Collection[NodeId]) -> 'RegularHistory':
        return self


class SyncHistory:
    __slots__ = '_accepted_lengths', '_log', '_sent_lengths'

    def __new__(cls,
                log: MutableSequence[Record],
                *,
                accepted_lengths: Dict[NodeId, int],
                sent_lengths: Dict[NodeId, int]) -> 'SyncHistory':
        self = super().__new__(cls)
        self._accepted_lengths, self._log, self._sent_lengths = (
            accepted_lengths, log, sent_lengths
        )
        return self

    __repr__ = generate_repr(__new__)

    @classmethod
    def from_nodes_ids(cls,
                       log: MutableSequence[Record],
                       nodes_ids: Collection[NodeId]) -> 'SyncHistory':
        return cls(log,
                   accepted_lengths={node_id: 0 for node_id in nodes_ids},
                   sent_lengths={node_id: len(log) for node_id in nodes_ids})

    @property
    def accepted_lengths(self) -> Dict[NodeId, int]:
        return self._accepted_lengths

    @property
    def sent_lengths(self) -> Dict[NodeId, int]:
        return self._sent_lengths

    @property
    def log(self) -> MutableSequence[Record]:
        return self._log

    def to_regular(self) -> 'RegularHistory':
        return RegularHistory(self.log)

    def with_nodes_ids(self, nodes_ids: Collection[NodeId]) -> 'SyncHistory':
        accepted_lengths = {node_id: self.accepted_lengths.get(node_id, 0)
                            for node_id in nodes_ids}
        sent_lengths = {node_id: self.sent_lengths.get(node_id, len(self.log))
                        for node_id in nodes_ids}
        return SyncHistory(self.log,
                           accepted_lengths=accepted_lengths,
                           sent_lengths=sent_lengths)


History = Union[RegularHistory, SyncHistory]


def append_record(history: History, record: Record) -> None:
    history.log.append(record)


def append_records(history: History, records: Iterable[Record]) -> None:
    history.log.extend(records)


def to_nodes_ids_that_accepted_more_records(history: SyncHistory,
                                            commit_length: int
                                            ) -> Collection[NodeId]:
    return [node_id
            for node_id, length in history.accepted_lengths.items()
            if length > commit_length]


def to_log_term(history: History) -> Term:
    return history.log[-1].term if history.log else 0
