from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Iterable, Mapping, Self

from nominal_api import storage_writer_api

from nominal._utils import batched
from nominal.protos.compute.v1 import log_predicates_pb2, result_common_pb2
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

_EMPTY_MAP: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True)
class LogPoint:
    """LogPoint is a single, timestamped log entry.

    LogPoints are added to a Dataset using `Dataset.write_logs`.

    """

    timestamp: IntegralNanosecondsUTC
    message: str
    args: Mapping[str, str]

    @classmethod
    def create(
        cls, timestamp: str | datetime | IntegralNanosecondsUTC, message: str, args: Mapping[str, str] | None
    ) -> Self:
        return cls(
            timestamp=_SecondsNanos.from_flexible(timestamp).to_nanoseconds(),
            message=message,
            args=_EMPTY_MAP if args is None else MappingProxyType(args),
        )

    @classmethod
    def _from_conjure(cls, point: storage_writer_api.LogPoint) -> Self:
        return cls(
            timestamp=_SecondsNanos.from_api(point.timestamp).to_nanoseconds(),
            message=point.value.message,
            args=MappingProxyType(point.value.args),
        )

    @classmethod
    def _from_proto(cls, point: result_common_pb2.LogSample) -> Self:
        return cls(
            timestamp=_SecondsNanos.from_proto(point.timestamp).to_nanoseconds(),
            message=point.message,
            args=dict(point.attributes),
        )

    def _to_conjure(self) -> storage_writer_api.LogPoint:
        return storage_writer_api.LogPoint(
            timestamp=_SecondsNanos.from_nanoseconds(self.timestamp).to_api(),
            value=storage_writer_api.LogValue(
                message=self.message,
                args=dict(self.args),
            ),
        )


def _log_filter_operator(
    regex_match: str | None = None, insensitive_match: str | None = None
) -> log_predicates_pb2.LogPredicate:
    if regex_match and insensitive_match:
        raise ValueError("Only one of `regex_match` or `insensitive_match` may be provided")
    if regex_match:
        return log_predicates_pb2.LogPredicate(message_regex=regex_match)
    return log_predicates_pb2.LogPredicate(message_contains_case_insensitive=insensitive_match or "")


def _write_logs(
    auth_header: str,
    client: storage_writer_api.NominalChannelWriterService,
    data_source_rid: str,
    logs: Iterable[LogPoint],
    channel_name: str,
    batch_size: int,
) -> None:
    for batch in batched(logs, batch_size):
        request = storage_writer_api.WriteLogsRequest(
            logs=[log._to_conjure() for log in batch],
            channel=channel_name,
        )
        client.write_logs(auth_header, data_source_rid, request)
