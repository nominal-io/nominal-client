from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Iterable, Mapping

from nominal_api import api, datasource, scout_compute_api, storage_writer_api
from typing_extensions import Self

from nominal._utils import batched
from nominal.ts import IntegralNanosecondsUTC, LogTimestampType, _SecondsNanos

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
    def _from_compute_api(cls, point: scout_compute_api.LogValue, timestamp: api.Timestamp) -> Self:
        return cls(
            timestamp=_SecondsNanos.from_api(timestamp).to_nanoseconds(),
            message=point.message,
            args=point.args,
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
) -> scout_compute_api.LogFilterOperator:
    if regex_match and insensitive_match:
        raise ValueError("Only one of `regex_match` or `insensitive_match` may be provided")
    elif regex_match:
        return scout_compute_api.LogFilterOperator(
            regex_filter=scout_compute_api.LogRegexFilterOperator(regex=regex_match)
        )
    else:
        return scout_compute_api.LogFilterOperator(
            exact_match_case_insensitive_filter=scout_compute_api.LogExactMatchCaseInsensitiveFilter(
                token=insensitive_match or ""
            )
        )


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


def _log_timestamp_type_to_conjure(log_timestamp_type: LogTimestampType) -> datasource.TimestampType:
    if log_timestamp_type == "absolute":
        return datasource.TimestampType.ABSOLUTE
    elif log_timestamp_type == "relative":
        return datasource.TimestampType.RELATIVE
    raise ValueError(f"timestamp type {log_timestamp_type} must be 'relative' or 'absolute'")


def _log_timestamp_type_from_conjure(log_timestamp_type: datasource.TimestampType) -> LogTimestampType:
    if log_timestamp_type == datasource.TimestampType.ABSOLUTE:
        return "absolute"
    elif log_timestamp_type == datasource.TimestampType.RELATIVE:
        return "relative"
    raise ValueError(f"unhandled timestamp type {log_timestamp_type}")
