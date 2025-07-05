from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol

from nominal_api import api, datasource, datasource_logset, datasource_logset_api, scout_compute_api, storage_writer_api
from typing_extensions import Self

from nominal._utils import batched
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid
from nominal.ts import IntegralNanosecondsUTC, LogTimestampType, _SecondsNanos

_EMTPY_MAP: Mapping[str, str] = MappingProxyType({})


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
            args=_EMTPY_MAP if args is None else MappingProxyType(args),
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


@dataclass(frozen=True)
class LogSet(HasRid):
    """LogSet is a collection of logs. LogSets are deprecated."""

    rid: str
    name: str
    timestamp_type: LogTimestampType
    description: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def logset(self) -> datasource_logset.LogSetService: ...

    def _stream_logs_paginated(self) -> Iterable[datasource_logset_api.Log]:
        request = datasource_logset_api.SearchLogsRequest()
        while True:
            response = self._clients.logset.search_logs(
                self._clients.auth_header,
                log_set_rid=self.rid,
                request=request,
            )
            yield from response.logs
            if response.next_page_token is None:
                break
            request = datasource_logset_api.SearchLogsRequest(token=response.next_page_token)

    def stream_logs(self) -> Iterable[Log]:
        """Iterate over the logs."""
        for log in self._stream_logs_paginated():
            yield Log._from_conjure(log)

    @classmethod
    def _from_conjure(cls, clients: _Clients, log_set_metadata: datasource_logset_api.LogSetMetadata) -> Self:
        return cls(
            rid=log_set_metadata.rid,
            name=log_set_metadata.name,
            timestamp_type=_log_timestamp_type_from_conjure(log_set_metadata.timestamp_type),
            description=log_set_metadata.description,
            _clients=clients,
        )


@dataclass(frozen=True)
class Log:
    """A single log in a LogSet. LogSets are deprecated."""

    timestamp: IntegralNanosecondsUTC
    body: str

    def _to_conjure(self) -> datasource_logset_api.Log:
        return datasource_logset_api.Log(
            time=_SecondsNanos.from_nanoseconds(self.timestamp).to_api(),
            body=datasource_logset_api.LogBody(
                basic=datasource_logset_api.BasicLogBody(message=self.body, properties={}),
            ),
        )

    @classmethod
    def _from_conjure(cls, log: datasource_logset_api.Log) -> Self:
        if log.body.basic is None:
            raise RuntimeError(f"unhandled log body type: expected 'basic' but got {log.body.type!r}")
        return cls(timestamp=_SecondsNanos.from_api(log.time).to_nanoseconds(), body=log.body.basic.message)


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


def _get_log_set(clients: LogSet._Clients, log_set_rid: str) -> datasource_logset_api.LogSetMetadata:
    return clients.logset.get_log_set_metadata(clients.auth_header, log_set_rid)


def _logs_to_conjure(
    logs: Iterable[Log] | Iterable[tuple[datetime | IntegralNanosecondsUTC, str]],
) -> Iterable[datasource_logset_api.Log]:
    for log in logs:
        if isinstance(log, Log):
            yield log._to_conjure()
        elif isinstance(log, tuple):
            ts, body = log
            yield Log(timestamp=_SecondsNanos.from_flexible(ts).to_nanoseconds(), body=body)._to_conjure()
