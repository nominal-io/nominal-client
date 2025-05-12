from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol

from nominal_api import datasource, datasource_logset, datasource_logset_api, storage_writer_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid, batched
from nominal.ts import IntegralNanosecondsUTC, LogTimestampType, _SecondsNanos

_EMTPY_MAP: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True)
class LogPoint:
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

    def _to_conjure(self) -> storage_writer_api.LogPoint:
        return storage_writer_api.LogPoint(
            timestamp=_SecondsNanos.from_nanoseconds(self.timestamp).to_api(),
            value=storage_writer_api.LogValue(
                message=self.message,
                args=dict(self.args),
            ),
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


def _log_timestamp_type_from_conjure(log_timestamp_type: datasource.TimestampType) -> LogTimestampType:
    if log_timestamp_type == datasource.TimestampType.ABSOLUTE:
        return "absolute"
    elif log_timestamp_type == datasource.TimestampType.RELATIVE:
        return "relative"
    raise ValueError(f"unhandled timestamp type {log_timestamp_type}")


def _get_log_set(clients: LogSet._Clients, log_set_rid: str) -> datasource_logset_api.LogSetMetadata:
    return clients.logset.get_log_set_metadata(clients.auth_header, log_set_rid)
