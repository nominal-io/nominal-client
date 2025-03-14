from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Protocol

from nominal_api import datasource, datasource_logset, datasource_logset_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid
from nominal.ts import IntegralNanosecondsUTC, LogTimestampType, _SecondsNanos


@dataclass(frozen=True)
class LogSet(HasRid):
    _rid: str
    _name: str
    _timestamp_type: LogTimestampType
    _description: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def logset(self) -> datasource_logset.LogSetService: ...

    @property
    def rid(self) -> str:
        return self._rid

    @property
    def name(self) -> str:
        return self._name

    @property
    def timestamp_type(self) -> LogTimestampType:
        return self._timestamp_type

    @property
    def description(self) -> str | None:
        return self._description

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
            _rid=log_set_metadata.rid,
            _name=log_set_metadata.name,
            _timestamp_type=_log_timestamp_type_from_conjure(log_set_metadata.timestamp_type),
            _description=log_set_metadata.description,
            _clients=clients,
        )


@dataclass(frozen=True)
class Log:
    _timestamp: IntegralNanosecondsUTC
    _body: str

    @property
    def timestamp(self) -> IntegralNanosecondsUTC:
        return self._timestamp

    @property
    def body(self) -> str:
        return self._body

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
        return cls(_timestamp=_SecondsNanos.from_api(log.time).to_nanoseconds(), _body=log.body.basic.message)


def _log_timestamp_type_from_conjure(log_timestamp_type: datasource.TimestampType) -> LogTimestampType:
    if log_timestamp_type == datasource.TimestampType.ABSOLUTE:
        return "absolute"
    elif log_timestamp_type == datasource.TimestampType.RELATIVE:
        return "relative"
    raise ValueError(f"unhandled timestamp type {log_timestamp_type}")


def _get_log_set(clients: LogSet._Clients, log_set_rid: str) -> datasource_logset_api.LogSetMetadata:
    return clients.logset.get_log_set_metadata(clients.auth_header, log_set_rid)
