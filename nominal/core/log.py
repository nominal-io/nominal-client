from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from typing_extensions import Self

from .._api.combined import datasource, datasource_logset, datasource_logset_api
from ..ts import IntegralNanosecondsUTC, LogTimestampType, _SecondsNanos
from ._clientsbunch import ClientsBunch
from ._utils import HasRid


@dataclass(frozen=True)
class LogSet(HasRid):
    rid: str
    name: str
    timestamp_type: LogTimestampType
    description: str | None
    _clients: ClientsBunch = field(repr=False)

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
    def _from_conjure(cls, clients: ClientsBunch, log_set_metadata: datasource_logset_api.LogSetMetadata) -> Self:
        return cls(
            rid=log_set_metadata.rid,
            name=log_set_metadata.name,
            timestamp_type=_log_timestamp_type_from_conjure(log_set_metadata.timestamp_type),
            description=log_set_metadata.description,
            _clients=clients,
        )


@dataclass(frozen=True)
class Log:
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


def _get_log_set(
    auth_header: str, client: datasource_logset.LogSetService, log_set_rid: str
) -> datasource_logset_api.LogSetMetadata:
    return client.get_log_set_metadata(auth_header, log_set_rid)
