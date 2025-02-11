from __future__ import annotations

import itertools
import logging
import warnings
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Iterable, Literal, Mapping, Protocol, Sequence

from nominal_api import (
    datasource_api,
    scout_datasource,
    scout_datasource_connection,
    scout_datasource_connection_api,
    storage_writer_api,
    timeseries_logicalseries,
    timeseries_logicalseries_api,
)

from nominal.core._clientsbunch import HasAuthHeader, ProtoWriteService
from nominal.core._utils import HasRid
from nominal.core.batch_processor import process_batch_legacy
from nominal.core.channel import Channel
from nominal.core.stream import WriteStream


@dataclass(frozen=True)
class Connection(HasRid):
    rid: str
    name: str
    description: str | None
    _tags: Mapping[str, Sequence[str]]
    _clients: _Clients = field(repr=False)
    _nominal_data_source_rid: str | None = None

    class _Clients(Channel._Clients, HasAuthHeader, Protocol):
        @property
        def connection(self) -> scout_datasource_connection.ConnectionService: ...
        @property
        def datasource(self) -> scout_datasource.DataSourceService: ...
        @property
        def logical_series(self) -> timeseries_logicalseries.LogicalSeriesService: ...
        @property
        def proto_write(self) -> ProtoWriteService: ...
        @property
        def storage_writer(self) -> storage_writer_api.NominalChannelWriterService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, response: scout_datasource_connection_api.Connection) -> Connection:
        return cls(
            rid=response.rid,
            name=response.display_name,
            description=response.description,
            _tags=response.available_tag_values,
            _clients=clients,
            _nominal_data_source_rid=response.connection_details.nominal.nominal_data_source_rid
            if response.connection_details.nominal is not None
            else None,
        )

    def _get_series_achetypes_paginated(self) -> Iterable[datasource_api.ChannelMetadata]:
        request = datasource_api.SearchChannelsRequest(
            data_sources=[self.rid], exact_match=[], fuzzy_search_text="", previously_selected_channels={}
        )
        while True:
            response = self._clients.datasource.search_channels(self._clients.auth_header, request)
            for channel_metadata in response.results:
                if channel_metadata.series_rid.type == "seriesArchetype":
                    yield channel_metadata
                else:
                    logging.debug("ignoring channel with series_rid type %s", channel_metadata.series_rid.type)
            if response.next_page_token is None:
                break
            request = datasource_api.SearchChannelsRequest(
                data_sources=[self.rid],
                exact_match=[],
                fuzzy_search_text="",
                previously_selected_channels={},
                next_page_token=response.next_page_token,
            )

    def _resolve_archetypes_to_channels(self, channel_names: Sequence[str]) -> Iterable[Channel]:
        """Given archetype names ("shirts", "sweaters") and existing tag values
        ({color: [red, green], size: [S, M, L]}), resolve all possible combinations of archetype names and tag values
        to logical series. We will try to resolve the following logical series:
            * shirts, {color: red, size: S}
            * shirts, {color: red, size: M}
            * shirts, {color: red, size: L}
            * shirts, {color: green, size: S}
            * shirts, {color: green, size: M}
            * shirts, {color: green, size: L}
            * sweaters, {color: red, size: S}
            * sweaters, {color: red, size: M}
            * sweaters, {color: red, size: L}
            * sweaters, {color: green, size: S}
            * sweaters, {color: green, size: M}
            * sweaters, {color: green, size: L}
        """
        req = timeseries_logicalseries_api.BatchResolveSeriesRequest(
            requests=[
                timeseries_logicalseries_api.ResolveSeriesRequest(datasource=self.rid, name=name, tags=tags)
                for name in channel_names
                for tags in _tag_product(self._tags)
            ]
        )
        resp = self._clients.logical_series.resolve_batch(self._clients.auth_header, req)
        # TODO(alkasm): is there a batch get_logical_series ?
        for resolved_series in resp.series:
            if resolved_series.type == "error" and resolved_series.error is not None:
                raise RuntimeError(f"error resolving series: {resolved_series.error}")
            elif resolved_series.rid is None:
                raise RuntimeError(f"error resolving series for series {resolved_series}: no rid returned")
            series = self._clients.logical_series.get_logical_series(self._clients.auth_header, resolved_series.rid)
            yield Channel._from_conjure_logicalseries_api(self._clients, series)

    def _get_channels(self) -> Iterable[Channel]:
        """Retrieve all channels associated with this connection."""
        channel_names = [channel.name for channel in self._get_series_achetypes_paginated()]
        return self._resolve_archetypes_to_channels(channel_names)

    def get_channel(self, name: str, tags: dict[str, str] | None = None) -> Channel:
        """Retrieve a channel with the given name and tags."""
        req = timeseries_logicalseries_api.BatchResolveSeriesRequest(
            requests=[
                timeseries_logicalseries_api.ResolveSeriesRequest(
                    datasource=self.rid, name=name, tags={} if tags is None else tags
                )
            ]
        )
        resp = self._clients.logical_series.resolve_batch(self._clients.auth_header, req)
        if len(resp.series) == 0:
            raise ValueError("no channel found with name {name!r} and tags {tags!r}")
        elif len(resp.series) > 1:
            raise ValueError("multiple channels found with name {name!r} and tags {tags!r}")

        resolved_series = resp.series[0]
        if resolved_series.type == "error" and resolved_series.error is not None:
            raise RuntimeError(f"error resolving series: {resolved_series.error}")
        elif resolved_series.rid is None:
            raise RuntimeError(f"error resolving series for series {resolved_series}: no rid returned")
        series = self._clients.logical_series.get_logical_series(self._clients.auth_header, resolved_series.rid)
        return Channel._from_conjure_logicalseries_api(self._clients, series)

    def get_nominal_write_stream(self, batch_size: int = 50_000, max_wait_sec: int = 1) -> WriteStream:
        """get_nominal_write_stream is deprecated and will be removed in a future version,
        use get_write_stream instead.
        """
        warnings.warn(
            "get_nominal_write_stream is deprecated and will be removed in a future version,"
            "use get_write_stream instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.get_write_stream(batch_size, timedelta(seconds=max_wait_sec))

    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        data_format: Literal["json", "protobuf"] = "json",
    ) -> WriteStream:
        """Stream to write non-blocking messages to a datasource.

        Args:
        ----
            batch_size (int): How big the batch can get before writing to Nominal. Default 10
            max_wait (timedelta): How long a batch can exist before being flushed to Nominal. Default 5 seconds
            data_format (Literal["json", "protobuf"]): Send data as protobufs or as json. Default json

        Examples:
        --------
            Standard Usage:
            ```py
            with connection.get_write_stream() as stream:
                stream.enqueue("my_channel_name", "2021-01-01T00:00:00Z", 42.0)
                stream.enqueue("my_channel_name2", "2021-01-01T00:00:01Z", 43.0, {"tag1": "value1"})
                ...
            ```

            Without a context manager:
            ```py
            stream = connection.get_write_stream()
            stream.enqueue("my_channel_name", "2021-01-01T00:00:00Z", 42.0)
            stream.enqueue("my_channel_name2", "2021-01-01T00:00:01Z", 43.0, {"tag1": "value1"})
            ...
            stream.close()
            ```

        """
        if data_format == "json":
            return WriteStream.create(
                batch_size,
                max_wait,
                lambda batch: process_batch_legacy(
                    batch, self._nominal_data_source_rid, self._clients.auth_header, self._clients.storage_writer
                ),
            )

        try:
            from nominal.core.batch_processor_proto import process_batch
        except ImportError:
            raise ImportError("nominal-api-protos is required to use get_write_stream with use_protos=True")

        return WriteStream.create(
            batch_size,
            max_wait,
            lambda batch: process_batch(
                batch=batch,
                nominal_data_source_rid=self._nominal_data_source_rid,
                auth_header=self._clients.auth_header,
                proto_write=self._clients.proto_write,
            ),
        )

    def archive(self) -> None:
        """Archive this connection.
        Archived connections are not deleted, but are hidden from the UI.
        """
        self._clients.connection.archive_connection(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this connection, making it visible in the UI."""
        self._clients.connection.unarchive_connection(self._clients.auth_header, self.rid)


def _get_connections(
    clients: Connection._Clients, connection_rids: Sequence[str]
) -> Sequence[scout_datasource_connection_api.Connection]:
    return [clients.connection.get_connection(clients.auth_header, rid) for rid in connection_rids]


def _tag_product(tags: Mapping[str, Sequence[str]]) -> list[dict[str, str]]:
    # {color: [red, green], size: [S, M, L]} -> [{color: red, size: S}, {color: red, size: M}, ...,
    #                                            {color: green, size: L}]
    return [dict(zip(tags.keys(), values)) for values in itertools.product(*tags.values())]
