from __future__ import annotations

import itertools
import logging
from types import TracebackType
import warnings
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from typing import Iterable, Literal, Mapping, Protocol, Sequence, Type
from queue import Queue, Empty
import threading
import time

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
from nominal.ts import IntegralNanosecondsUTC


@dataclass()
class Connection(HasRid):
    rid: str
    name: str
    description: str | None
    _tags: Mapping[str, Sequence[str]]
    _clients: _Clients = field(repr=False)

    class _Clients(Channel._Clients, HasAuthHeader, Protocol):
        @property
        def connection(self) -> scout_datasource_connection.ConnectionService: ...
        @property
        def datasource(self) -> scout_datasource.DataSourceService: ...
        @property
        def logical_series(self) -> timeseries_logicalseries.LogicalSeriesService: ...
        @property
        def storage_writer(self) -> storage_writer_api.NominalChannelWriterService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, response: scout_datasource_connection_api.Connection) -> Connection | NominalStreamingConnection:
        """Factory method to create the appropriate Connection subclass based on connection details"""
        if response.connection_details.nominal is not None:
            return NominalStreamingConnection(
                rid=response.rid,
                name=response.display_name,
                description=response.description,
                _tags=response.available_tag_values,
                _clients=clients,
                nominal_data_source_rid=response.connection_details.nominal.nominal_data_source_rid,
            )
        return cls(
            rid=response.rid,
            name=response.display_name,
            description=response.description,
            _tags=response.available_tag_values,
            _clients=clients,
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

    def archive(self) -> None:
        """Archive this connection.
        Archived connections are not deleted, but are hidden from the UI.
        """
        self._clients.connection.archive_connection(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this connection, making it visible in the UI."""
        self._clients.connection.unarchive_connection(self._clients.auth_header, self.rid)


@dataclass(frozen=True)
class BatchItem:
    channel_name: str
    timestamp: str | datetime | IntegralNanosecondsUTC
    value: float | str
    tags: dict[str, str] | None = None

@dataclass()
class NominalStreamingConnection(Connection):
    nominal_data_source_rid: str
    _batch_size: int = field(default=50_000)
    _max_wait: timedelta = field(default=timedelta(seconds=1))
    _item_queue: Queue[BatchItem] = field(default_factory=Queue, repr=False)
    _batch_queue: Queue[list[BatchItem]] = field(default_factory=Queue, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _batch_thread: threading.Thread | None = field(default=None, repr=False)
    _process_thread: threading.Thread | None = field(default=None, repr=False)

    def start_streaming(self, data_format: Literal["json", "protobuf"] = "json") -> None:
        """Start the streaming threads."""
        if self._batch_thread is not None or self._process_thread is not None:
            raise RuntimeError("Streaming already started")

        self._batch_thread = threading.Thread(target=self._batch_worker, daemon=True)
        self._process_thread = threading.Thread(target=self._process_worker, args=(data_format,), daemon=True)
        self._batch_thread.start()
        self._process_thread.start()

    def stop_streaming(self, wait: bool = True) -> None:
        """Stop the streaming threads."""
        self._stop.set()
        if wait and self._batch_thread and self._process_thread:
            self._item_queue.join()
            self._batch_queue.join()
            self._batch_thread.join()
            self._process_thread.join()
            self._batch_thread = None
            self._process_thread = None
            self._stop.clear()

    def _batch_worker(self) -> None:
        """Worker that creates batches from individual items."""
        batch: list[BatchItem] = []
        next_batch_time = time.time() + self._max_wait.total_seconds()

        while not self._stop.is_set():
            now = time.time()
            timeout = max(0, next_batch_time - now)
            
            try:
                item = self._item_queue.get(timeout=timeout)
                batch.append(item)
                self._item_queue.task_done()
            except Empty:
                pass

            if len(batch) >= self._batch_size or time.time() >= next_batch_time:
                if batch:
                    self._batch_queue.put(batch)
                    batch = []
                next_batch_time = time.time() + self._max_wait.total_seconds()

        # Flush remaining items
        if batch:
            self._batch_queue.put(batch)

    def _process_worker(self, data_format: Literal["json", "protobuf"]) -> None:
        """Worker that processes batches."""
        while not self._stop.is_set():
            try:
                batch = self._batch_queue.get(timeout=0.1)
                try:
                    if data_format == "json":
                        process_batch_legacy(
                            batch, 
                            self.nominal_data_source_rid, 
                            self._clients.auth_header, 
                            self._clients.storage_writer
                        )
                    else:
                        from nominal.core.batch_processor_proto import process_batch
                        process_batch(
                            batch=batch,
                            nominal_data_source_rid=self.nominal_data_source_rid,
                            auth_header=self._clients.auth_header,
                            proto_write=self._clients.proto_write,
                        )
                except Exception as e:
                    raise Exception(f"Batch processing failed: {e}")
                finally:
                    self._batch_queue.task_done()
            except Empty:
                continue

    def write(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a single value to a channel."""
        if self._batch_thread is None:
            raise RuntimeError("Streaming not started. Call connection_obj.start_streaming() first")
        item = BatchItem(channel_name, timestamp, value, tags)
        self._item_queue.put(item)

    def write_batch(
        self,
        channel_name: str,
        timestamps: Sequence[str | datetime | IntegralNanosecondsUTC],
        values: Sequence[float | str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write multiple values to a channel."""
        if len(timestamps) != len(values):
            raise ValueError(
                f"Expected equal numbers of timestamps and values! Received: {len(timestamps)} vs. {len(values)}"
            )
        for timestamp, value in zip(timestamps, values):
            self.write(channel_name, timestamp, value, tags)

    def __enter__(self) -> NominalStreamingConnection:
        self.start_streaming()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop_streaming()

    # Deprecated methods for backward compatibility
    def get_nominal_write_stream(self, batch_size: int = 50_000, max_wait_sec: int = 1) -> WriteStream:
        warnings.warn(
            "get_nominal_write_stream is deprecated and will be removed in a future version. "
            "Use start_streaming() and write() methods instead.",
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
        warnings.warn(
            "get_write_stream is deprecated and will be removed in a future version. "
            "Use start_streaming() and write() methods instead.",
            UserWarning,
            stacklevel=2,
        )
        # Return legacy WriteStream for backward compatibility
        return WriteStream.create(batch_size, max_wait, lambda batch: self._process_batch(batch))


def _get_connections(
    clients: Connection._Clients, connection_rids: Sequence[str]
) -> Sequence[scout_datasource_connection_api.Connection]:
    return [clients.connection.get_connection(clients.auth_header, rid) for rid in connection_rids]


def _tag_product(tags: Mapping[str, Sequence[str]]) -> list[dict[str, str]]:
    # {color: [red, green], size: [S, M, L]} -> [{color: red, size: S}, {color: red, size: M}, ...,
    #                                            {color: green, size: L}]
    return [dict(zip(tags.keys(), values)) for values in itertools.product(*tags.values())]
