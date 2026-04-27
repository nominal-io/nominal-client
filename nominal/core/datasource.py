from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Iterable, Literal, Mapping, Protocol, Sequence, overload

from nominal_api import (
    api,
    datasource_api,
    ingest_api,
    scout,
    scout_catalog,
    scout_compute_api,
    scout_dataexport_api,
    scout_datasource,
    scout_datasource_connection,
    storage_writer_api,
    timeseries_channelmetadata,
    timeseries_channelmetadata_api,
    timeseries_metadata,
    timeseries_metadata_api,
    upload_api,
)

from nominal._utils import batched
from nominal._utils.deprecation_tools import warn_on_deprecated_argument
from nominal.core._clientsbunch import HasScoutParams, ProtoWriteService
from nominal.core._stream.batch_processor import process_batch_legacy
from nominal.core._stream.write_stream import DataStream, WriteStream
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.unit import UnitMapping, _build_unit_update, _error_on_invalid_units
from nominal.ts import (
    _AnyExportableTimestampType,
    _to_export_timestamp_format,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHANNEL_BATCH_SIZE = 500


@dataclass(frozen=True)
class CreateChannelRequest:
    name: str
    data_type: ChannelDataType
    description: str | None = None
    unit: str | None = None


@dataclass(frozen=True)
class BatchAddChannelsResult:
    channels: Sequence[Channel]
    missing: Sequence[CreateChannelRequest]


@dataclass(frozen=True)
class DataSource(HasRid):
    rid: str
    _clients: _Clients = field(repr=False)

    class _Clients(Channel._Clients, HasScoutParams, Protocol):
        @property
        def catalog(self) -> scout_catalog.CatalogService: ...
        @property
        def dataexport(self) -> scout_dataexport_api.DataExportService: ...
        @property
        def datasource(self) -> scout_datasource.DataSourceService: ...
        @property
        def units(self) -> scout.UnitsService: ...
        @property
        def ingest(self) -> ingest_api.IngestService: ...
        @property
        def upload(self) -> upload_api.UploadService: ...
        @property
        def connection(self) -> scout_datasource_connection.ConnectionService: ...
        @property
        def storage_writer(self) -> storage_writer_api.NominalChannelWriterService: ...
        @property
        def proto_write(self) -> ProtoWriteService: ...
        @property
        def channel_metadata(self) -> timeseries_channelmetadata.ChannelMetadataService: ...
        @property
        def series_metadata(self) -> timeseries_metadata.SeriesMetadataService: ...
        @property
        def containerized_extractors(self) -> ingest_api.ContainerizedExtractorService: ...

    def get_channel(self, name: str) -> Channel:
        for channel in self.get_channels(names=[name]):
            if channel.name == name:
                return channel
        raise ValueError(f"channel {name!r} not found in dataset {self.rid!r}")

    def get_channels(
        self,
        *,
        names: Iterable[str] | None = None,
    ) -> Iterable[Channel]:
        """Look up the metadata for all matching channels associated with this datasource

        Args:
        ----
            names: List of channel names to look up metadata for.

        Yields:
        ------
            Yields a sequence of channel metadata objects which match the provided query parameters

        """
        if not names:
            names = [channel.name for channel in self.search_channels()]

        for batch_channel_names in batched(names, _DEFAULT_CHANNEL_BATCH_SIZE):
            requests = [
                timeseries_channelmetadata_api.GetChannelMetadataRequest(
                    channel_identifier=timeseries_channelmetadata_api.ChannelIdentifier(
                        channel_name=channel_name, data_source_rid=self.rid
                    )
                )
                for channel_name in batch_channel_names
            ]

            batch_request = timeseries_channelmetadata_api.BatchGetChannelMetadataRequest(requests=requests)
            response = self._clients.channel_metadata.batch_get_channel_metadata(
                self._clients.auth_header, batch_request
            )
            yield from (Channel._from_channel_metadata_api(self._clients, channel) for channel in response.responses)

    @overload
    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        data_format: Literal["json", "protobuf", "experimental"] | None = None,
    ) -> DataStream: ...
    @overload
    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        data_format: Literal["rust_experimental"] | None = None,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
    ) -> DataStream: ...
    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        data_format: Literal["json", "protobuf", "experimental", "rust_experimental"] | None = None,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
    ) -> DataStream:
        """Stream to write timeseries data to a datasource.

        Data is written asynchronously.

        Args:
        ----
            batch_size: How big the batch can get before writing to Nominal.
            max_wait: How long a batch can exist before being flushed to Nominal.
            data_format: Serialized data format to use during upload.
                NOTE: selecting 'protobuf' or 'experimental' requires that `nominal` was installed
                      with `protos` extras.
            file_fallback: Filepath to write failed batches to during streaming
                NOTE: expects a .avro filename
                NOTE: only works with `data_format='rust_experimental'`
            log_level: Log level to use in underlying rust streaming code.
                NOTE: Should be a rust log level e.g. 'debug', 'trace', 'info', etc.
                NOTE: only works with `data_format='rust_experimental'`
            num_workers: Number of worker threads to use in underlying rust streaming code.
                NOTE: use with care-- this may have large impacts on streaming performance.
                NOTE: only works with `data_format='rust_experimental'`

        Returns:
        --------
            Write stream object configured to send data to nominal. This may be used as a context manager
            (so that resources are automatically released upon exiting the context), or if not used as a context
            manager, should be explicitly `close()`-ed once no longer needed.
        """
        return _get_write_stream(
            batch_size=batch_size,
            max_wait=max_wait,
            data_format=data_format,
            file_fallback=file_fallback,
            log_level=log_level,
            num_workers=num_workers,
            write_rid=self.rid,
            clients=self._clients,
        )

    @warn_on_deprecated_argument(
        "exact_match",
        "'exact_match' is deprecated and will be removed in a future version of Nominal. "
        "Use 'substring_matches' instead.",
    )
    def search_channels(
        self,
        substring_matches: Sequence[str] | None = None,
        fuzzy_search_text: str = "",
        *,
        exact_match: Sequence[str] | None = None,
        data_types: Sequence[ChannelDataType] | None = None,
    ) -> Iterable[Channel]:
        """Look up channels associated with a datasource.

        Args:
            substring_matches: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
            exact_match: Deprecated. Use ``substring_matches`` instead.
            fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.
            data_types: Filter the returned channels to those that match any of the provided types

        Yields:
            Channel objects for each matching channel
        """
        effective_substring_matches = substring_matches if substring_matches is not None else exact_match
        if isinstance(effective_substring_matches, str):
            argument_name = "substring_matches" if substring_matches is not None else "exact_match"
            raise TypeError(f"{argument_name} must be a sequence of strings, not a single string.")
        effective_substring_matches = tuple(effective_substring_matches or ())
        allowable_types = set(data_types) if data_types else None
        next_page_token = None
        while True:
            query = datasource_api.SearchChannelsRequest(
                data_sources=[self.rid],
                exact_match=list(effective_substring_matches),
                fuzzy_search_text=fuzzy_search_text,
                previously_selected_channels={},
                next_page_token=next_page_token,
                data_types=[],
                page_size=None,
                prefix=None,
            )
            response = self._clients.datasource.search_channels(self._clients.auth_header, query)
            for channel_metadata in response.results:
                # If user provided a set of datatypes to filter on, ensure the channel has the right type
                # TODO (drake): move this into the backend
                if allowable_types is not None:
                    data_type = (
                        ChannelDataType._from_conjure(channel_metadata.data_type)
                        if channel_metadata.data_type
                        else None
                    )
                    if data_type not in allowable_types:
                        continue

                channel = Channel._from_conjure_datasource_api(self._clients, channel_metadata)
                if not all(match.casefold() in channel.name.casefold() for match in effective_substring_matches):
                    continue

                yield channel
            if response.next_page_token is None:
                break
            next_page_token = response.next_page_token

    def set_channel_units(
        self,
        channels_to_units: UnitMapping,
        validate_schema: bool = False,
        allow_display_only_units: bool = False,
    ) -> None:
        """Set units for channels based on a provided mapping of channel names to units.

        Args:
        ----
            channels_to_units: A mapping of channel names to unit symbols.
                NOTE: any existing units may be cleared from a channel by providing None as a symbol.
            validate_schema: If true, raises a ValueError if non-existent channel names are provided in
                `channels_to_units`. Default is False.
            allow_display_only_units: If true, allow units that would be treated as display-only by Nominal.

        Raises:
        ------
            ValueError: Unsupported unit symbol provided
            conjure_python_client.ConjureHTTPError: Error completing requests.
        """
        channel_names = set(channel.name for channel in self.get_channels())

        if validate_schema:
            for channel in channels_to_units:
                if channel not in channel_names:
                    raise ValueError(f"Cannot update units for channel {channel}-- no such channel exists!")
        else:
            channels_to_units = {
                channel: unit for channel, unit in channels_to_units.items() if channel in channel_names
            }

        if not channels_to_units:
            logger.warning("No channels specified to have updated units, nothing to update.")
            return

        if not allow_display_only_units:
            _error_on_invalid_units(channels_to_units, self._clients.units, self._clients.auth_header)

        # For each channel / unit combination, create an update request
        update_requests = [
            timeseries_channelmetadata_api.UpdateChannelMetadataRequest(
                channel_identifier=timeseries_channelmetadata_api.ChannelIdentifier(
                    channel_name=channel_name, data_source_rid=self.rid
                ),
                unit_update=_build_unit_update(unit),
            )
            for channel_name, unit in channels_to_units.items()
        ]

        # Set units in database using batch update
        batch_request = timeseries_channelmetadata_api.BatchUpdateChannelMetadataRequest(requests=update_requests)
        self._clients.channel_metadata.batch_update_channel_metadata(self._clients.auth_header, batch_request)

    def set_channel_prefix_tree(self, delimiter: str = ".") -> None:
        """Index channels hierarchically by a given delimiter.

        Primarily, the result of this operation is to prompt the frontend to represent channels
        in a tree-like manner that allows folding channels by common roots.
        """
        request = datasource_api.IndexChannelPrefixTreeRequest(self.rid, delimiter=delimiter)
        self._clients.datasource.index_channel_prefix_tree(self._clients.auth_header, request)

    def add_channel(
        self,
        name: str,
        data_type: ChannelDataType,
        *,
        description: str | None = None,
        unit: str | None = None,
    ) -> Channel:
        """Create a new channel (series metadata) for this data source.

        This creates the channel metadata entry without uploading any data points.
        Use this to pre-register channels before streaming data to them.

        Args:
            name: The name of the channel to create.
            data_type: The data type of the channel (e.g., DOUBLE, STRING, LOG, INT).
            description: Optional human-readable description of the channel.
            unit: Optional unit symbol to associate with the channel.

        Returns:
            The created Channel object.

        Raises:
            conjure_python_client.ConjureHTTPError: If a channel with this name already exists
                or if there's an error creating the channel.
        """
        request = _build_series_metadata_request(
            self.rid, CreateChannelRequest(name=name, data_type=data_type, description=description, unit=unit)
        )
        self._clients.series_metadata.create(self._clients.auth_header, request)
        return self.get_channel(name)

    def batch_add_channels(
        self,
        channels: Sequence[CreateChannelRequest],
    ) -> BatchAddChannelsResult:
        """Create multiple channels (series metadata) for this data source in batches.

        Args:
            channels: Sequence of CreateChannelRequest objects.

        Returns:
            A BatchAddChannelsResult with the created channels and any requests that were
            not found after creation (i.e. silently dropped by the server).

        Note:
            This operation is idempotent with respect to channels that already exist — they are
            silently skipped. However, if the same channel name appears more than once within a
            single batch, the entire batch will fail. Callers are responsible for deduplicating
            channel names before passing them to this method.
        """
        if not channels:
            return BatchAddChannelsResult(channels=[], missing=[])
        for batch in batched(channels, _DEFAULT_CHANNEL_BATCH_SIZE):
            requests = [_build_series_metadata_request(self.rid, req) for req in batch]
            batch_request = timeseries_metadata_api.BatchCreateSeriesMetadataRequest(requests=requests)
            self._clients.series_metadata.batch_create(self._clients.auth_header, batch_request)
        created = {ch.name: ch for ch in self.get_channels(names=[req.name for req in channels])}
        return BatchAddChannelsResult(
            channels=list(created.values()),
            missing=[req for req in channels if req.name not in created],
        )


def _build_series_metadata_request(
    data_source_rid: str, req: CreateChannelRequest
) -> timeseries_metadata_api.CreateSeriesMetadataRequest:
    nominal_data_type = req.data_type._to_nominal_data_type()
    locator = timeseries_metadata_api.LocatorTemplate(
        nominal=timeseries_metadata_api.NominalLocatorTemplate(
            channel=req.name,
            type=nominal_data_type,
        )
    )
    return timeseries_metadata_api.CreateSeriesMetadataRequest(
        channel=req.name,
        data_source_rid=data_source_rid,
        locator=locator,
        tags={},
        description=req.description,
        unit=req.unit,
    )


def _construct_export_request(
    channels: Sequence[Channel],
    start: api.Timestamp,
    end: api.Timestamp,
    tags: Mapping[str, str] | None,
    enable_gzip: bool,
    timestamp_type: _AnyExportableTimestampType = "iso_8601",
) -> scout_dataexport_api.ExportDataRequest:
    export_channels = [channel._to_time_domain_channel(tags=tags) for channel in channels]
    request = scout_dataexport_api.ExportDataRequest(
        channels=scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=export_channels,
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    # only one series will be returned, so no need to merge
                    none=scout_dataexport_api.NoneStrategy(),
                ),
                output_timestamp_format=_to_export_timestamp_format(timestamp_type),
            )
        ),
        start_time=start,
        end_time=end,
        context=scout_compute_api.Context(
            function_variables={},
            variables={},
        ),
        format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
        resolution=scout_dataexport_api.ResolutionOption(
            undecimated=scout_dataexport_api.UndecimatedResolution(),
        ),
        compression=scout_dataexport_api.CompressionFormat.GZIP if enable_gzip else None,
    )
    return request


def _get_write_stream(
    batch_size: int,
    max_wait: timedelta,
    data_format: Literal["json", "protobuf", "experimental", "rust_experimental"] | None,
    file_fallback: PathLike | None,
    log_level: str | None,
    num_workers: int | None,
    write_rid: str,
    clients: DataSource._Clients,
) -> DataStream:
    if data_format is None:
        data_format = "json"

    if data_format != "rust_experimental":
        new_kwargs = {
            "file_fallback": file_fallback,
            "log_level": log_level,
            "num_workers": num_workers,
        }
        for key, value in new_kwargs.items():
            if value is not None:
                logger.warning("Argument %s has no effect unless `data_format='rust_experimental'`", key)

    if data_format == "json":
        return WriteStream.create(
            batch_size=batch_size,
            max_wait=max_wait,
            process_batch=lambda batch: process_batch_legacy(
                batch=batch,
                nominal_data_source_rid=write_rid,
                auth_header=clients.auth_header,
                storage_writer=clients.storage_writer,
            ),
        )
    elif data_format == "protobuf":
        try:
            from nominal.core._stream.batch_processor_proto import process_batch
        except ImportError as ex:
            raise ImportError(
                "nominal-api-protos is required to use get_write_stream with data_format='protobuf'"
            ) from ex

        return WriteStream.create(
            batch_size,
            max_wait,
            lambda batch: process_batch(
                batch=batch,
                nominal_data_source_rid=write_rid,
                auth_header=clients.auth_header,
                proto_write=clients.proto_write,
            ),
        )
    elif data_format == "experimental":
        try:
            from nominal.experimental.stream_v2._serializer import BatchSerializer
            from nominal.experimental.stream_v2._write_stream import WriteStreamV2
        except ImportError as ex:
            raise ImportError(
                "nominal-api-protos is required to use get_write_stream with data_format='experimental'"
            ) from ex

        return WriteStreamV2.create(
            clients=clients,
            serializer=BatchSerializer.create(max_workers=None),
            nominal_data_source_rid=write_rid,
            max_batch_size=batch_size,
            max_wait=max_wait,
            max_queue_size=0,
            track_metrics=True,
            max_workers=None,
        )
    elif data_format == "rust_experimental":
        # Delayed import intentionally in case of any issues with experimental and pre-compiled binaries
        try:
            from nominal.experimental.rust_streaming.rust_write_stream import RustWriteStream
        except ImportError as ex:
            raise ImportError(
                "nominal-streaming is required to use get_write_stream with data_format='rust_experimental'"
            ) from ex

        return RustWriteStream._from_datasource(
            write_rid,
            clients,
            batch_size=batch_size,
            max_wait=max_wait,
            file_fallback=file_fallback,
            log_level=log_level,
            num_workers=num_workers,
        )
    else:
        raise ValueError(
            f"Expected `data_format` to be one of {{json, protobuf, experimental}}, received '{data_format}'"
        )
