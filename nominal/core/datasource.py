from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Iterable, Literal, Mapping, Protocol, Sequence, TypeAlias, overload

from google.protobuf.field_mask_pb2 import FieldMask
from nominal_api import (
    api,
    datasource_api,
    ingest_api,
    scout_catalog,
    scout_compute_api,
    scout_dataexport_api,
    scout_datasource,
    scout_datasource_connection,
    scout_video,
    storage_writer_api,
    upload_api,
)

from nominal._utils import batched
from nominal._utils.deprecation_tools import warn_on_deprecated_argument
from nominal.core._clientsbunch import HasScoutParams, ProtoWriteService
from nominal.core._stream.batch_processor_proto import process_batch
from nominal.core._stream.write_stream import DataStream, WriteStream
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.marking import MarkableMixin
from nominal.core.unit import UnitMapping, _error_on_invalid_units, _unit_symbol_from_unit_like
from nominal.protos.authentication.users.v1 import users_pb2_grpc
from nominal.protos.authorization.markings.v1 import markings_pb2_grpc
from nominal.protos.authorization.roles.v1 import roles_pb2_grpc
from nominal.protos.channelmetadata.v1 import channel_metadata_pb2
from nominal.protos.ingest.v2 import containerized_extractor_pb2_grpc
from nominal.protos.registry.v2 import registry_pb2_grpc
from nominal.protos.seriesmetadata.v1 import series_metadata_pb2, series_metadata_pb2_grpc
from nominal.protos.units.v1 import units_pb2_grpc
from nominal.ts import (
    _AnyExportableTimestampType,
    _to_export_timestamp_format,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHANNEL_BATCH_SIZE = 500
_MAX_CHANNEL_UPDATE_BATCH_SIZE = 10_000


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
class DataSource(HasRid, MarkableMixin):
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
        def video(self) -> scout_video.VideoService: ...
        @property
        def units(self) -> units_pb2_grpc.UnitsServiceStub: ...
        @property
        def ingest(self) -> ingest_api.IngestService: ...
        @property
        def ingest_jobs(self) -> ingest_api.IngestJobService: ...
        @property
        def upload(self) -> upload_api.UploadService: ...
        @property
        def connection(self) -> scout_datasource_connection.ConnectionService: ...
        @property
        def storage_writer(self) -> storage_writer_api.NominalChannelWriterService: ...
        @property
        def proto_write(self) -> ProtoWriteService: ...
        @property
        def series_metadata(self) -> series_metadata_pb2_grpc.SeriesMetadataServiceStub: ...
        @property
        def containerized_extractor(
            self,
        ) -> containerized_extractor_pb2_grpc.ContainerizedExtractorServiceStub: ...
        @property
        def registry(self) -> registry_pb2_grpc.RegistryServiceStub: ...
        @property
        def users(self) -> users_pb2_grpc.UserServiceStub: ...
        @property
        def roles(self) -> roles_pb2_grpc.RoleServiceStub: ...
        @property
        def markings(self) -> markings_pb2_grpc.MarkingServiceStub: ...

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
            names: List of channel names to look up metadata for.

        Yields:
            Yields a sequence of channel metadata objects which match the provided query parameters

        """
        if not names:
            names = [channel.name for channel in self.search_channels()]

        for batch_channel_names in batched(names, _DEFAULT_CHANNEL_BATCH_SIZE):
            request = channel_metadata_pb2.BatchGetChannelMetadataRequest(
                channel_identifiers=[
                    channel_metadata_pb2.ChannelIdentifier(channel_name=name, data_source_rid=self.rid)
                    for name in batch_channel_names
                ]
            )
            with translate_grpc_errors():
                response = self._clients.channel_metadata.BatchGetChannelMetadata(request)
            yield from (Channel._from_proto(self._clients, channel) for channel in response.channel_metadata)

    @property
    def _write_rid(self) -> str:
        """RID that write streams send to. A datasource is written to directly."""
        return self.rid

    # `implementation` and `data_format` are spelled in separate overloads so that passing both is a
    # type error, matching the ValueError raised at runtime. The rust overload comes first so that a
    # bare `get_write_stream()` resolves to the implementation it actually gets.
    @overload
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        implementation: Literal["rust", "rust_experimental"] | None = None,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
        track_metrics: bool = False,
    ) -> DataStream: ...
    @overload
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        implementation: Literal["python", "json", "protobuf", "experimental"] = ...,
    ) -> DataStream: ...
    @overload
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        *,
        data_format: Literal["rust", "rust_experimental"],
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
        track_metrics: bool = False,
    ) -> DataStream: ...
    @overload
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        *,
        data_format: Literal["python", "json", "protobuf", "experimental"],
    ) -> DataStream: ...
    @warn_on_deprecated_argument(
        "data_format",
        "The `data_format` argument is deprecated: pass `implementation` instead.",
    )
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        implementation: Literal["python", "rust", "json", "protobuf", "experimental", "rust_experimental"]
        | None = None,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
        track_metrics: bool = False,
        *,
        data_format: Literal["python", "rust", "json", "protobuf", "experimental", "rust_experimental"] | None = None,
    ) -> DataStream:
        """Stream to write timeseries data to a datasource.

        Data is written asynchronously.

        Args:
            batch_size: How big the batch can get before writing to Nominal.
            max_wait: How long a batch can exist before being flushed to Nominal.
            implementation: Streaming implementation to use: 'rust' or 'python'. When omitted, uses 'rust'
                if `nominal-streaming` is available, otherwise 'python'. Automatic selection cannot fall back
                to 'python' when ``file_fallback``, ``log_level``, or ``num_workers`` is supplied,
                or when ``track_metrics=True``.

                **Note:** 'json', 'protobuf', and 'rust_experimental' are deprecated spellings of
                'python', 'python', and 'rust' respectively.

                **Note:** 'experimental' is deprecated and retains its Python metrics implementation.
                Use ``implementation='rust'`` with ``track_metrics=True`` to migrate.
            file_fallback: Filepath to write failed batches to during streaming.

                **Note:** Expects a .avro filename.

                **Note:** Supported only by the Rust backend.
            log_level: Log level to use in underlying rust streaming code.

                **Note:** Should be a rust log level e.g. 'debug', 'trace', 'info', etc.

                **Note:** Supported only by the Rust backend.
            num_workers: Number of Rust upload workers and runtime worker threads.

                **Note:** Use with care-- this may have large impacts on streaming performance.

                **Note:** Supported only by the Rust backend.
            track_metrics: Emit request round-trip time and point staleness in seconds as
                ``__nominal.metric.*`` channels on the same dataset. Staleness is the SDK machine's
                wall-clock UTC time minus the point timestamp; future timestamps produce negative values.
                Request metrics measure the oldest and newest points immediately before and after a
                successful upload. ``enqueue_from_dict`` also emits ``enque_dict_start_staleness`` and
                ``enque_dict_end_staleness``, measured at the start and end of the enqueue. Disabled by default.

                **Note:** Request metrics are best-effort and travel with later data uploads. Pending request
                metrics are not sent separately on close and may be dropped if their buffer fills.

                **Note:** Supported only by the Rust backend.
            data_format: Deprecated name for `implementation`. Passing both is an error.

        Returns:
            Write stream object configured to send data to nominal. This may be used as a context manager
            (so that resources are automatically released upon exiting the context), or if not used as a context
            manager, should be explicitly `close()`-ed once no longer needed.

        Raises:
            ValueError: both `implementation` and `data_format` were given, or the implementation is unknown.
            ImportError: `nominal-streaming` is not installed and rust streaming was asked for, either
                explicitly via `implementation` or implicitly by passing a rust-only argument.
        """
        return _get_write_stream(
            batch_size=batch_size,
            max_wait=max_wait,
            implementation=implementation,
            data_format=data_format,
            file_fallback=file_fallback,
            log_level=log_level,
            num_workers=num_workers,
            track_metrics=track_metrics,
            write_rid=self._write_rid,
            clients=self._clients,
        )

    def search_channels(
        self,
        exact_match: Sequence[str] = (),
        fuzzy_search_text: str = "",
        *,
        data_types: Sequence[ChannelDataType] | None = None,
    ) -> Iterable[Channel]:
        """Look up channels associated with a datasource.

        Args:
            exact_match: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
            fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.
            data_types: Filter the returned channels to those that match any of the provided types

        Yields:
            Channel objects for each matching channel
        """
        allowable_types = set(data_types) if data_types else None
        next_page_token = None
        while True:
            query = datasource_api.SearchChannelsRequest(
                data_sources=[self.rid],
                exact_match=list(exact_match),
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

                yield Channel._from_conjure_datasource_api(self._clients, channel_metadata)
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

        Updates are sent sequentially in batches of at most 10,000 matching channels.
        Each batch is atomic. If a later batch fails, earlier batches remain applied and
        subsequent batches are not sent.

        Args:
            channels_to_units: A mapping of channel names to unit symbols.

                **Note:** Any existing units may be cleared from a channel by providing None as a symbol.
            validate_schema: If true, raises a ValueError if non-existent channel names are provided in
                `channels_to_units`. Default is False.
            allow_display_only_units: If true, allow units that would be treated as display-only by Nominal.

        Raises:
            ValueError: Unsupported unit symbol provided
            NominalError: Error validating or updating units via gRPC (e.g. NominalNotFoundError).
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
            _error_on_invalid_units(channels_to_units, self._clients.units)

        for batch in batched(channels_to_units.items(), _MAX_CHANNEL_UPDATE_BATCH_SIZE):
            update_requests = [
                channel_metadata_pb2.UpdateChannelMetadataRequest(
                    channel_metadata=channel_metadata_pb2.ChannelMetadata(
                        channel_identifier=channel_metadata_pb2.ChannelIdentifier(
                            channel_name=channel_name, data_source_rid=self.rid
                        ),
                        unit=_unit_symbol_from_unit_like(unit),
                    ),
                    # A selected but unset unit clears it; an unselected field is unchanged.
                    update_mask=FieldMask(paths=["unit"]),
                )
                for channel_name, unit in batch
            ]
            request = channel_metadata_pb2.BatchUpdateChannelMetadataRequest(requests=update_requests)
            with translate_grpc_errors():
                self._clients.channel_metadata.BatchUpdateChannelMetadata(request)

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
            The created or existing Channel object. Existing channels are returned unchanged.

        Raises:
            NominalError: Error creating the channel or fetching its metadata via gRPC.
            ValueError: Channel metadata was not found after creation.
        """
        request = _build_series_metadata_request(
            self.rid,
            CreateChannelRequest(name=name, data_type=data_type, description=description, unit=unit),
            if_exists=series_metadata_pb2.RETURN_EXISTING,
        )
        with translate_grpc_errors():
            self._clients.series_metadata.CreateSeriesMetadata(request)
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
            Existing channels are returned unchanged. If a name appears more than once and the
            channel does not already exist, the first request creates it; later requests with that
            name do not change its type, unit, or description, even across batch boundaries.
            The result's ``channels`` contains at most one Channel per distinct name, rather than
            one per request. If the metadata lookup omits a name, every request with that name
            is included in ``missing``.

            Batches are written sequentially; earlier batches remain applied if a later batch fails.
        """
        return self._batch_write_channels(channels, if_exists=series_metadata_pb2.RETURN_EXISTING)

    def batch_update_or_create_channels(
        self,
        channels: Sequence[CreateChannelRequest],
    ) -> BatchAddChannelsResult:
        """Create or update multiple channels (series metadata) for this data source in batches.

        For each request, creates the channel if it does not already exist. If it does exist,
        updates the locator and any provided `unit` and `description` fields while preserving
        existing values for fields left as None.

        Args:
            channels: Sequence of CreateChannelRequest objects.

        Returns:
            A BatchAddChannelsResult with the upserted channels and any requests that were
            not found afterwards (i.e. silently dropped by the server).

        Note:
            If the same channel name appears more than once within a single batch, the entire
            batch will fail. Callers are responsible for deduplicating channel names before
            passing them to this method. Earlier batches remain applied if a later batch fails.
        """
        return self._batch_write_channels(channels, if_exists=series_metadata_pb2.UPDATE_EXISTING)

    def _batch_write_channels(
        self,
        channels: Sequence[CreateChannelRequest],
        *,
        if_exists: series_metadata_pb2.IfSeriesMetadataExists.ValueType,
    ) -> BatchAddChannelsResult:
        if not channels:
            return BatchAddChannelsResult(channels=[], missing=[])
        for batch in batched(channels, _DEFAULT_CHANNEL_BATCH_SIZE):
            requests = [_build_series_metadata_request(self.rid, req, if_exists=if_exists) for req in batch]
            batch_request = series_metadata_pb2.BatchCreateSeriesMetadataRequest(requests=requests)
            with translate_grpc_errors():
                self._clients.series_metadata.BatchCreateSeriesMetadata(batch_request)
        created = {ch.name: ch for ch in self.get_channels(names=[req.name for req in channels])}
        return BatchAddChannelsResult(
            channels=list(created.values()),
            missing=[req for req in channels if req.name not in created],
        )


def _build_series_metadata_request(
    data_source_rid: str,
    req: CreateChannelRequest,
    *,
    if_exists: series_metadata_pb2.IfSeriesMetadataExists.ValueType,
) -> series_metadata_pb2.CreateSeriesMetadataRequest:
    nominal_data_type = req.data_type._to_proto()
    locator = series_metadata_pb2.LocatorTemplate(
        nominal=series_metadata_pb2.NominalLocatorTemplate(channel=req.name, data_type=nominal_data_type)
    )
    return series_metadata_pb2.CreateSeriesMetadataRequest(
        channel=req.name,
        data_source_rid=data_source_rid,
        locator_template=locator,
        description=req.description,
        unit=req.unit,
        if_exists=if_exists,
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
            dataset_references={},
            variables={},
            function_variables={},
        ),
        format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
        resolution=scout_dataexport_api.ResolutionOption(
            undecimated=scout_dataexport_api.UndecimatedResolution(),
        ),
        compression=scout_dataexport_api.CompressionFormat.GZIP if enable_gzip else None,
    )
    return request


# Every spelling `implementation` accepts, including the superseded ones. The public signatures spell
# the literals out so they render in the docs; the plumbing below uses this.
_AnyImplementation: TypeAlias = Literal["python", "rust", "json", "protobuf", "experimental", "rust_experimental"]


# Superseded spellings. Each still works and each warns; the value is the guidance in the warning.
_DEPRECATED_IMPLEMENTATIONS = {
    "json": "use implementation='python'",
    "protobuf": "use implementation='python'",
    "rust_experimental": "use implementation='rust'",
    "experimental": "use implementation='rust' with track_metrics=True to keep runtime metrics",
}

# _resolve_implementation -> _get_write_stream -> get_write_stream -> the
# @warn_on_deprecated_argument wrapper -> the caller we want to blame.
#
# Update this when changing the call chain.
_DEPRECATION_STACKLEVEL = 5


def _resolve_implementation(
    implementation: _AnyImplementation | None,
    data_format: _AnyImplementation | None,
) -> Literal["python", "rust", "experimental"] | None:
    """Map what the caller asked for onto a surviving implementation, warning about old spellings.

    Returns `None` when the caller named nothing, which means "pick one for me" -- that choice
    depends on what is installed, so `_get_write_stream` makes it.
    """
    if data_format is not None:
        if implementation is not None:
            raise ValueError("Pass only `implementation`: `data_format` is its deprecated name")

        # The deprecation itself is warned about by @warn_on_deprecated_argument on get_write_stream.
        implementation = data_format

    if implementation in _DEPRECATED_IMPLEMENTATIONS:
        warnings.warn(
            f"implementation={implementation!r} is deprecated: {_DEPRECATED_IMPLEMENTATIONS[implementation]}.",
            UserWarning,
            stacklevel=_DEPRECATION_STACKLEVEL,
        )

    match implementation:
        case None:
            return None
        case "json" | "protobuf":
            # Both were pure-python streams; 'python' is whichever of the two is worth having.
            return "python"
        case "rust_experimental":
            return "rust"
        case "python" | "rust" | "experimental":
            return implementation
        case _:
            raise ValueError(
                f"Expected `implementation` to be one of {{python, rust, experimental}}, received {implementation!r}"
            )


def _python_write_stream(
    batch_size: int, max_wait: timedelta, write_rid: str, clients: DataSource._Clients
) -> DataStream:
    """Build the pure-python stream, which serializes protobuf."""
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


def _get_write_stream(
    batch_size: int,
    max_wait: timedelta,
    implementation: _AnyImplementation | None,
    data_format: _AnyImplementation | None,
    file_fallback: PathLike | None,
    log_level: str | None,
    num_workers: int | None,
    track_metrics: bool,
    write_rid: str,
    clients: DataSource._Clients,
) -> DataStream:
    requested = _resolve_implementation(implementation, data_format)

    used = [
        key
        for key, value in {
            "file_fallback": file_fallback,
            "log_level": log_level,
            "num_workers": num_workers,
        }.items()
        if value is not None
    ]
    if track_metrics:
        used.append("track_metrics")

    if requested is not None and requested != "rust":
        for key in used:
            logger.warning("Argument %s has no effect unless `implementation='rust'`", key)

    if requested == "python":
        return _python_write_stream(batch_size, max_wait, write_rid, clients)

    if requested == "experimental":
        # Delayed import: `nominal.experimental.stream_v2` imports `nominal.core.connection`, which
        # imports this module. Not optional -- purely to break the cycle.
        from nominal.experimental.stream_v2._serializer import BatchSerializer
        from nominal.experimental.stream_v2._write_stream import WriteStreamV2

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

    # Rust, either named outright or chosen because nothing was. Delayed, and deliberately kept
    # apart from constructing the stream: a failed import means rust is unavailable on this machine,
    # while a failure building the stream is a real error that should surface.
    try:
        from nominal.core._stream.rust_write_stream import RustWriteStream
    except ImportError as ex:
        # Falling back would drop rust-only arguments. `file_fallback` in particular is a durability
        # feature, and silently turning it into a no-op is worse than refusing.
        if requested == "rust" or used:
            reason = (
                f"Using {', '.join(used)} requires the Rust streaming backend. nominal-streaming could not be imported."
                if used
                else "implementation='rust' requires nominal-streaming, which could not be imported."
            )
            raise ImportError(
                f"{reason} It ships pre-compiled binaries for a subset of platforms and interpreters; "
                "install it with: pip install nominal-streaming"
            ) from ex

        logger.info(
            "nominal-streaming is unavailable, falling back to `implementation='python'` streaming. "
            "Install nominal-streaming for higher-throughput streaming."
        )
        return _python_write_stream(batch_size, max_wait, write_rid, clients)

    return RustWriteStream._from_datasource(
        write_rid,
        clients,
        batch_size=batch_size,
        max_wait=max_wait,
        file_fallback=file_fallback,
        log_level=log_level,
        num_workers=num_workers,
        track_metrics=track_metrics,
    )
