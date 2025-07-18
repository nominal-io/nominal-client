from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Literal, Mapping, Protocol, Sequence

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
    timeseries_logicalseries,
    upload_api,
)

from nominal._utils import batched
from nominal.core._batch_processor import process_batch_legacy
from nominal.core._clientsbunch import HasScoutParams, ProtoWriteService
from nominal.core._utils import HasRid
from nominal.core.channel import Channel, ChannelDataType, _create_timestamp_format
from nominal.core.stream import WriteStream
from nominal.core.unit import UnitMapping, _build_unit_update, _error_on_invalid_units
from nominal.core.write_stream_base import WriteStreamBase
from nominal.ts import IntegralNanosecondsUTC, _LiteralTimeUnit

logger = logging.getLogger(__name__)


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
        def logical_series(self) -> timeseries_logicalseries.LogicalSeriesService: ...
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

        # Process in batches of 500
        batch_size = 500
        for batch_channel_names in batched(names, batch_size):
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

    def get_write_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
        data_format: Literal["json", "protobuf", "experimental"] = "json",
    ) -> WriteStreamBase:
        """Stream to write messages to a datasource.

        Messages are written asynchronously.

        Args:
        ----
            batch_size: How big the batch can get before writing to Nominal.
            max_wait: How long a batch can exist before being flushed to Nominal.
            data_format: Serialized data format to use during upload.
                NOTE: selecting 'protobuf' or 'experimental' requires that `nominal` was installed
                      with `protos` extras.

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
            write_rid=self.rid,
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


def _construct_export_request(
    channels: Sequence[Channel],
    start: api.Timestamp,
    end: api.Timestamp,
    tags: Mapping[str, str] | None,
    enable_gzip: bool,
    relative_to: datetime | IntegralNanosecondsUTC | None = None,
    relative_resolution: _LiteralTimeUnit = "nanoseconds",
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
                output_timestamp_format=_create_timestamp_format(relative_to, relative_resolution),
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
    data_format: Literal["json", "protobuf", "experimental"],
    write_rid: str,
    clients: DataSource._Clients,
) -> WriteStreamBase:
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
            from nominal.core._batch_processor_proto import process_batch
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
    else:
        raise ValueError(
            f"Expected `data_format` to be one of {{json, protobuf, experimental}}, received '{data_format}'"
        )
