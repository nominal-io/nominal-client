from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Mapping, Protocol, Sequence

import typing_extensions
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

from nominal._utils import warn_on_deprecated_argument
from nominal.core._clientsbunch import HasAuthHeader, ProtoWriteService
from nominal.core._conjure_utils import _available_units, _build_unit_update
from nominal.core._utils import HasRid, batched
from nominal.core.channel import Channel, ChannelDataType
from nominal.ts import IntegralNanosecondsUTC

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataSource(HasRid):
    rid: str
    _clients: _Clients = field(repr=False)

    class _Clients(Channel._Clients, HasAuthHeader, Protocol):
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

    @warn_on_deprecated_argument(
        "tags", "The 'tags' argument is deprecated because it's not used and will be removed in a future version."
    )
    def get_channel(self, name: str, tags: dict[str, str] | None = None) -> Channel:
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

    def search_channels(
        self,
        exact_match: Sequence[str] = (),
        fuzzy_search_text: str = "",
    ) -> Iterable[Channel]:
        """Look up channels associated with a datasource.

        Args:
            exact_match: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
            fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.

        Yields:
            Channel objects for each matching channel
        """
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
                yield Channel._from_conjure_datasource_api(self._clients, channel_metadata)
            if response.next_page_token is None:
                break
            next_page_token = response.next_page_token

    @typing_extensions.deprecated(
        "`datasource.to_pandas` is deprecated and will be removed in a future version. "
        "Use `nominal.thirdparty.pandas.datasource_to_dataframe` instead."
    )
    def to_pandas(
        self,
        channel_exact_match: Sequence[str] = (),
        channel_fuzzy_search_text: str = "",
        start: str | datetime | IntegralNanosecondsUTC | None = None,
        end: str | datetime | IntegralNanosecondsUTC | None = None,
        tags: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Download a dataset to a pandas dataframe, optionally filtering for only specific channels of the dataset."""
        from nominal.thirdparty.pandas import datasource_to_dataframe

        return datasource_to_dataframe(self, channel_exact_match, channel_fuzzy_search_text, start, end, tags)

    def set_channel_units(self, channels_to_units: Mapping[str, str | None], validate_schema: bool = False) -> None:
        """Set units for channels based on a provided mapping of channel names to units.

        Args:
        ----
            channels_to_units: A mapping of channel names to unit symbols.
                NOTE: any existing units may be cleared from a channel by providing None as a symbol.
            validate_schema: If true, raises a ValueError if non-existent channel names are provided in
                `channels_to_units`. Default is False.

        Raises:
        ------
            ValueError: Unsupported unit symbol provided
            conjure_python_client.ConjureHTTPError: Error completing requests.
        """
        # Validate that all user provided unit symbols are valid
        if validate_schema:
            # Get the set of all available unit symbols
            supported_symbols = set(
                [unit.symbol for unit in _available_units(self._clients.auth_header, self._clients.units)]
            )

            for channel_name, unit_symbol in channels_to_units.items():
                # User is clearing the unit for this channel-- don't validate
                if unit_symbol is None:
                    continue

                if unit_symbol not in supported_symbols:
                    raise ValueError(
                        f"Provided unit '{unit_symbol}' for channel '{channel_name}' does not resolve to a unit "
                        "recognized by nominal. For more information on valid symbols, see https://ucum.org/ucum"
                    )

        # Get metadata for all requested channels
        found_channels = {channel.name: channel for channel in self.get_channels(names=list(channels_to_units.keys()))}

        # For each channel / unit combination, create an update request
        update_requests = []

        for channel_name, unit in channels_to_units.items():
            # No data uploaded to channel yet ...
            if channel_name not in found_channels:
                if validate_schema:
                    raise ValueError(f"Unable to set unit for {channel_name} to {unit}: no data uploaded for channel")
                else:
                    logger.info("Not setting unit for channel %s: no data uploaded for channel", channel_name)
                    continue

            channel_request = timeseries_channelmetadata_api.UpdateChannelMetadataRequest(
                channel_identifier=timeseries_channelmetadata_api.ChannelIdentifier(
                    channel_name=channel_name, data_source_rid=self.rid
                ),
                unit_update=_build_unit_update(unit),
            )
            update_requests.append(channel_request)

        if not update_requests:
            return

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
    datasource_rid: str,
    start: api.Timestamp,
    end: api.Timestamp,
    tags: dict[str, str] | None,
) -> scout_dataexport_api.ExportDataRequest:
    export_channels = []

    converted_tags = {}
    if tags:
        for key, value in tags.items():
            converted_tags[key] = scout_compute_api.StringConstant(literal=value)
    for channel in channels:
        if channel.data_type == ChannelDataType.DOUBLE:
            export_channels.append(
                scout_dataexport_api.TimeDomainChannel(
                    column_name=channel.name,
                    compute_node=scout_compute_api.Series(
                        numeric=scout_compute_api.NumericSeries(
                            channel=scout_compute_api.ChannelSeries(
                                data_source=scout_compute_api.DataSourceChannel(
                                    channel=scout_compute_api.StringConstant(literal=channel.name),
                                    data_source_rid=scout_compute_api.StringConstant(literal=datasource_rid),
                                    tags=converted_tags,
                                )
                            )
                        )
                    ),
                )
            )
        elif channel.data_type == ChannelDataType.STRING:
            export_channels.append(
                scout_dataexport_api.TimeDomainChannel(
                    column_name=channel.name,
                    compute_node=scout_compute_api.Series(
                        enum=scout_compute_api.EnumSeries(
                            channel=scout_compute_api.ChannelSeries(
                                data_source=scout_compute_api.DataSourceChannel(
                                    channel=scout_compute_api.StringConstant(literal=channel.name),
                                    data_source_rid=scout_compute_api.StringConstant(literal=datasource_rid),
                                    tags=converted_tags,
                                )
                            )
                        )
                    ),
                )
            )

    request = scout_dataexport_api.ExportDataRequest(
        channels=scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=export_channels,
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    # only one series will be returned, so no need to merge
                    none=scout_dataexport_api.NoneStrategy(),
                ),
                output_timestamp_format=scout_dataexport_api.TimestampFormat(
                    iso8601=scout_dataexport_api.Iso8601TimestampFormat()
                ),
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
    )
    return request
