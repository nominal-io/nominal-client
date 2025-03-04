from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Mapping, Protocol, Sequence

import pandas as pd
from nominal_api import (
    datasource_api,
    ingest_api,
    scout,
    scout_catalog,
    scout_dataexport_api,
    scout_datasource,
    scout_datasource_connection,
    scout_run_api,
    storage_writer_api,
    timeseries_channelmetadata,
    timeseries_channelmetadata_api,
    timeseries_logicalseries,
    upload_api,
)

from nominal.core._clientsbunch import HasAuthHeader, ProtoWriteService
from nominal.core._conjure_utils import _available_units, _build_unit_update
from nominal.core._utils import HasRid
from nominal.core.channel_v2 import Channel
from nominal.ts import IntegralNanosecondsUTC

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
        def channel_metadata(self) -> timeseries_channelmetadata.ChannelService: ...

    def get_channel(self, name: str) -> Channel:  # TODO(vtupuri): use new endpoint
        for channel in self.get_channels([name]):
            if channel.name == name:
                return channel
        raise ValueError(f"channel {name!r} not found in dataset {self.rid!r}")

    def get_channels(
        self,
        channel_names: list[str] | None = None,
    ) -> Iterable[Channel]:  # TODO(vtupuri): use new endpoint
        """Look up the metadata for all matching channels associated with this dataset.
        NOTE: Provided channels may also be associated with other datasets-- use with caution.

        Args:
        ----
            channel_names: List of channel names to look up metadata for.

        Yields:
        ------
            Yields a sequence of channel metadata objects which match the provided query parameters

        """
        if not channel_names:
            channel_names = [channel.name for channel in self.search_channels()]

        requests = [
            timeseries_channelmetadata_api.GetChannelMetadataRequest(
                channel_name=channel_name, data_source_rid=self.rid
            )
            for channel_name in channel_names
        ]

        batch_request = timeseries_channelmetadata_api.BatchGetChannelMetadataRequest(requests=requests)
        response = self._clients.channel_metadata.batch_get_channel_metadata(self._clients.auth_header, batch_request)

        return (Channel._from_channel_metadata_api(self._clients, channel) for channel in response.responses)

    def search_channels(
        self,
        exact_match: Sequence[str] = (),
        fuzzy_search_text: str = "",
        tags: dict[str, str] = {},
        start: scout_run_api.UtcTimestamp | None = None,
        end: scout_run_api.UtcTimestamp | None = None,
    ) -> Iterable[Channel]:
        """Look up channels associated with a datasource.

        Args:
            exact_match: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
            fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.
            tags: Dictionary of tags to filter channels by
            start: The minimum data updated time to filter channels by
            end: The maximum data start time to filter channels by

        Yields:
            Channel objects for each matching channel
        """
        query = datasource_api.SearchFilteredChannelsRequest(
            data_sources=[self.rid],
            exact_match=list(exact_match),
            fuzzy_search_text=fuzzy_search_text,
            tags={self.rid: tags} if tags else {},
            min_data_updated_time=start if start else None,
            max_data_start_time=end if end else None,
        )
        response = self._clients.datasource.search_filtered_channels(self._clients.auth_header, query)
        for channel_metadata in response.results:
            yield Channel._from_conjure_datasource_api(self._clients, channel_metadata)

    def to_pandas(
        self,
        channel_exact_match: Sequence[str] = (),
        channel_fuzzy_search_text: str = "",
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        tags: dict[str, str] = {},
    ) -> pd.DataFrame:
        """Download a dataset to a pandas dataframe, optionally filtering for only specific channels of the dataset.

        Args:
        ----
            channel_exact_match: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
                For example, a channel named 'engine_turbine_rpm' would match against ['engine', 'turbine', 'rpm'],
                whereas a channel named 'engine_turbine_flowrate' would not!
            channel_fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided
                string.
            tags: Dictionary of tags to filter channels by
            start: The minimum data updated time to filter channels by
            end: The maximum data start time to filter channels by

        Returns:
        -------
            A pandas dataframe whose index is the timestamp of the data, and column names match those of the selected
                channels.

        Example:
        -------
        ```
        import nominal as nm

        rid = "..." # Taken from the UI or via the SDK
        dataset = nm.get_dataset(rid)
        s = dataset.to_pandas()
        print("index:", s.index, "index mean:", s.index.mean())
        ```

        """
        df = pd.DataFrame()
        return df

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
        # Get the set of all available unit symbols
        supported_symbols = set(
            [unit.symbol for unit in _available_units(self._clients.auth_header, self._clients.units)]
        )

        # Validate that all user provided unit symbols are valid
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
        found_channels = {channel.name: channel for channel in self.get_channels(list(channels_to_units.keys()))}

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
                channel_name=channel_name,
                data_source_rid=self.rid,
                unit_update=_build_unit_update(unit),
            )
            update_requests.append(channel_request)

        if not update_requests:
            return

        # Set units in database using batch update
        batch_request = timeseries_channelmetadata_api.BatchUpdateChannelMetadataRequest(requests=update_requests)
        self._clients.channel_metadata.batch_update_channel_metadata(self._clients.auth_header, batch_request)

    def set_channel_prefix_tree(self, delimiter: str = ".") -> None:  # TODO(vtupuri): use new endpoint
        """Index channels hierarchically by a given delimiter.

        Primarily, the result of this operation is to prompt the frontend to represent channels
        in a tree-like manner that allows folding channels by common roots.
        """
        request = datasource_api.IndexChannelPrefixTreeRequest(self.rid, delimiter=delimiter)
        self._clients.datasource.index_channel_prefix_tree(self._clients.auth_header, request)
