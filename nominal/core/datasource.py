from __future__ import annotations

import logging
from dataclasses import dataclass, field
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
    storage_writer_api,
    timeseries_logicalseries,
    timeseries_logicalseries_api,
    upload_api,
)

from nominal.core._clientsbunch import HasAuthHeader, ProtoWriteService
from nominal.core._conjure_utils import _available_units, _build_unit_update
from nominal.core._utils import HasRid
from nominal.core.channel import Channel, _get_series_values_csv
from nominal.ts import _MAX_TIMESTAMP, _MIN_TIMESTAMP

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

    def get_channel(self, name: str) -> Channel:  # TODO(vtupuri): use new endpoint
        for channel in self.get_channels(exact_match=[name]):
            if channel.name == name:
                return channel
        raise ValueError(f"channel {name!r} not found in dataset {self.rid!r}")

    def get_channels(
        self,
        exact_match: Sequence[str] = (),
        fuzzy_search_text: str = "",
    ) -> Iterable[Channel]:  # TODO(vtupuri): use new endpoint
        """Look up the metadata for all matching channels associated with this dataset.
        NOTE: Provided channels may also be associated with other datasets-- use with caution.

        Args:
        ----
            exact_match: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
                For example, a channel named 'engine_turbine_rpm' would match against ['engine', 'turbine', 'rpm'],
                whereas a channel named 'engine_turbine_flowrate' would not!
            fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.

        Yields:
        ------
            Yields a sequence of channel metadata objects which match the provided query parameters

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
                # Skip series archetypes for now-- they aren't handled by the rest of the SDK in a graceful manner
                if channel_metadata.series_rid.logical_series is None:
                    continue

                yield Channel._from_conjure_datasource_api(self._clients, channel_metadata)

            if response.next_page_token is None:
                break
            else:
                next_page_token = response.next_page_token

    def to_pandas(self, channel_exact_match: Sequence[str] = (), channel_fuzzy_search_text: str = "") -> pd.DataFrame:
        """Download a dataset to a pandas dataframe, optionally filtering for only specific channels of the dataset.

        Args:
        ----
            channel_exact_match: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
                For example, a channel named 'engine_turbine_rpm' would match against ['engine', 'turbine', 'rpm'],
                whereas a channel named 'engine_turbine_flowrate' would not!
            channel_fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided
                string.

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
        rid_name = {ch.rid: ch.name for ch in self.get_channels(channel_exact_match, channel_fuzzy_search_text)}
        # TODO(alkasm): parametrize start/end times with dataset bounds
        body = _get_series_values_csv(
            self._clients.auth_header,
            self._clients.dataexport,
            rid_name,
            _MIN_TIMESTAMP.to_api(),
            _MAX_TIMESTAMP.to_api(),
        )
        df = pd.read_csv(body, parse_dates=["timestamp"], index_col="timestamp")
        return df

    def set_channel_units(
        self, channels_to_units: Mapping[str, str | None], validate_schema: bool = False
    ) -> None:  # TODO(vtupuri): use new endpoint
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

        # Get metadata (specifically, RIDs) for all requested channels
        found_channels = {channel.name: channel for channel in self.get_channels() if channel.name in channels_to_units}

        # For each channel / unit combination, create an update request to set the series's unit
        # to that symbol
        update_requests = []
        for channel_name, unit in channels_to_units.items():
            # No data uploaded to channel yet ...
            if channel_name not in found_channels:
                if validate_schema:
                    raise ValueError(
                        f"Unable to set unit for {channel_name} to {unit_symbol}: no data uploaded for channel"
                    )
                else:
                    logger.info("Not setting unit for channel %s: no data uploaded for channel", channel_name)
                    continue

            channel = found_channels[channel_name]
            channel_request = timeseries_logicalseries_api.UpdateLogicalSeries(
                logical_series_rid=channel.rid,
                unit_update=_build_unit_update(unit),
            )
            update_requests.append(channel_request)

        if not update_requests:
            return
        # Set units in database
        request = timeseries_logicalseries_api.BatchUpdateLogicalSeriesRequest(update_requests)
        self._clients.logical_series.batch_update_logical_series(self._clients.auth_header, request)

    def set_channel_prefix_tree(self, delimiter: str = ".") -> None:  # TODO(vtupuri): use new endpoint
        """Index channels hierarchically by a given delimiter.

        Primarily, the result of this operation is to prompt the frontend to represent channels
        in a tree-like manner that allows folding channels by common roots.
        """
        request = datasource_api.IndexChannelPrefixTreeRequest(self.rid, delimiter=delimiter)
        self._clients.datasource.index_channel_prefix_tree(self._clients.auth_header, request)
