from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Protocol, Sequence

import pandas as pd
from nominal_api import (
    datasource_api,
    scout_dataexport_api,
    scout_run_api,
    scout_datasource,
    timeseries_logicalseries,
    timeseries_logicalseries_api,
)

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core.channel import Channel, _get_series_values_csv
from nominal.ts import _MAX_TIMESTAMP, _MIN_TIMESTAMP, IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


class ExportClients(Channel._Clients, HasAuthHeader, Protocol):
    @property
    def auth_header(self) -> str: ...
    @property
    def datasource(self) -> scout_datasource.DataSourceService: ...
    @property
    def dataexport(self) -> scout_dataexport_api.DataExportService: ...
    @property
    def logical_series(self) -> timeseries_logicalseries.LogicalSeriesService: ...


def get_channels(
    clients: ExportClients,
    datasource_rid: str,
    min_data_updated_time: scout_run_api.UtcTimestamp,
    max_data_start_time: scout_run_api.UtcTimestamp,
    exact_match: Sequence[str] = (),
    fuzzy_search_text: str = "",
    tags: dict[str, str] = {},
    
) -> Iterable[Channel]:
    """Look up channels associated with a datasource.

    Args:
        clients: Client objects with necessary services
        datasource_rid: The RID of the datasource to search channels in
        exact_match: Filter the returned channels to those whose names match all provided strings
            (case insensitive).
        fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.

    Yields:
        Channel objects for each matching channel
    """
    query = datasource_api.SearchFilteredChannelsRequest(
        data_sources=[datasource_rid],
        exact_match=list(exact_match),
        fuzzy_search_text=fuzzy_search_text,
        tags = {datasource_rid: tags},
        min_data_updated_time=min_data_updated_time,
        max_data_start_time=max_data_start_time,
    )
    response = clients.datasource.search_filtered_channels(clients.auth_header, query)
    for channel_metadata in response.results:
        yield channel_metadata


def export_channels_data(
    clients: ExportClients,
    datasource_rid: str,
    start: str | datetime | IntegralNanosecondsUTC | None = None,
    end: str | datetime | IntegralNanosecondsUTC | None = None,
    channel_exact_match: Sequence[str] = (),
    channel_fuzzy_search_text: str = "",
    tags: dict[str, str] = {},
) -> pd.DataFrame:
    """Export channel data from a datasource and return it as a pandas DataFrame.

    Args:
        clients: Client objects with necessary services
        datasource_rid: The RID of the datasource to export data from
        start: The start time for the data export
            Can be a string (ISO format), datetime, or IntegralNanosecondsUTC
        end: The end time for the data export
            Can be a string (ISO format), datetime, or IntegralNanosecondsUTC
        channel_exact_match: Filter the returned channels to those whose names match all provided strings
            (case insensitive).
        channel_fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.
        tags: Dictionary of tags to filter channels by

    Returns:
        A pandas DataFrame containing the exported channel data
    """


    start_time = _SecondsNanos.from_flexible(start).to_api() if start else _MIN_TIMESTAMP.to_api()
    end_time = _SecondsNanos.from_flexible(end).to_api() if end else _MAX_TIMESTAMP.to_api()
    start_time_scout_api =  _SecondsNanos.from_flexible(start).to_scout_run_api() if start else _MIN_TIMESTAMP.to_scout_run_api()
    end_time_scout_api =  _SecondsNanos.from_flexible(end).to_scout_run_api() if end else _MAX_TIMESTAMP.to_scout_run_api()
    # Get all channels from the datasource
    all_channels = list(get_channels(clients=clients, datasource_rid=datasource_rid, min_data_updated_time=start_time_scout_api, max_data_start_time=end_time_scout_api, exact_match=channel_exact_match, fuzzy_search_text=channel_fuzzy_search_text, tags=tags))
    # Extract channel names from the Channel objects
    channel_names = [channel.name for channel in all_channels]


    # Process channel names in batches of 20
    batch_size = 20
    all_dataframes = []

    for i in range(0, len(channel_names), batch_size):
        batch_channel_names = channel_names[i : i + batch_size]

        # Resolve channel batch
        request = timeseries_logicalseries_api.BatchResolveSeriesRequest(
            requests=[
                timeseries_logicalseries_api.ResolveSeriesRequest(
                    datasource=datasource_rid,
                    name=name,
                    tags=tags,
                )
                for name in batch_channel_names
            ]
        )
        response = clients.logical_series.resolve_batch(clients.auth_header, request)
        batch_channel_info = [
            (s.rid, name) for s, name in zip(response.series, batch_channel_names) if s.rid is not None
        ]

        if not batch_channel_info:
            continue

        total_batches = (len(channel_names) + batch_size - 1) // batch_size
        current_batch = i // batch_size + 1
        percent_complete = current_batch / total_batches * 100
        bar_length = 20
        filled_length = int(bar_length * current_batch // total_batches)
        bar = "█" * filled_length + "░" * (bar_length - filled_length)
        print(
            f"\rExporting data: [{bar}] {percent_complete:.1f}% ({current_batch}/{total_batches} batches)",
            end="",
            flush=True,
        )

        # Create a dictionary mapping RIDs to channel names
        rid_to_name = {rid: name for rid, name in batch_channel_info}

        # Use _get_series_values_csv to get the data

        export_response = _get_series_values_csv(
            clients.auth_header, clients.dataexport, rid_to_name, start_time, end_time
        )

        # Convert the response to a pandas DataFrame and add to list
        batch_df = pd.DataFrame(pd.read_csv(export_response))
        if not batch_df.empty:
            all_dataframes.append(batch_df)

    if not all_dataframes:
        logger.warning(f"No data found for export from datasource {datasource_rid}")
        return pd.DataFrame()

    # Merge all dataframes, keeping the timestamp column from each
    result_df = pd.concat(all_dataframes, axis=0)

    # Sort by timestamp and drop duplicates if needed
    if "timestamp" in result_df.columns:
        result_df = result_df.sort_values("timestamp").reset_index(drop=True)

    print(f"\nExport complete: {len(result_df)} total rows")
    return result_df
