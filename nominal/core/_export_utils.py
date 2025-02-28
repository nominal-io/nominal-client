from __future__ import annotations

import logging
from datetime import datetime
from typing import Protocol

import pandas as pd
from nominal_api import (
    datasource_api,
    scout_compute_api,
    scout_dataexport_api,
    scout_datasource,
    timeseries_logicalseries,
    timeseries_logicalseries_api,
)

from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


class ExportClients(Protocol):
    @property
    def auth_header(self) -> str: ...
    @property
    def datasource(self) -> scout_datasource.DataSourceService: ...
    @property
    def dataexport(self) -> scout_dataexport_api.DataExportService: ...
    @property
    def logical_series(self) -> timeseries_logicalseries.LogicalSeriesService: ...


def export_channels_data(
    clients: ExportClients,
    datasource_rid: str,
    start: str | datetime | IntegralNanosecondsUTC,
    end: str | datetime | IntegralNanosecondsUTC,
    channel_names: list[str] | None = None,
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
        channel_names: List of channel names to export. If None, all channels will be exported
        tags: Dictionary of tags to filter channels by

    Returns:
        A pandas DataFrame containing the exported channel data
    """
    # If no channel names are provided, search for all channels in the datasource
    if not channel_names:
        req = datasource_api.SearchChannelsRequest(
            data_sources=[datasource_rid],
            exact_match=[],
            fuzzy_search_text="",
            previously_selected_channels={},
        )
        resp = clients.datasource.search_channels(clients.auth_header, req)
        channel_names = [r.name for r in resp.results if r.series_rid.series_archetype is not None]

    # Process channel names in batches of 100
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

        # Create export request for this batch
        export_request = scout_dataexport_api.ExportDataRequest(
            channels=scout_dataexport_api.ExportChannels(
                time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                    channels=[
                        scout_dataexport_api.TimeDomainChannel(
                            column_name=name,
                            compute_node=scout_compute_api.Series(raw=scout_compute_api.Reference(name=name)),
                        )
                        for _, name in batch_channel_info
                    ],
                    merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                        none=scout_dataexport_api.NoneStrategy(),
                    ),
                    output_timestamp_format=scout_dataexport_api.TimestampFormat(
                        iso8601=scout_dataexport_api.Iso8601TimestampFormat(),
                    ),
                )
            ),
            start_time=_SecondsNanos.from_flexible(start).to_api(),
            end_time=_SecondsNanos.from_flexible(end).to_api(),
            context=scout_compute_api.Context(
                function_variables={},
                variables={
                    name: scout_compute_api.VariableValue(
                        series=scout_compute_api.SeriesSpec(rid=rid),
                    )
                    for rid, name in batch_channel_info
                },
            ),
            format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
            resolution=scout_dataexport_api.ResolutionOption(
                undecimated=scout_dataexport_api.UndecimatedResolution(),
            ),
        )

        export_response = clients.dataexport.export_channel_data(clients.auth_header, export_request)

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
