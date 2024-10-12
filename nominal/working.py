import certifi
import pandas as pd
from conjure_python_client import RequestsClient, ServiceConfiguration, SslConfiguration

from nominal import _config as auth_config
from nominal._api.combined import (
    api,
    scout_compute_api,
    scout_dataexport_api,
    timeseries_logicalseries,
    timeseries_logicalseries_api,
)
from nominal.core._utils import construct_user_agent_string
from nominal.nominal import _DEFAULT_BASE_URL as API_URL


def _get_channel_rid(
    client: timeseries_logicalseries.LogicalSeriesService, bearer_token: str, rid: str, name: str
) -> str:
    """
    rid: datasource rid
    name: series (column) name
    """
    request = timeseries_logicalseries_api.BatchResolveSeriesRequest(
        requests=[
            timeseries_logicalseries_api.ResolveSeriesRequest(
                datasource=rid,
                name=name,
                tags={},
            )
        ]
    )
    response = client.resolve_batch(bearer_token, request)
    if len(response.series) == 0:
        raise ValueError("no series found")
    elif len(response.series) > 1:
        raise ValueError("multiple series found")
    series = response.series[0]
    ## optional bc union
    if series.rid is None:
        raise ValueError("rid is None")
    return series.rid


def get_channel(rid: str, name: str) -> pd.DataFrame:
    """Get channel data for a dataset.

    Args:
        rid (str): The dataset rid where the channel exists
        name (str): The name of the channel

    Returns:
        (pd.DataFrame): Dataframe of the timestamp of the channel (column 'timestamp') and the value (column name matching the channel name)
    """
    trust_store_path = certifi.where()
    cfg = ServiceConfiguration(
        uris=["https://api.gov.nominal.io/api"], security=SslConfiguration(trust_store_path=trust_store_path)
    )
    agent = construct_user_agent_string()
    logicalseries_client = RequestsClient.create(timeseries_logicalseries.LogicalSeriesService, agent, cfg)
    export_client = RequestsClient.create(scout_dataexport_api.DataExportService, agent, cfg)
    bearer_token = auth_config.get_token(API_URL)
    channel_rid = _get_channel_rid(logicalseries_client, bearer_token, rid, name)
    request = scout_dataexport_api.ExportDataRequest(
        channels=scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=[
                    scout_dataexport_api.TimeDomainChannel(
                        column_name=name,
                        compute_node=scout_compute_api.ComputeNode(  # type: ignore
                            raw=scout_compute_api.RawUntypedSeriesNode(name=name)
                        ),
                    ),
                ],
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    none=scout_dataexport_api.NoneStrategy(),
                    # all_timestamps_forward_fill=scout_dataexport_api.AllTimestampsForwardFillStrategy(
                    #     look_back_period=scout_run_api.Duration(seconds=0, nanos=0)
                    # ),
                ),
                output_timestamp_format=scout_dataexport_api.TimestampFormat(
                    iso8601=scout_dataexport_api.Iso8601TimestampFormat()
                ),
            )
        ),
        start_time=api.Timestamp(seconds=0, nanos=0),
        # long max is 9,223,372,036,854,775,807, backend converts to long nanoseconds, so this is the last valid timestamp
        # that can be represented in the API. (2262-04-11 19:47:16.854775807)
        end_time=api.Timestamp(seconds=9223372036, nanos=854775807),
        context=scout_compute_api.Context(
            function_variables={},
            variables={
                name: scout_compute_api.VariableValue(
                    series=scout_compute_api.SeriesSpec(rid=channel_rid),
                )
            },
        ),
        format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
        resolution=scout_dataexport_api.ResolutionOption(
            undecimated=scout_dataexport_api.UndecimatedResolution(),
        ),
        # compression=scout_dataexport_api.CompressionFormat.GZIP,
    )
    return pd.read_csv(export_client.export_channel_data(bearer_token, request))
