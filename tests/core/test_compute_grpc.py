from unittest.mock import MagicMock

import pandas as pd
import pytest

from nominal.core.channel import Channel, ChannelDataType
from nominal.protos.compute.v1 import (
    categorical_results_pb2,
    compute_pb2,
    execution_options_pb2,
    execution_results_pb2,
    numeric_results_pb2,
    result_common_pb2,
)
from nominal.protos.types.time import time_pb2
from nominal.thirdparty.pandas._pandas import channel_to_dataframe_decimated


def test_log_search_follows_nested_cursor_and_preserves_attributes():
    """Log searches follow nested paging tokens while retaining timestamps and attributes."""
    clients = MagicMock()
    channel = Channel(
        name="logs", data_source="dataset", data_type=ChannelDataType.LOG, unit=None, description=None, _clients=clients
    )
    clients.compute.Compute.side_effect = [
        compute_pb2.ComputeResponse(
            result=execution_results_pb2.ComputationResult(
                log_samples=categorical_results_pb2.LogSamples(
                    samples=[
                        result_common_pb2.LogSample(
                            timestamp=time_pb2.Timestamp(seconds=-1, nanos=999999999),
                            message="first",
                            attributes={"level": "info"},
                        )
                    ],
                    next_page_token="next",
                )
            )
        ),
        compute_pb2.ComputeResponse(
            result=execution_results_pb2.ComputationResult(
                log_samples=categorical_results_pb2.LogSamples(
                    samples=[result_common_pb2.LogSample(timestamp=time_pb2.Timestamp(nanos=1), message="last")]
                )
            )
        ),
    ]
    points = list(channel.search_logs(regex_match="first|last", tags={"host": "one"}, start=-1, end=2))
    assert [(point.timestamp, point.message, dict(point.args)) for point in points] == [
        (-1, "first", {"level": "info"}),
        (1, "last", {}),
    ]
    first, second = [call.args[0].input for call in clients.compute.Compute.call_args_list]
    assert first.computation.series_summary.input.log.filter.predicate.message_regex == "first|last"
    assert (
        first.computation.series_summary.summarization.point_page.direction
        == execution_options_pb2.COMPUTE_TIME_DIRECTION_ASCENDING
    )
    assert second.computation.series_summary.summarization.point_page.page_token == "next"
    assert (
        second.computation.series_summary.input.log.filter.input.channel_selection.data_source.tags.exact_values.values[
            "host"
        ].literal
        == "one"
    )


def test_pandas_raw_summary_preserves_value_schema_and_nanoseconds():
    """Undecimated summaries retain the value column and UTC nanosecond index."""
    clients = MagicMock()
    channel = Channel(
        name="speed",
        data_source="dataset",
        data_type=ChannelDataType.DOUBLE,
        unit=None,
        description=None,
        _clients=clients,
    )
    result = execution_results_pb2.ComputationResult(
        numeric_samples=numeric_results_pb2.NumericSamples(
            samples=[
                result_common_pb2.NumericSample(timestamp=time_pb2.Timestamp(seconds=-1, nanos=999999999), value=3)
            ]
        )
    )
    clients.compute.Compute.return_value = compute_pb2.ComputeResponse(result=result)

    frame = channel_to_dataframe_decimated(channel, -1, 2, buckets=1)

    assert frame.index.tolist() == [pd.Timestamp(-1, unit="ns", tz="UTC")]
    assert frame.index.name == "timestamp"
    assert list(frame.columns) == ["value"]
    assert frame.iloc[0]["value"] == 3


def test_pandas_bucketed_summary_preserves_missing_statistics():
    """Bucketed summaries retain zero counts and distinguish missing statistics."""
    clients = MagicMock()
    channel = Channel(
        name="speed",
        data_source="dataset",
        data_type=ChannelDataType.DOUBLE,
        unit=None,
        description=None,
        _clients=clients,
    )
    result = execution_results_pb2.ComputationResult(
        numeric_buckets=numeric_results_pb2.NumericBuckets(
            buckets=[
                numeric_results_pb2.NumericResultBucket(
                    end=time_pb2.Timestamp(seconds=-1, nanos=999999999),
                    statistics=numeric_results_pb2.NumericBucketStatistics(min=0, max=3, count=0),
                )
            ]
        )
    )
    clients.compute.Compute.return_value = compute_pb2.ComputeResponse(result=result)

    frame = channel_to_dataframe_decimated(channel, -1, 2, buckets=1)

    assert frame.index.tolist() == [pd.Timestamp(-1, unit="ns", tz="UTC")]
    assert frame.index.name == "timestamp"
    assert list(frame.columns) == ["min", "max", "mean", "count", "variance"]
    assert frame.iloc[0]["min"] == 0
    assert frame.iloc[0]["count"] == 0
    assert pd.isna(frame.iloc[0]["mean"])


@pytest.mark.parametrize("tags", [None, {}])
def test_untagged_channel_omits_tag_constraints(tags):
    """The channel serializer omits unspecified and explicitly empty tag maps."""
    channel = Channel(
        name="value",
        data_source="dataset",
        data_type=ChannelDataType.DOUBLE,
        unit=None,
        description=None,
        _clients=MagicMock(),
    )
    selection = channel._to_proto_compute_series(tags).numeric.channel_selection.data_source
    assert not selection.HasField("tags")
