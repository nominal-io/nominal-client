from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from nominal.thirdparty.pandas import datasource_to_dataframe


@pytest.mark.parametrize("channel_count", [1, 2])
def test_failed_batch_raises_instead_of_returning_empty_or_partial_data(mock_clients, make_channel, channel_count):
    channels = [make_channel(name) for name in ("temperature", "humidity")[:channel_count]]
    failure = requests.HTTPError("export rejected")

    def export(_auth, request):
        name = request.channels.time_domain.channels[0].column_name
        if name == "temperature":
            raise failure
        return BytesIO(b"timestamp,humidity\n2024-09-05T18:00:00Z,50.0\n")

    mock_clients.dataexport.export_channel_data.side_effect = export
    datasource = MagicMock(_clients=mock_clients)

    with pytest.raises(requests.HTTPError) as raised:
        datasource_to_dataframe(datasource, channels=channels, channel_batch_size=1, num_workers=2, enable_gzip=False)
    assert raised.value is failure


def test_successful_empty_export_returns_expected_columns(mock_clients, make_channel):
    mock_clients.dataexport.export_channel_data.return_value = BytesIO(b"timestamp,temperature\n")
    result = datasource_to_dataframe(
        MagicMock(_clients=mock_clients), channels=[make_channel("temperature")], enable_gzip=False
    )
    assert result.empty
    assert list(result.columns) == ["temperature"]
    assert result.index.name == "timestamp"


def test_successful_batches_are_joined_by_timestamp(mock_clients, make_channel):
    def export(_auth, request):
        name = request.channels.time_domain.channels[0].column_name
        value = 20.0 if name == "temperature" else 50.0
        return BytesIO(f"timestamp,{name}\n2024-09-05T18:00:00Z,{value}\n".encode())

    mock_clients.dataexport.export_channel_data.side_effect = export
    result = datasource_to_dataframe(
        MagicMock(_clients=mock_clients),
        channels=[make_channel("temperature"), make_channel("humidity")],
        channel_batch_size=1,
        num_workers=2,
        enable_gzip=False,
    )
    expected = pd.DataFrame(
        {"temperature": [20.0], "humidity": [50.0]},
        index=pd.DatetimeIndex(["2024-09-05T18:00:00Z"], name="timestamp"),
    )
    pd.testing.assert_frame_equal(result.reindex(columns=expected.columns), expected)
