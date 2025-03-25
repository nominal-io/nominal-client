import functools
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from nominal_api.timeseries_channelmetadata_api import BatchUpdateChannelMetadataRequest

from nominal.io.core.channel import Channel
from nominal.io.core.dataset import Dataset, DatasetBounds
from nominal.io.core.unit import Unit
from nominal.io.exceptions import NominalIngestError, NominalIngestFailed

UNITS = [
    Unit(name="coulomb", symbol="C"),
    Unit(name="kilograms", symbol="kg"),
    Unit(name="mole", symbol="mol"),
]


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.logical_series = MagicMock()
    return clients


@pytest.fixture
def mock_dataset(mock_clients):
    ds = Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=mock_clients,
    )

    spy = MagicMock(wraps=ds.refresh)
    object.__setattr__(ds, "refresh", spy)
    ds.refresh.return_value = ds

    return ds


@patch("nominal.core.datasource._available_units", return_value=UNITS)
@patch.object(Dataset, "get_channels")
def test_set_channel_units(mock_get_channels: MagicMock, mock_available_units: MagicMock, mock_dataset: Dataset):
    mock_get_channels.return_value = [
        Channel(
            _rid="ch-1",
            name="channel1",
            data_source="ds-1",
            data_type="float",
            unit=None,
            description="Test Channel 1",
            _clients=mock_dataset._clients,
        ),
        Channel(
            _rid="ch-2",
            name="channel2",
            data_source="ds-2",
            data_type="float",
            unit=None,
            description="Test Channel 2",
            _clients=mock_dataset._clients,
        ),
    ]

    channels_to_units = {"channel1": "mol", "channel2": "kg", "channel3": None}
    mock_dataset.set_channel_units(channels_to_units)

    mock_available_units.assert_called_once_with(mock_dataset._clients.auth_header, mock_dataset._clients.units)
    mock_get_channels.assert_called_once()

    batch_request = mock_dataset._clients.channel_metadata.batch_update_channel_metadata.call_args[0][1]
    assert isinstance(batch_request, BatchUpdateChannelMetadataRequest)
    assert len(batch_request.requests) == 2
    assert batch_request.requests[0].channel_identifier.channel_name == "channel1"
    assert batch_request.requests[0].channel_identifier.data_source_rid == "test-rid"
    assert batch_request.requests[1].channel_identifier.channel_name == "channel2"
    assert batch_request.requests[1].channel_identifier.data_source_rid == "test-rid"


@patch("nominal.core.datasource._available_units", return_value=UNITS)
@patch.object(Dataset, "get_channels")
def test_set_channel_units_invalid_unit(
    mock_get_channels: MagicMock, mock_available_units: MagicMock, mock_dataset: Dataset
):
    mock_get_channels.return_value = [
        Channel(
            _rid="ch-1",
            name="channel1",
            data_source="ds-1",
            data_type="float",
            unit=None,
            description="Test Channel 1",
            _clients=mock_dataset._clients,
        ),
    ]

    invalid_channels_to_units = {"channel1": "invalid_unit"}

    with pytest.raises(ValueError, match="Provided unit 'invalid_unit' for channel 'channel1'"):
        mock_dataset.set_channel_units(invalid_channels_to_units)
    mock_available_units.assert_called_once()


@pytest.mark.parametrize("validate", [True, False])
@patch("nominal.core.datasource._available_units", return_value=UNITS)
@patch.object(Dataset, "get_channels")
def test_set_channel_units_no_channel_data(
    mock_get_channels: MagicMock, mock_available_units: MagicMock, mock_dataset: Dataset, validate: bool
):
    mock_get_channels.return_value = []
    channels_to_units = {"channel1": "kg"}
    call = functools.partial(mock_dataset.set_channel_units, channels_to_units, validate_schema=validate)
    if validate:
        with pytest.raises(ValueError, match="Unable to set unit for channel1 to kg: no data uploaded for channel"):
            call()
    else:
        call()
        mock_dataset._clients.logical_series.batch_update_logical_series.assert_not_called()
    mock_available_units.assert_called_once()


@patch("time.sleep", return_value=None)
def test_poll_until_ingestion_completed_success(mock_sleep: MagicMock, mock_dataset: Dataset):
    mock_dataset._clients.catalog.get_ingest_progress_v2.return_value = MagicMock(
        ingest_status=MagicMock(type="success")
    )

    mock_dataset.poll_until_ingestion_completed(interval=timedelta(seconds=1))
    mock_dataset._clients.catalog.get_ingest_progress_v2.assert_called()


@patch("time.sleep", return_value=None)
def test_poll_until_ingestion_completed_in_progress(mock_sleep: MagicMock, mock_dataset: Dataset):
    mock_dataset._clients.catalog.get_ingest_progress_v2.side_effect = [
        MagicMock(ingest_status=MagicMock(type="inProgress")),
        MagicMock(ingest_status=MagicMock(type="inProgress")),
        MagicMock(ingest_status=MagicMock(type="success")),
    ]

    mock_dataset.poll_until_ingestion_completed(interval=timedelta(seconds=1))

    assert mock_dataset._clients.catalog.get_ingest_progress_v2.call_count == 3
    assert mock_sleep.call_count == 2


@patch("time.sleep", return_value=None)
def test_poll_until_ingestion_completed_error(mock_sleep: MagicMock, mock_dataset: Dataset):
    mock_dataset._clients.catalog.get_ingest_progress_v2.side_effect = [
        MagicMock(
            ingest_status=MagicMock(type="error", error=MagicMock(message="Ingest failed", error_type="type_error"))
        ),
        MagicMock(ingest_status=MagicMock(type="inProgress")),
    ]

    with pytest.raises(NominalIngestFailed) as e:
        mock_dataset.poll_until_ingestion_completed(interval=timedelta(seconds=1))
    assert str(e.value) == "ingest failed for dataset 'test-rid': Ingest failed (type_error)"
    mock_sleep.assert_not_called()


@patch("time.sleep", return_value=None)
def test_poll_until_ingestion_completed_error_is_none(mock_sleep: MagicMock, mock_dataset: Dataset):
    mock_dataset._clients.catalog.get_ingest_progress_v2.side_effect = [
        MagicMock(ingest_status=MagicMock(type="error", error=None)),
        MagicMock(ingest_status=MagicMock(type="inProgress")),
    ]

    with pytest.raises(NominalIngestError) as e:
        mock_dataset.poll_until_ingestion_completed(interval=timedelta(seconds=1))
    assert str(e.value) == "ingest status type marked as 'error' but with no instance for dataset 'test-rid'"
    mock_sleep.assert_not_called()


@patch("time.sleep", return_value=None)
def test_poll_until_ingestion_completed_unknown_status(mock_sleep: MagicMock, mock_dataset: Dataset):
    mock_dataset._clients.catalog.get_ingest_progress_v2.return_value = MagicMock(
        ingest_status=MagicMock(type="unknown_status")
    )

    with pytest.raises(NominalIngestError, match="unhandled ingest status 'unknown_status' for dataset 'test-rid'"):
        mock_dataset.poll_until_ingestion_completed(interval=timedelta(seconds=1))
    mock_sleep.assert_not_called()
