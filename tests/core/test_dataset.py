from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.unit import Unit
from nominal.exceptions import NominalIngestError, NominalIngestFailed

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
