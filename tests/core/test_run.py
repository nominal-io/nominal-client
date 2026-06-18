from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core.connection import Connection
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.run import Run
from nominal.core.video import Video

SCOPE_NAME = "test-scope"


@pytest.fixture
def mock_clients():
    return MagicMock()


@pytest.fixture
def mock_run(mock_clients):
    return Run(
        rid="run-rid-1",
        name="Test Run",
        description="",
        properties={},
        labels=[],
        links=[],
        start=0,
        end=None,
        run_number=1,
        assets=[],
        created_at=0,
        _clients=mock_clients,
    )


@pytest.fixture
def mock_dataset(mock_clients):
    return Dataset(
        rid="dataset-rid-1",
        name="Test Dataset",
        description=None,
        bounds=DatasetBounds(start=0, end=1),
        properties={},
        labels=[],
        _clients=mock_clients,
    )


@pytest.fixture
def mock_connection(mock_clients):
    return Connection(
        rid="connection-rid-1",
        name="Test Connection",
        description=None,
        _clients=mock_clients,
    )


@pytest.fixture
def mock_video(mock_clients):
    return Video(
        rid="video-rid-1",
        name="Test Video",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        _clients=mock_clients,
    )


def test_get_dataset_fetches_scoped_dataset(mock_run, mock_clients, mock_dataset):
    """A dataset ref name is resolved to one backing dataset fetch."""
    raw_dataset = MagicMock()

    with (
        patch.object(Run, "_list_datasource_rids", return_value={SCOPE_NAME: "dataset-rid-1"}) as list_rids,
        patch("nominal.core.run._get_dataset", return_value=raw_dataset) as get_dataset,
        patch.object(Dataset, "_from_conjure", return_value=mock_dataset) as from_conjure,
    ):
        result = mock_run.get_dataset(SCOPE_NAME)

    assert result == mock_dataset
    list_rids.assert_called_once_with("dataset")
    get_dataset.assert_called_once_with(mock_clients.auth_header, mock_clients.catalog, "dataset-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_dataset)


def test_get_connection_fetches_scoped_connection(mock_run, mock_clients, mock_connection):
    """A connection ref name is resolved to one backing connection fetch."""
    raw_connection = MagicMock()
    mock_clients.connection.get_connection.return_value = raw_connection

    with (
        patch.object(Run, "_list_datasource_rids", return_value={SCOPE_NAME: "connection-rid-1"}) as list_rids,
        patch.object(Connection, "_from_conjure", return_value=mock_connection) as from_conjure,
    ):
        result = mock_run.get_connection(SCOPE_NAME)

    assert result == mock_connection
    list_rids.assert_called_once_with("connection")
    mock_clients.connection.get_connection.assert_called_once_with(mock_clients.auth_header, "connection-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_connection)


def test_get_video_fetches_scoped_video(mock_run, mock_clients, mock_video):
    """A video ref name is resolved to one backing video fetch."""
    raw_video = MagicMock()

    with (
        patch.object(Run, "_list_datasource_rids", return_value={SCOPE_NAME: "video-rid-1"}) as list_rids,
        patch("nominal.core.run._get_video", return_value=raw_video) as get_video,
        patch.object(Video, "_from_conjure", return_value=mock_video) as from_conjure,
    ):
        result = mock_run.get_video(SCOPE_NAME)

    assert result == mock_video
    list_rids.assert_called_once_with("video")
    get_video.assert_called_once_with(mock_clients, "video-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_video)


def test_get_dataset_raises_value_error_when_dataset_scope_is_missing(mock_run):
    """A missing typed dataset ref name does not fetch a backing dataset."""
    with (
        patch.object(Run, "_list_datasource_rids", return_value={}) as list_rids,
        patch("nominal.core.run._get_dataset") as get_dataset,
        pytest.raises(ValueError, match=f"No dataset with ref name '{SCOPE_NAME}' found for this run"),
    ):
        mock_run.get_dataset(SCOPE_NAME)

    list_rids.assert_called_once_with("dataset")
    get_dataset.assert_not_called()


def test_get_connection_raises_value_error_when_connection_scope_is_missing(mock_run, mock_clients):
    """A missing typed connection ref name does not fetch a backing connection."""
    with (
        patch.object(Run, "_list_datasource_rids", return_value={}) as list_rids,
        patch.object(Connection, "_from_conjure") as from_conjure,
        pytest.raises(ValueError, match=f"No connection with ref name '{SCOPE_NAME}' found for this run"),
    ):
        mock_run.get_connection(SCOPE_NAME)

    list_rids.assert_called_once_with("connection")
    mock_clients.connection.get_connection.assert_not_called()
    from_conjure.assert_not_called()


def test_get_video_raises_value_error_when_video_scope_is_missing(mock_run):
    """A missing typed video ref name does not fetch a backing video."""
    with (
        patch.object(Run, "_list_datasource_rids", return_value={}) as list_rids,
        patch("nominal.core.run._get_video") as get_video,
        pytest.raises(ValueError, match=f"No video with ref name '{SCOPE_NAME}' found for this run"),
    ):
        mock_run.get_video(SCOPE_NAME)

    list_rids.assert_called_once_with("video")
    get_video.assert_not_called()


def test_get_dataset_propagates_backing_dataset_error(mock_run):
    """If the ref exists but the dataset fetch fails, the fetch error is surfaced."""
    error = ValueError("dataset 'stale-dataset-rid' not found")

    with (
        patch.object(Run, "_list_datasource_rids", return_value={SCOPE_NAME: "stale-dataset-rid"}),
        patch("nominal.core.run._get_dataset", side_effect=error),
        patch.object(Dataset, "_from_conjure") as from_conjure,
        pytest.raises(ValueError) as exc_info,
    ):
        mock_run.get_dataset(SCOPE_NAME)

    assert exc_info.value is error
    from_conjure.assert_not_called()


def test_get_connection_propagates_backing_connection_error(mock_run, mock_clients):
    """If the ref exists but the connection fetch fails, the fetch error is surfaced."""
    error = RuntimeError("connection 'stale-connection-rid' not found")
    mock_clients.connection.get_connection.side_effect = error

    with (
        patch.object(Run, "_list_datasource_rids", return_value={SCOPE_NAME: "stale-connection-rid"}),
        patch.object(Connection, "_from_conjure") as from_conjure,
        pytest.raises(RuntimeError) as exc_info,
    ):
        mock_run.get_connection(SCOPE_NAME)

    assert exc_info.value is error
    from_conjure.assert_not_called()


def test_get_video_propagates_backing_video_error(mock_run):
    """If the ref exists but the video fetch fails, the fetch error is surfaced."""
    error = RuntimeError("video 'stale-video-rid' not found")

    with (
        patch.object(Run, "_list_datasource_rids", return_value={SCOPE_NAME: "stale-video-rid"}),
        patch("nominal.core.run._get_video", side_effect=error),
        patch.object(Video, "_from_conjure") as from_conjure,
        pytest.raises(RuntimeError) as exc_info,
    ):
        mock_run.get_video(SCOPE_NAME)

    assert exc_info.value is error
    from_conjure.assert_not_called()
