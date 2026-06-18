from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.connection import Connection
from nominal.core.dataset import Dataset
from nominal.core.video import Video

SCOPE_NAME = "test-scope"


def _run_source(source_type: str, rid: str):
    data_source = SimpleNamespace(type=source_type)
    setattr(data_source, source_type, rid)
    return SimpleNamespace(data_source=data_source)


def _set_run_sources(mock_clients, *sources):
    mock_clients.run.get_run.return_value = SimpleNamespace(data_sources=dict(sources))


def _assert_run_fetched(mock_clients, run_rid: str):
    mock_clients.run.get_run.assert_called_once_with(mock_clients.auth_header, run_rid)


def _assert_dataset_fetched(mock_clients, dataset_rid: str):
    mock_clients.catalog.get_enriched_datasets.assert_called_once()
    args = mock_clients.catalog.get_enriched_datasets.call_args.args
    assert args[0] == mock_clients.auth_header
    assert args[1].dataset_rids == [dataset_rid]


def test_get_dataset_fetches_scoped_dataset(mock_run, mock_clients, mock_dataset):
    """A dataset ref name is resolved to one backing dataset fetch."""
    raw_dataset = MagicMock()
    mock_clients.catalog.get_enriched_datasets.return_value = [raw_dataset]
    _set_run_sources(mock_clients, (SCOPE_NAME, _run_source("dataset", "dataset-rid-1")))

    with patch.object(Dataset, "_from_conjure", return_value=mock_dataset) as from_conjure:
        result = mock_run.get_dataset(SCOPE_NAME)

    assert result == mock_dataset
    _assert_run_fetched(mock_clients, mock_run.rid)
    _assert_dataset_fetched(mock_clients, "dataset-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_dataset)


def test_get_connection_fetches_scoped_connection(mock_run, mock_clients, mock_connection):
    """A connection ref name is resolved to one backing connection fetch."""
    raw_connection = MagicMock()
    mock_clients.connection.get_connection.return_value = raw_connection
    _set_run_sources(mock_clients, (SCOPE_NAME, _run_source("connection", "connection-rid-1")))

    with patch.object(Connection, "_from_conjure", return_value=mock_connection) as from_conjure:
        result = mock_run.get_connection(SCOPE_NAME)

    assert result == mock_connection
    _assert_run_fetched(mock_clients, mock_run.rid)
    mock_clients.connection.get_connection.assert_called_once_with(mock_clients.auth_header, "connection-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_connection)


def test_get_video_fetches_scoped_video(mock_run, mock_clients, mock_video):
    """A video ref name is resolved to one backing video fetch."""
    raw_video = MagicMock()
    mock_clients.video.get.return_value = raw_video
    _set_run_sources(mock_clients, (SCOPE_NAME, _run_source("video", "video-rid-1")))

    with patch.object(Video, "_from_conjure", return_value=mock_video) as from_conjure:
        result = mock_run.get_video(SCOPE_NAME)

    assert result == mock_video
    _assert_run_fetched(mock_clients, mock_run.rid)
    mock_clients.video.get.assert_called_once_with(mock_clients.auth_header, "video-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_video)


def test_get_dataset_raises_value_error_when_dataset_scope_is_missing(mock_run, mock_clients):
    """A missing typed dataset ref name does not fetch a backing dataset."""
    _set_run_sources(mock_clients)

    with pytest.raises(ValueError, match=f"No dataset with ref name '{SCOPE_NAME}' found for this run"):
        mock_run.get_dataset(SCOPE_NAME)

    _assert_run_fetched(mock_clients, mock_run.rid)
    mock_clients.catalog.get_enriched_datasets.assert_not_called()


def test_get_connection_raises_value_error_when_connection_scope_is_missing(mock_run, mock_clients):
    """A missing typed connection ref name does not fetch a backing connection."""
    _set_run_sources(mock_clients)

    with pytest.raises(ValueError, match=f"No connection with ref name '{SCOPE_NAME}' found for this run"):
        mock_run.get_connection(SCOPE_NAME)

    _assert_run_fetched(mock_clients, mock_run.rid)
    mock_clients.connection.get_connection.assert_not_called()


def test_get_video_raises_value_error_when_video_scope_is_missing(mock_run, mock_clients):
    """A missing typed video ref name does not fetch a backing video."""
    _set_run_sources(mock_clients)

    with pytest.raises(ValueError, match=f"No video with ref name '{SCOPE_NAME}' found for this run"):
        mock_run.get_video(SCOPE_NAME)

    _assert_run_fetched(mock_clients, mock_run.rid)
    mock_clients.video.get.assert_not_called()


def test_get_dataset_propagates_backing_dataset_error(mock_run, mock_clients):
    """If the ref exists but the dataset fetch fails, the fetch error is surfaced."""
    error = ValueError("dataset 'stale-dataset-rid' not found")
    mock_clients.catalog.get_enriched_datasets.side_effect = error
    _set_run_sources(mock_clients, (SCOPE_NAME, _run_source("dataset", "stale-dataset-rid")))

    with pytest.raises(ValueError) as exc_info:
        mock_run.get_dataset(SCOPE_NAME)

    assert exc_info.value is error
    _assert_run_fetched(mock_clients, mock_run.rid)
    _assert_dataset_fetched(mock_clients, "stale-dataset-rid")


def test_get_connection_propagates_backing_connection_error(mock_run, mock_clients):
    """If the ref exists but the connection fetch fails, the fetch error is surfaced."""
    error = RuntimeError("connection 'stale-connection-rid' not found")
    mock_clients.connection.get_connection.side_effect = error
    _set_run_sources(mock_clients, (SCOPE_NAME, _run_source("connection", "stale-connection-rid")))

    with pytest.raises(RuntimeError) as exc_info:
        mock_run.get_connection(SCOPE_NAME)

    assert exc_info.value is error
    _assert_run_fetched(mock_clients, mock_run.rid)
    mock_clients.connection.get_connection.assert_called_once_with(mock_clients.auth_header, "stale-connection-rid")


def test_get_video_propagates_backing_video_error(mock_run, mock_clients):
    """If the ref exists but the video fetch fails, the fetch error is surfaced."""
    error = RuntimeError("video 'stale-video-rid' not found")
    mock_clients.video.get.side_effect = error
    _set_run_sources(mock_clients, (SCOPE_NAME, _run_source("video", "stale-video-rid")))

    with pytest.raises(RuntimeError) as exc_info:
        mock_run.get_video(SCOPE_NAME)

    assert exc_info.value is error
    _assert_run_fetched(mock_clients, mock_run.rid)
    mock_clients.video.get.assert_called_once_with(mock_clients.auth_header, "stale-video-rid")
