from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from nominal.core.dataset import Dataset
from nominal.core.run import Run
from nominal.thirdparty.pandas import _pandas as pandas_module


def _make_dataset(clients: MagicMock, rid: str) -> Dataset:
    return Dataset(
        rid=rid,
        name=rid,
        description=None,
        bounds=None,
        properties={},
        labels=[],
        _clients=clients,
    )


@pytest.fixture
def mock_clients() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_dataset(mock_clients: MagicMock) -> Dataset:
    return _make_dataset(mock_clients, "dataset-rid-1")


@pytest.fixture
def mock_run(mock_clients: MagicMock) -> Run:
    return Run(
        rid="run-rid-1",
        name="Test Run",
        description="",
        properties={},
        labels=(),
        links=(),
        start=1_000,
        end=2_000,
        run_number=1,
        assets=(),
        created_at=0,
        _clients=mock_clients,
    )


@pytest.fixture
def patched_export():
    """datasource_to_dataframe is patched to return a uniquely-identifiable DataFrame per call."""
    with patch.object(pandas_module, "datasource_to_dataframe") as mock:
        mock.side_effect = lambda dataset, **kwargs: pd.DataFrame({"rid": [dataset.rid]})
        yield mock


def test_default_downloads_all_run_datasets(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """With no filters, every datascope on the run is downloaded scoped to the run's start/end."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2")

    with patch.object(Run, "list_datasets", return_value=[("primary", ds_1), ("secondary", ds_2)]):
        result = pandas_module.run_to_dataframe(mock_run)

    assert set(result.keys()) == {"primary", "secondary"}
    assert patched_export.call_count == 2
    for call in patched_export.call_args_list:
        assert call.kwargs["start"] == 1_000
        assert call.kwargs["end"] == 2_000


def test_open_run_forwards_none_end(mock_run: Run, mock_dataset: Dataset, patched_export: MagicMock):
    """When the run has no end (open run), end=None is forwarded to datasource_to_dataframe."""
    open_run = dataclasses.replace(mock_run, end=None)

    with patch.object(Run, "list_datasets", return_value=[("primary", mock_dataset)]):
        pandas_module.run_to_dataframe(open_run)

    assert patched_export.call_args.kwargs["end"] is None


def test_datascope_filter_downloads_only_matched(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """A datascopes filter restricts the download to the matching ref_names."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2")

    with patch.object(Run, "list_datasets", return_value=[("primary", ds_1), ("secondary", ds_2)]):
        result = pandas_module.run_to_dataframe(mock_run, datascopes=["secondary"])

    assert set(result.keys()) == {"secondary"}


def test_unknown_datascope_warns_and_skips(
    mock_run: Run, mock_dataset: Dataset, patched_export: MagicMock, caplog: pytest.LogCaptureFixture
):
    """An unknown datascope ref_name is skipped with a warning; valid ref_names still download."""
    with (
        patch.object(Run, "list_datasets", return_value=[("primary", mock_dataset)]),
        caplog.at_level("WARNING", logger=pandas_module.__name__),
    ):
        result = pandas_module.run_to_dataframe(mock_run, datascopes=["primary", "nonexistent"])

    assert set(result.keys()) == {"primary"}
    assert any("does not have a datascope" in r.message and "'nonexistent'" in r.message for r in caplog.records)


def test_multi_asset_run_raises(mock_run: Run):
    """A run with more than one asset raises RuntimeError."""
    multi_asset_run = dataclasses.replace(mock_run, assets=("asset-rid-1", "asset-rid-2"))

    with pytest.raises(RuntimeError, match="only supports single-asset"):
        pandas_module.run_to_dataframe(multi_asset_run)


def test_channel_and_export_options_forwarded(mock_run: Run, mock_dataset: Dataset, patched_export: MagicMock):
    """Channel/export options are forwarded verbatim, and gzip is always enabled."""
    with patch.object(Run, "list_datasets", return_value=[("primary", mock_dataset)]):
        pandas_module.run_to_dataframe(
            mock_run,
            channel_exact_match=["engine", "rpm"],
            num_workers=4,
            channel_batch_size=10,
        )

    kwargs = patched_export.call_args.kwargs
    assert kwargs["channel_exact_match"] == ["engine", "rpm"]
    assert kwargs["num_workers"] == 4
    assert kwargs["channel_batch_size"] == 10
    assert kwargs["enable_gzip"] is True


def test_run_method_delegates_to_run_to_dataframe(mock_run: Run):
    """Run.to_dataframe forwards every kwarg verbatim to run_to_dataframe."""
    expected: dict[str, pd.DataFrame] = {}
    with patch("nominal.thirdparty.pandas._pandas.run_to_dataframe", return_value=expected) as mock:
        result = mock_run.to_dataframe(
            datascopes=["primary"],
            channel_exact_match=["x"],
            num_workers=2,
            channel_batch_size=15,
        )

    assert result is expected
    mock.assert_called_once_with(
        mock_run,
        datascopes=["primary"],
        channel_exact_match=["x"],
        num_workers=2,
        channel_batch_size=15,
    )
