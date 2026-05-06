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


def test_unknown_datascope_raises(mock_run: Run, mock_dataset: Dataset, patched_export: MagicMock):
    """An unknown datascope ref_name raises ValueError before any export happens."""
    with patch.object(Run, "list_datasets", return_value=[("primary", mock_dataset)]):
        with pytest.raises(ValueError, match="does not have datascope.*nonexistent"):
            pandas_module.run_to_dataframe(mock_run, datascopes=["primary", "nonexistent"])

    patched_export.assert_not_called()


def test_multi_asset_run_raises(mock_run: Run):
    """A run with more than one asset raises RuntimeError."""
    multi_asset_run = dataclasses.replace(mock_run, assets=("asset-rid-1", "asset-rid-2"))

    with pytest.raises(RuntimeError, match="only supports single-asset"):
        pandas_module.run_to_dataframe(multi_asset_run)
