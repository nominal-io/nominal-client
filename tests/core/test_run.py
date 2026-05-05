from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from nominal.core.asset import Asset
from nominal.core.dataset import Dataset
from nominal.core.run import Run
from nominal.thirdparty.pandas import _pandas as pandas_module


def _make_dataset(
    clients: MagicMock,
    rid: str,
    *,
    labels: list[str] | None = None,
    properties: dict[str, str] | None = None,
) -> Dataset:
    return Dataset(
        rid=rid,
        name=rid,
        description=None,
        bounds=None,
        properties=properties or {},
        labels=labels or [],
        _clients=clients,
    )


def _make_asset(clients: MagicMock, rid: str) -> Asset:
    return Asset(
        rid=rid,
        name=rid,
        description=None,
        properties={},
        labels=[],
        created_at=0,
        _clients=clients,
    )


@pytest.fixture
def mock_clients() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_dataset(mock_clients: MagicMock) -> Dataset:
    return _make_dataset(mock_clients, "dataset-rid-1")


@pytest.fixture
def mock_asset(mock_clients: MagicMock) -> Asset:
    return _make_asset(mock_clients, "asset-rid-1")


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
    """With no filters, every dataset on the run is downloaded scoped to the run's start/end."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2")

    with patch.object(Run, "list_datasets", return_value=[("primary", ds_1), ("secondary", ds_2)]):
        result = pandas_module.run_to_dataframe(mock_run)

    assert set(result.keys()) == {"dataset-rid-1", "dataset-rid-2"}
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


def test_dataset_filter_downloads_only_matched(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """A Dataset filter restricts the download to that single dataset."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2")

    with patch.object(Run, "list_datasets", return_value=[("primary", ds_1), ("secondary", ds_2)]):
        result = pandas_module.run_to_dataframe(mock_run, data_filters=ds_2)

    assert set(result.keys()) == {"dataset-rid-2"}


def test_dataset_not_in_run_warns_and_skips(
    mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock, caplog: pytest.LogCaptureFixture
):
    """A Dataset that's not on the run is skipped with a warning; valid filter items still download."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    stranger = _make_dataset(mock_clients, "dataset-rid-stranger")

    with (
        patch.object(Run, "list_datasets", return_value=[("primary", ds_1)]),
        caplog.at_level("WARNING", logger=pandas_module.__name__),
    ):
        result = pandas_module.run_to_dataframe(mock_run, data_filters=[stranger, ds_1])

    assert set(result.keys()) == {"dataset-rid-1"}
    assert any("does not have a dataset" in r.message and "dataset-rid-stranger" in r.message for r in caplog.records)


def test_asset_filter_includes_all_asset_datasets(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """An Asset filter expands to every dataset within the asset, even ones not directly on the run."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2")
    asset = _make_asset(mock_clients, "asset-rid-1")
    run = dataclasses.replace(mock_run, assets=("asset-rid-1",))

    with (
        patch.object(Run, "list_datasets", return_value=[("primary", ds_1)]),
        patch.object(Asset, "list_datasets", return_value=[("from_asset_a", ds_1), ("from_asset_b", ds_2)]),
    ):
        result = pandas_module.run_to_dataframe(run, data_filters=asset)

    assert set(result.keys()) == {"dataset-rid-1", "dataset-rid-2"}


def test_asset_not_in_run_warns_and_skips(
    mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock, caplog: pytest.LogCaptureFixture
):
    """An Asset whose RID isn't in run.assets is skipped with a warning; valid items still download."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    stranger_asset = _make_asset(mock_clients, "asset-rid-stranger")

    with (
        patch.object(Run, "list_datasets", return_value=[("primary", ds_1)]),
        caplog.at_level("WARNING", logger=pandas_module.__name__),
    ):
        result = pandas_module.run_to_dataframe(mock_run, data_filters=[stranger_asset, ds_1])

    assert set(result.keys()) == {"dataset-rid-1"}
    assert any("does not have an asset" in r.message and "asset-rid-stranger" in r.message for r in caplog.records)


def test_filter_sequence_unions_and_dedupes_by_rid(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """A mixed sequence of filters takes the union; datasets matched twice are downloaded once."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1")
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2")
    asset = _make_asset(mock_clients, "asset-rid-1")
    run = dataclasses.replace(mock_run, assets=("asset-rid-1",))

    with (
        patch.object(Run, "list_datasets", return_value=[("primary", ds_1), ("secondary", ds_2)]),
        patch.object(Asset, "list_datasets", return_value=[("a", ds_1), ("b", ds_2)]),
    ):
        result = pandas_module.run_to_dataframe(run, data_filters=[ds_1, asset])

    assert set(result.keys()) == {"dataset-rid-1", "dataset-rid-2"}
    assert patched_export.call_count == 2


def test_labels_filter_requires_all_labels(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """Only datasets whose labels are a superset of the requested labels are kept."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1", labels=["env-prod", "team-a"])
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2", labels=["env-prod"])
    ds_3 = _make_dataset(mock_clients, "dataset-rid-3", labels=["env-dev", "team-a"])

    with patch.object(Run, "list_datasets", return_value=[("a", ds_1), ("b", ds_2), ("c", ds_3)]):
        result = pandas_module.run_to_dataframe(mock_run, labels=["env-prod", "team-a"])

    assert set(result.keys()) == {"dataset-rid-1"}


def test_properties_filter_requires_all_kv_pairs(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """Only datasets whose properties contain every requested key/value pair are kept."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1", properties={"environment": "prod", "owner": "team-a"})
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2", properties={"environment": "prod"})
    ds_3 = _make_dataset(mock_clients, "dataset-rid-3", properties={"environment": "dev", "owner": "team-a"})

    with patch.object(Run, "list_datasets", return_value=[("a", ds_1), ("b", ds_2), ("c", ds_3)]):
        result = pandas_module.run_to_dataframe(mock_run, properties={"environment": "prod", "owner": "team-a"})

    assert set(result.keys()) == {"dataset-rid-1"}


def test_labels_and_properties_combined(mock_run: Run, mock_clients: MagicMock, patched_export: MagicMock):
    """When both filters are given, only datasets matching both are kept (intersection)."""
    ds_1 = _make_dataset(mock_clients, "dataset-rid-1", labels=["foo"], properties={"k": "v"})
    ds_2 = _make_dataset(mock_clients, "dataset-rid-2", labels=["foo"], properties={"k": "other"})
    ds_3 = _make_dataset(mock_clients, "dataset-rid-3", labels=[], properties={"k": "v"})

    with patch.object(Run, "list_datasets", return_value=[("a", ds_1), ("b", ds_2), ("c", ds_3)]):
        result = pandas_module.run_to_dataframe(mock_run, labels=["foo"], properties={"k": "v"})

    assert set(result.keys()) == {"dataset-rid-1"}


def test_channel_and_export_options_forwarded(mock_run: Run, mock_dataset: Dataset, patched_export: MagicMock):
    """Channel/tag/transport options are forwarded verbatim to datasource_to_dataframe."""
    with patch.object(Run, "list_datasets", return_value=[("primary", mock_dataset)]):
        pandas_module.run_to_dataframe(
            mock_run,
            channel_exact_match=["engine", "rpm"],
            channel_fuzzy_search_text="rpm",
            tags={"sensor": "alpha"},
            enable_gzip=False,
            num_workers=4,
            channel_batch_size=10,
        )

    kwargs = patched_export.call_args.kwargs
    assert kwargs["channel_exact_match"] == ["engine", "rpm"]
    assert kwargs["channel_fuzzy_search_text"] == "rpm"
    assert kwargs["tags"] == {"sensor": "alpha"}
    assert kwargs["enable_gzip"] is False
    assert kwargs["num_workers"] == 4
    assert kwargs["channel_batch_size"] == 10


def test_run_method_delegates_to_run_to_dataframe(mock_run: Run, mock_dataset: Dataset, mock_asset: Asset):
    """Run.to_dataframe forwards every kwarg verbatim to run_to_dataframe."""
    expected = {"dataset-rid-1": pd.DataFrame()}
    with patch("nominal.thirdparty.pandas._pandas.run_to_dataframe", return_value=expected) as mock:
        result = mock_run.to_dataframe(
            data_filters=[mock_dataset, mock_asset],
            labels=["env-prod"],
            properties={"k": "v"},
            channel_exact_match=["x"],
            channel_fuzzy_search_text="y",
            tags={"t": "v"},
            enable_gzip=False,
            num_workers=2,
            channel_batch_size=15,
        )

    assert result is expected
    mock.assert_called_once_with(
        mock_run,
        data_filters=[mock_dataset, mock_asset],
        labels=["env-prod"],
        properties={"k": "v"},
        channel_exact_match=["x"],
        channel_fuzzy_search_text="y",
        tags={"t": "v"},
        enable_gzip=False,
        num_workers=2,
        channel_batch_size=15,
    )
