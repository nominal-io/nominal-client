from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from nominal.core.asset import Asset
from nominal.core.dataset import Dataset
from nominal.core.run import Run
from nominal.thirdparty.pandas import _pandas as pandas_module


def _make_dataset(rid: str, *, labels: list[str] | None = None, properties: dict[str, str] | None = None) -> Any:
    ds = MagicMock(spec=Dataset)
    ds.rid = rid
    ds.labels = labels or []
    ds.properties = properties or {}
    return ds


def _make_asset(rid: str, dataset_pairs: list[tuple[str, Any]]) -> Any:
    asset = MagicMock(spec=Asset)
    asset.rid = rid
    asset.list_datasets.return_value = dataset_pairs
    return asset


def _make_run(
    *,
    rid: str = "run-rid",
    start: int = 1_000,
    end: int | None = 2_000,
    dataset_pairs: list[tuple[str, Any]] | None = None,
    asset_rids: list[str] | None = None,
) -> Any:
    run = MagicMock(spec=Run)
    run.rid = rid
    run.start = start
    run.end = end
    run.assets = asset_rids or []
    run.list_datasets.return_value = dataset_pairs or []
    return run


@pytest.fixture
def patched_export():
    """Patch datasource_to_dataframe in _pandas to return a uniquely-identifiable DataFrame per call."""
    with patch.object(pandas_module, "datasource_to_dataframe") as mock:
        mock.side_effect = lambda dataset, **kwargs: pd.DataFrame({"rid": [dataset.rid]})
        yield mock


def test_default_downloads_all_run_datasets(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a")
    ds_b = _make_dataset("rid-b")
    run = _make_run(dataset_pairs=[("primary", ds_a), ("secondary", ds_b)])

    result = pandas_module.run_to_dataframe(run)

    assert set(result.keys()) == {"rid-a", "rid-b"}
    assert patched_export.call_count == 2
    for call in patched_export.call_args_list:
        kwargs = call.kwargs
        assert kwargs["start"] == 1_000
        assert kwargs["end"] == 2_000


def test_open_run_forwards_none_end(patched_export: MagicMock):
    ds = _make_dataset("rid-a")
    run = _make_run(end=None, dataset_pairs=[("primary", ds)])

    pandas_module.run_to_dataframe(run)

    assert patched_export.call_args.kwargs["end"] is None


def test_dataset_filter_downloads_only_matched(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a")
    ds_b = _make_dataset("rid-b")
    run = _make_run(dataset_pairs=[("primary", ds_a), ("secondary", ds_b)])

    result = pandas_module.run_to_dataframe(run, data_filters=ds_b)

    assert set(result.keys()) == {"rid-b"}


def test_dataset_not_in_run_warns_and_skips(patched_export: MagicMock, caplog: pytest.LogCaptureFixture):
    ds_a = _make_dataset("rid-a")
    stranger = _make_dataset("rid-stranger")
    run = _make_run(dataset_pairs=[("primary", ds_a)])

    with caplog.at_level("WARNING", logger=pandas_module.__name__):
        result = pandas_module.run_to_dataframe(run, data_filters=[stranger, ds_a])

    assert set(result.keys()) == {"rid-a"}
    assert any("does not have a dataset" in r.message and "rid-stranger" in r.message for r in caplog.records)


def test_asset_filter_includes_all_asset_datasets(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a")
    ds_b = _make_dataset("rid-b")
    asset = _make_asset("asset-rid", [("from_asset_a", ds_a), ("from_asset_b", ds_b)])
    run = _make_run(dataset_pairs=[("primary", ds_a)], asset_rids=["asset-rid"])

    result = pandas_module.run_to_dataframe(run, data_filters=asset)

    assert set(result.keys()) == {"rid-a", "rid-b"}


def test_asset_not_in_run_warns_and_skips(patched_export: MagicMock, caplog: pytest.LogCaptureFixture):
    ds_a = _make_dataset("rid-a")
    asset = _make_asset("asset-rid", [("foo", ds_a)])
    run = _make_run(dataset_pairs=[("primary", ds_a)], asset_rids=[])

    with caplog.at_level("WARNING", logger=pandas_module.__name__):
        result = pandas_module.run_to_dataframe(run, data_filters=[asset, ds_a])

    assert set(result.keys()) == {"rid-a"}
    assert any("does not have an asset" in r.message and "asset-rid" in r.message for r in caplog.records)


def test_filter_sequence_unions_and_dedupes_by_rid(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a")
    ds_b = _make_dataset("rid-b")
    asset = _make_asset("asset-rid", [("from_asset_a", ds_a), ("from_asset_b", ds_b)])
    run = _make_run(
        dataset_pairs=[("primary", ds_a), ("secondary", ds_b)],
        asset_rids=["asset-rid"],
    )

    # ds_a is matched both directly and through the asset; should still be downloaded once.
    result = pandas_module.run_to_dataframe(run, data_filters=[ds_a, asset])

    assert set(result.keys()) == {"rid-a", "rid-b"}
    assert patched_export.call_count == 2


def test_labels_filter_requires_all_labels(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a", labels=["env-prod", "team-a"])
    ds_b = _make_dataset("rid-b", labels=["env-prod"])
    ds_c = _make_dataset("rid-c", labels=["env-dev", "team-a"])
    run = _make_run(dataset_pairs=[("a", ds_a), ("b", ds_b), ("c", ds_c)])

    result = pandas_module.run_to_dataframe(run, labels=["env-prod", "team-a"])

    assert set(result.keys()) == {"rid-a"}


def test_properties_filter_requires_all_kv_pairs(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a", properties={"environment": "prod", "owner": "team-a"})
    ds_b = _make_dataset("rid-b", properties={"environment": "prod"})
    ds_c = _make_dataset("rid-c", properties={"environment": "dev", "owner": "team-a"})
    run = _make_run(dataset_pairs=[("a", ds_a), ("b", ds_b), ("c", ds_c)])

    result = pandas_module.run_to_dataframe(run, properties={"environment": "prod", "owner": "team-a"})

    assert set(result.keys()) == {"rid-a"}


def test_labels_and_properties_combined(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a", labels=["foo"], properties={"k": "v"})
    ds_b = _make_dataset("rid-b", labels=["foo"], properties={"k": "other"})
    ds_c = _make_dataset("rid-c", labels=[], properties={"k": "v"})
    run = _make_run(dataset_pairs=[("a", ds_a), ("b", ds_b), ("c", ds_c)])

    result = pandas_module.run_to_dataframe(run, labels=["foo"], properties={"k": "v"})

    assert set(result.keys()) == {"rid-a"}


def test_channel_filters_and_export_options_forwarded(patched_export: MagicMock):
    ds_a = _make_dataset("rid-a")
    run = _make_run(dataset_pairs=[("primary", ds_a)])

    pandas_module.run_to_dataframe(
        run,
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


def test_run_method_delegates_to_run_to_dataframe():
    """Run.to_dataframe forwards every kwarg verbatim to run_to_dataframe."""
    ds = _make_dataset("rid-a")
    asset = _make_asset("asset-rid", [])

    run = Run(
        rid="run-rid",
        name="r",
        description="",
        properties={},
        labels=(),
        links=(),
        start=1_000,
        end=2_000,
        run_number=1,
        assets=(),
        created_at=0,
        _clients=MagicMock(),
    )

    expected = {"rid-a": pd.DataFrame()}
    with patch("nominal.thirdparty.pandas._pandas.run_to_dataframe", return_value=expected) as mock:
        result = run.to_dataframe(
            data_filters=[ds, asset],
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
    mock.assert_called_once()
    args, kwargs = mock.call_args
    assert args == (run,)
    assert kwargs == dict(
        data_filters=[ds, asset],
        labels=["env-prod"],
        properties={"k": "v"},
        channel_exact_match=["x"],
        channel_fuzzy_search_text="y",
        tags={"t": "v"},
        enable_gzip=False,
        num_workers=2,
        channel_batch_size=15,
    )
