from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.run.v1 import run_service_pb2


@pytest.fixture
def clients():
    clients = MagicMock()
    clients.catalog.get_enriched_datasets.return_value = []
    return clients


def _scope(name, **source):
    return asset_pb2.DataScope(data_scope_name=name, data_source=asset_pb2.DataSource(**source))


def _asset(clients, *scopes):
    api = asset_pb2.Asset(rid="asset", data_scopes=scopes)
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(responses={"asset": api})
    return Asset._from_proto(clients, api)


def _run(clients, **sources):
    api = run_service_pb2.Run(
        rid="run",
        assets=["asset"],
        data_sources={
            name: run_service_pb2.RunDataSource(data_source=run_service_pb2.DataSource(**source))
            for name, source in sources.items()
        },
    )
    clients.run.GetRun.return_value = run_service_pb2.GetRunResponse(run=api)
    return Run._from_proto(clients, api)


@pytest.mark.parametrize(
    ("resolve", "requests"),
    [
        pytest.param(lambda asset: asset.list_data_scopes(), (1, 1), id="list"),
        pytest.param(lambda asset: asset.get_data_scope("vid-b"), (0, 1), id="get"),
    ],
)
def test_scope_resolution_makes_one_request_per_needed_type(clients, resolve, requests):
    """The asset is read once, and each needed type is fetched in one batch rather than once per scope."""
    clients.video.batch_get.side_effect = lambda auth, request: MagicMock(
        responses=[MagicMock(rid=rid) for rid in request.video_rids]
    )
    asset = _asset(
        clients,
        _scope("ds-a", dataset="ds-a"),
        _scope("ds-b", dataset="ds-b"),
        _scope("conn-a", connection="conn-a"),
        _scope("conn-b", connection="conn-b"),
        _scope("vid-a", video="vid-a"),
        _scope("vid-b", video="vid-b"),
    )

    resolve(asset)

    assert clients.assets.GetAssets.call_count == 1
    assert (
        clients.catalog.get_enriched_datasets.call_count,
        clients.video.batch_get.call_count,
    ) == requests
    clients.video.get.assert_not_called()


@pytest.mark.parametrize("source", [{"dataset": "gone"}, {"connection": "gone"}, {"video": "gone"}])
def test_a_scope_that_does_not_resolve_is_left_out_of_lists_and_not_found_by_lookup(clients, source):
    """Missing and unreadable resources look the same: lists leave the scope out, and lookups raise NotFound.

    The NotFound is still a ValueError, which these lookups raised before, so existing handlers keep working.
    """
    clients.video.batch_get.return_value.responses = []
    asset = _asset(clients, _scope("missing", **source))

    assert asset.list_data_scopes() == ()
    with pytest.raises(NominalNotFoundError, match="No such data scope found on asset asset") as raised:
        asset.get_data_scope("missing")
    assert isinstance(raised.value, ValueError)


def test_a_run_reads_its_own_refs_not_the_scopes_it_inherits(clients, make_enriched_dataset):
    """A run's dataset methods see its own refs, not the asset scopes it inherits for scoped ingest."""
    clients.catalog.get_enriched_datasets.side_effect = lambda auth, request: [
        make_enriched_dataset(rid) for rid in request.dataset_rids
    ]
    run = _run(clients, direct={"dataset": "direct"})
    clients.run.GetRun.return_value.run.asset_data_scopes.append(
        run_service_pb2.DataScope(data_scope_name="inherited", data_source=run_service_pb2.DataSource(dataset="other"))
    )

    assert [(name, dataset.rid) for name, dataset in run.list_datasets()] == [("direct", "direct")]
    with pytest.raises(NominalNotFoundError, match="No dataset with ref name 'inherited' found for this run"):
        run.get_dataset("inherited")


def test_a_spatial_scope_is_not_a_core_data_scope(clients):
    """Spatials are reached through the experimental spatial helpers, not the core data scope methods."""
    asset = _asset(clients, _scope("spatial", spatial="spatial-rid"))

    assert asset.list_data_scopes() == ()
    with pytest.raises(NominalNotFoundError, match="with data_scope_name spatial"):
        asset.get_data_scope("spatial")
