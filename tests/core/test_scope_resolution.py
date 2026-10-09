from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.asset import Asset
from nominal.core.run import Run
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


def _dataset(rid):
    return MagicMock(rid=rid, name=rid, description=None, labels=[], properties={}, bounds=None, is_archived=False)


@pytest.mark.parametrize(
    ("resolve", "requests"),
    [
        pytest.param(lambda asset: asset.list_data_scopes(), (1, 1, 1), id="list"),
        pytest.param(lambda asset: asset.get_data_scope("conn-b"), (0, 1, 0), id="get"),
    ],
)
def test_scope_resolution_makes_one_request_per_needed_type(clients, resolve, requests):
    """The asset is read once, and each needed type is fetched in one batch rather than once per scope."""
    clients.connection.get_connections.side_effect = lambda auth, rids: [MagicMock(rid=rid) for rid in rids]
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
        clients.connection.get_connections.call_count,
        clients.video.batch_get.call_count,
    ) == requests
    clients.connection.get_connection.assert_not_called()
    clients.video.get.assert_not_called()


@pytest.mark.parametrize(
    ("kind", "single_lookup"),
    [
        pytest.param("connection", lambda clients: clients.connection.get_connection, id="connection"),
        pytest.param("video", lambda clients: clients.video.get, id="video"),
    ],
)
def test_a_scope_the_batch_omits_still_raises_the_single_lookup_error(clients, kind, single_lookup):
    """Batch endpoints silently drop missing or unreadable RIDs; those must still fail as a single lookup does."""
    clients.connection.get_connections.return_value = []
    clients.video.batch_get.return_value.responses = []
    single_lookup(clients).side_effect = LookupError("not found")
    asset = _asset(clients, _scope("missing", **{kind: "missing-rid"}))

    with pytest.raises(LookupError, match="not found"):
        asset.list_data_scopes()

    single_lookup(clients).assert_called_once_with(clients.auth_header, "missing-rid")


def test_a_run_reads_its_own_refs_not_the_scopes_it_inherits(clients):
    """Run resource lookup and inherited asset-scope ingest use separate namespaces."""
    clients.catalog.get_enriched_datasets.return_value = [_dataset("direct")]
    run = _run(clients, direct={"dataset": "direct"})
    clients.run.GetRun.return_value.run.asset_data_scopes.append(
        run_service_pb2.DataScope(data_scope_name="inherited", data_source=run_service_pb2.DataSource(dataset="other"))
    )

    assert [(name, dataset.rid) for name, dataset in run.list_datasets()] == [("direct", "direct")]


def test_a_spatial_scope_still_groups_for_experimental_resolution(clients):
    """Spatial helpers share the parent lookup without entering core resource listings."""
    asset = _asset(clients, _scope("spatial", spatial="spatial-rid"))

    assert asset._scope_rids("spatial") == {"spatial": "spatial-rid"}
    assert asset.list_data_scopes() == ()
