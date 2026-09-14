from __future__ import annotations

import re
from typing import Sequence
from unittest.mock import MagicMock

import pytest
from nominal_api import scout_run_api

from nominal.core._utils.api_tools import pair_by_rid
from nominal.core.asset import Asset
from nominal.core.run import Run

_ASSET_RID = "ri.scout.x.asset.a"
_RUN_RID = "ri.scout.x.run.r"


def _scope(name: str, **arm: str) -> MagicMock:
    """A data scope carrying a real `DataSource` union, so the arm and discriminator behave."""
    scope = MagicMock()
    scope.data_scope_name = name
    scope.data_source = scout_run_api.DataSource(**arm)
    return scope


def _connection(rid: str) -> MagicMock:
    conn = MagicMock()
    conn.rid = rid
    conn.display_name = f"connection {rid}"
    conn.description = None
    conn.connection_details.nominal = None
    return conn


@pytest.fixture
def clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    # Every batch endpoint answers empty by default, so a test only sets up the type it cares
    # about and any stray request shows up as an unexpected call rather than a crash.
    clients.catalog.get_enriched_datasets.return_value = []
    clients.connection.get_connections.return_value = []
    clients.video.batch_get.return_value.responses = []
    return clients


def _asset(clients: MagicMock, *scopes: MagicMock) -> Asset:
    api_asset = MagicMock()
    api_asset.data_scopes = list(scopes)
    clients.assets.get_assets.return_value = {_ASSET_RID: api_asset}
    return Asset(
        rid=_ASSET_RID,
        name="asset",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        is_archived=False,
        _clients=clients,
    )


def _run(
    clients: MagicMock, *, assets: Sequence[str] = ("ri.a.1",), **sources_by_ref_name: scout_run_api.DataSource
) -> Run:
    api_run = MagicMock()
    api_run.data_sources = {
        ref_name: MagicMock(data_source=data_source) for ref_name, data_source in sources_by_ref_name.items()
    }
    api_run.assets = list(assets)
    clients.run.get_run.return_value = api_run
    return Run(
        rid=_RUN_RID,
        name="run",
        description="",
        properties={},
        labels=[],
        links=[],
        start=0,
        end=None,
        run_number=1,
        assets=list(assets),
        created_at=0,
        is_archived=False,
        _clients=clients,
    )


# --- pairing names to resources -----------------------------------------------


def test_names_pair_with_resources_by_rid_not_by_position() -> None:
    """Batch endpoints answer with an unordered set, so position carries no meaning."""
    resources = [MagicMock(rid="rid-b"), MagicMock(rid="rid-a")]

    paired = pair_by_rid({"first": "rid-a", "second": "rid-b"}, resources)

    assert [(name, resource.rid) for name, resource in paired] == [("first", "rid-a"), ("second", "rid-b")]


def test_a_name_whose_resource_did_not_come_back_is_dropped() -> None:
    """Batch endpoints omit RIDs the caller cannot read, and the rest must still pair correctly."""
    paired = pair_by_rid({"readable": "rid-a", "hidden": "rid-b"}, [MagicMock(rid="rid-a")])

    assert [name for name, _ in paired] == ["readable"]


# --- asset scopes -------------------------------------------------------------


def test_listing_every_scope_fetches_the_asset_once(clients: MagicMock) -> None:
    """One asset payload already carries every scope, whatever its type."""
    asset = _asset(
        clients,
        _scope("ds", dataset="ri.d.1"),
        _scope("conn", connection="ri.c.1"),
        _scope("vid", video="ri.v.1"),
        _scope("spat", spatial="ri.s.1"),
    )

    asset.list_data_scopes()

    assert clients.assets.get_assets.call_count == 1


def test_scope_types_absent_from_an_asset_cost_no_request(clients: MagicMock) -> None:
    """A type with no scopes has nothing to resolve, so it must not reach the service."""
    asset = _asset(clients, _scope("conn", connection="ri.c.1"))

    asset.list_data_scopes()

    clients.connection.get_connections.assert_called_once()
    clients.catalog.get_enriched_datasets.assert_not_called()
    clients.video.batch_get.assert_not_called()


def test_getting_one_scope_resolves_only_that_scope(clients: MagicMock) -> None:
    """Resolving a scope by name must not fetch every other data source on the asset."""
    clients.connection.get_connections.return_value = [_connection("ri.c.1")]
    asset = _asset(clients, _scope("conn", connection="ri.c.1"), _scope("vid", video="ri.v.1"))

    resolved = asset.get_data_scope("conn")

    assert resolved.rid == "ri.c.1"
    assert clients.assets.get_assets.call_count == 1
    clients.connection.get_connections.assert_called_once()
    clients.video.batch_get.assert_not_called()


def test_getting_an_unknown_scope_raises_without_resolving_anything(clients: MagicMock) -> None:
    """An unknown name is a caller mistake, and finding that out should cost one request."""
    asset = _asset(clients, _scope("conn", connection="ri.c.1"))

    with pytest.raises(ValueError, match="No such data scope found"):
        asset.get_data_scope("nope")

    assert clients.assets.get_assets.call_count == 1


def test_getting_a_scope_whose_data_source_is_unreadable_does_not_report_it_missing(clients: MagicMock) -> None:
    """The scope is on the asset, so the error must not send the caller looking for a typo."""
    asset = _asset(clients, _scope("conn", connection="ri.c.1"))
    clients.connection.get_connections.return_value = []

    with pytest.raises(ValueError, match="could not be resolved"):
        asset.get_data_scope("conn")


def test_asset_connections_resolve_in_a_single_request(clients: MagicMock) -> None:
    """Every connection scope is resolved by one call carrying all of their RIDs."""
    clients.connection.get_connections.return_value = [_connection("ri.c.2"), _connection("ri.c.1")]
    asset = _asset(clients, _scope("a", connection="ri.c.1"), _scope("b", connection="ri.c.2"))

    resolved = asset.list_connections()

    assert [(name, connection.rid) for name, connection in resolved] == [("a", "ri.c.1"), ("b", "ri.c.2")]
    clients.connection.get_connections.assert_called_once_with("Bearer t", ["ri.c.1", "ri.c.2"])


# --- run data sources ---------------------------------------------------------


def test_run_videos_resolve_in_a_single_request(clients: MagicMock) -> None:
    """Listing videos on a run batches their RIDs instead of fetching one at a time."""
    run = _run(
        clients,
        one=scout_run_api.DataSource(video="ri.v.1"),
        two=scout_run_api.DataSource(video="ri.v.2"),
        three=scout_run_api.DataSource(video="ri.v.3"),
    )

    run.list_videos()

    clients.video.batch_get.assert_called_once()
    assert sorted(clients.video.batch_get.call_args.args[1].video_rids) == ["ri.v.1", "ri.v.2", "ri.v.3"]
    clients.video.get.assert_not_called()


def test_run_scope_types_absent_from_a_run_cost_no_request(clients: MagicMock) -> None:
    """A run with no video refs must not call the video service at all."""
    run = _run(clients, ds=scout_run_api.DataSource(dataset="ri.d.1"))

    run.list_videos()

    clients.video.batch_get.assert_not_called()
    clients.video.get.assert_not_called()


def test_run_datasets_omit_a_ref_whose_dataset_did_not_come_back(clients: MagicMock) -> None:
    """A ref the catalog omits is dropped rather than raising KeyError, as it already was on Asset."""
    clients.catalog.get_enriched_datasets.return_value = [MagicMock(rid="ri.d.1")]
    run = _run(
        clients,
        readable=scout_run_api.DataSource(dataset="ri.d.1"),
        hidden=scout_run_api.DataSource(dataset="ri.d.2"),
    )

    assert [name for name, _ in run.list_datasets()] == ["readable"]


def test_run_videos_omit_a_ref_whose_video_did_not_come_back(clients: MagicMock) -> None:
    """Omitting an unreadable data source now covers videos too, not just datasets."""
    clients.video.batch_get.return_value.responses = [MagicMock(rid="ri.v.1")]
    run = _run(
        clients,
        readable=scout_run_api.DataSource(video="ri.v.1"),
        hidden=scout_run_api.DataSource(video="ri.v.2"),
    )

    assert [name for name, _ in run.list_videos()] == ["readable"]


# --- adding a scope -----------------------------------------------------------


def _raw_asset(title: str) -> MagicMock:
    """A conjure Asset bean as `add_data_scopes_to_asset` answers with."""
    raw = MagicMock()
    raw.rid = _ASSET_RID
    raw.title = title
    raw.description = None
    raw.properties = {}
    raw.labels = []
    raw.created_at = 0
    raw.is_archived = False
    raw.created_by = None
    return raw


def test_adding_a_scope_to_an_asset_updates_it_in_place(clients: MagicMock) -> None:
    """The response carries the new state, so the caller is not left holding a stale asset."""
    asset = _asset(clients)
    clients.assets.add_data_scopes_to_asset.return_value = _raw_asset("renamed-elsewhere")

    asset.add_dataset("ds", "ri.d.1")

    assert asset.name == "renamed-elsewhere"


def test_adding_a_scope_to_a_run_updates_it_in_place(clients: MagicMock) -> None:
    """Same for a run: the add response is the run's new state, not something to discard."""
    run = _run(clients)
    updated = MagicMock()
    updated.rid, updated.title, updated.description = _RUN_RID, "renamed-elsewhere", ""
    updated.properties, updated.labels, updated.links = {}, [], []
    updated.start_time = scout_run_api.UtcTimestamp(seconds_since_epoch=0, offset_nanoseconds=0)
    updated.end_time, updated.run_number = None, 1
    updated.assets, updated.is_archived, updated.created_by = [], False, None
    updated.created_at = 0
    clients.run.add_data_sources_to_run.return_value = updated

    run.add_dataset("ds", "ri.d.1")

    assert run.name == "renamed-elsewhere"


# --- the shared scope surface -------------------------------------------------


def test_listing_every_scope_on_a_run_fetches_the_run_once(clients: MagicMock) -> None:
    """A run groups its data sources from one payload, as an asset does with its scopes."""
    run = _run(
        clients,
        ds=scout_run_api.DataSource(dataset="ri.d.1"),
        conn=scout_run_api.DataSource(connection="ri.c.1"),
        vid=scout_run_api.DataSource(video="ri.v.1"),
    )

    run.list_data_scopes()

    assert clients.run.get_run.call_count == 1


def test_getting_one_scope_on_a_run_resolves_only_that_scope(clients: MagicMock) -> None:
    """Narrowing by name works the same on a run as on an asset."""
    clients.connection.get_connections.return_value = [_connection("ri.c.1")]
    run = _run(
        clients,
        conn=scout_run_api.DataSource(connection="ri.c.1"),
        vid=scout_run_api.DataSource(video="ri.v.1"),
    )

    resolved = run.get_data_scope("conn")

    assert resolved.rid == "ri.c.1"
    clients.connection.get_connections.assert_called_once()
    clients.video.batch_get.assert_not_called()


def test_a_run_reads_its_own_refs_not_the_scopes_it_inherits(clients: MagicMock) -> None:
    """`data_sources` and `asset_data_scopes` are separate namespaces and must not be conflated."""
    clients.catalog.get_enriched_datasets.return_value = [MagicMock(rid="ri.d.1")]
    run = _run(clients, ds=scout_run_api.DataSource(dataset="ri.d.1"))
    run._get_latest_api().asset_data_scopes = [_scope("inherited", dataset="ri.d.99")]

    assert [name for name, _ in run.list_datasets()] == ["ds"]


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (lambda asset: asset.get_dataset("nope"), "No dataset with data scope name 'nope' found for this asset"),
        (lambda asset: asset.get_connection("nope"), "No connection with data scope name 'nope' found for this asset"),
        (lambda asset: asset.get_video("nope"), "No video with data scope name 'nope' found for this asset"),
    ],
    ids=["dataset", "connection", "video"],
)
def test_asset_getters_keep_their_messages(clients: MagicMock, call, expected: str) -> None:
    """Each public getter still says what it said before the lookup moved into the shared mixin."""
    asset = _asset(clients)

    with pytest.raises(ValueError, match=re.escape(expected)):
        call(asset)


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (lambda run: run.get_dataset("nope"), "No dataset with ref name 'nope' found for this run"),
        (lambda run: run.get_connection("nope"), "No connection with ref name 'nope' found for this run"),
        (lambda run: run.get_video("nope"), "No video with ref name 'nope' found for this run"),
    ],
    ids=["dataset", "connection", "video"],
)
def test_run_getters_keep_their_messages(clients: MagicMock, call, expected: str) -> None:
    """A run says "ref name" where an asset says "data scope name", on all four getters."""
    run = _run(clients)

    with pytest.raises(ValueError, match=re.escape(expected)):
        call(run)


# --- multi-asset runs ---------------------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        lambda run: run.list_datasets(),
        lambda run: run.list_connections(),
        lambda run: run.list_videos(),
        lambda run: run.list_data_scopes(),
        lambda run: run.get_data_scope("ds"),
        lambda run: run.get_dataset("ds"),
        lambda run: run.get_connection("conn"),
        lambda run: run.get_video("vid"),
    ],
    ids=["datasets", "connections", "videos", "scopes", "scope", "dataset", "connection", "video"],
)
def test_a_multi_asset_run_refuses_to_resolve_scopes(clients: MagicMock, call) -> None:
    """The server leaves these fields empty on a multi-asset run, so answering would mean answering wrongly."""
    run = _run(clients, assets=["ri.a.1", "ri.a.2"], ds=scout_run_api.DataSource(dataset="ri.d.1"))

    with pytest.raises(RuntimeError, match="single asset"):
        call(run)


def test_the_ingest_facade_refuses_a_multi_asset_run_too(clients: MagicMock) -> None:
    """`_list_dataset_scopes` shares the guard, so both paths fail the same way."""
    run = _run(clients, assets=["ri.a.1", "ri.a.2"])

    with pytest.raises(RuntimeError, match="single asset"):
        run._list_dataset_scopes()


def test_a_run_with_no_assets_resolves_to_nothing_rather_than_raising(clients: MagicMock) -> None:
    """A run without assets genuinely has no data sources, so empty is the right answer."""
    run = _run(clients, assets=[])

    assert run.list_datasets() == []
    assert run.list_data_scopes() == []


def test_removing_data_sources_from_a_multi_asset_run_refuses(clients: MagicMock) -> None:
    """The write path is guarded too: the kept-set would otherwise be computed from an empty map."""
    run = _run(clients, assets=["ri.a.1", "ri.a.2"], ds=scout_run_api.DataSource(dataset="ri.d.1"))

    with pytest.raises(RuntimeError, match="single asset"):
        run.remove_data_sources(ref_names=["ds"])

    clients.run.update_run.assert_not_called()


def test_a_spatial_scope_still_groups_even_though_core_cannot_resolve_it(clients: MagicMock) -> None:
    """`nominal.experimental.spatial` reads its rids from the same grouping the core types use."""
    asset = _asset(clients, _scope("spat", spatial="ri.s.1"))

    assert asset._scope_rids_by_type()["spatial"] == {"spat": "ri.s.1"}


def test_getting_a_spatial_scope_by_name_says_where_it_lives(clients: MagicMock) -> None:
    """The scope exists, so "no such data scope" would send the caller looking for a typo."""
    asset = _asset(clients, _scope("spat", spatial="ri.s.1"))

    with pytest.raises(ValueError, match="is a spatial, which core does not resolve"):
        asset.get_data_scope("spat")


def test_listing_scopes_leaves_spatials_out(clients: MagicMock) -> None:
    """Core lists only the types it can resolve; spatials come from list_spatials_in_asset."""
    clients.connection.get_connections.return_value = [_connection("ri.c.1")]
    asset = _asset(clients, _scope("conn", connection="ri.c.1"), _scope("spat", spatial="ri.s.1"))

    assert [name for name, _ in asset.list_data_scopes()] == ["conn"]
