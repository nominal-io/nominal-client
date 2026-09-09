from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.run import Run
from nominal.protos.event.v2 import event_pb2


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "workspace"
    return clients


@pytest.fixture
def make_run(mock_clients):
    def _make_run(assets):
        return Run(
            rid="run-rid-1",
            name="Test Run",
            description="",
            properties={},
            labels=[],
            links=[],
            start=0,
            end=1,
            run_number=1,
            assets=assets,
            created_at=0,
            is_archived=False,
            _clients=mock_clients,
        )

    return _make_run


@pytest.fixture
def mock_run(make_run):
    return make_run(["asset-rid-1", "asset-rid-2"])


def test_search_events_ors_run_assets(mock_run, mock_clients):
    """Run.search_events matches events on any of the run's assets (a single OR asset filter)."""
    mock_clients.event.SearchEvents.return_value = event_pb2.SearchEventsResponse()

    result = mock_run.search_events()

    assert result == []
    mock_clients.event.SearchEvents.assert_called_once()
    request = mock_clients.event.SearchEvents.call_args.args[0]
    assert request.query == event_pb2.SearchQuery(
        **{
            "and": event_pb2.SearchQueryList(
                queries=[
                    event_pb2.SearchQuery(
                        assets=event_pb2.AssetsFilter(assets=["asset-rid-1", "asset-rid-2"], operator=event_pb2.OR)
                    )
                ]
            )
        }
    )


def test_search_events_empty_assets_returns_no_events(make_run, mock_clients):
    """A run with no associated assets returns no events instead of searching all events."""
    run = make_run([])

    result = run.search_events()

    assert result == []
    mock_clients.event.SearchEvents.assert_not_called()


def test_nominal_url_identifies_the_run_by_rid(mock_run, mock_clients):
    """Run pages are addressed by rid: the app no longer serves run-number URLs."""
    mock_clients.app_base_url = "https://app.nominal.test"
    mock_clients.resolve_default_workspace_rid.return_value = "ri.workspace.test"

    assert mock_run.nominal_url == "https://app.nominal.test/w/ri.workspace.test/runs/run-rid-1"


def test_proto_hydration_preserves_presence_and_nanoseconds(mock_clients):
    from nominal.protos.run.v1 import run_pb2
    from nominal.protos.run.v1 import run_service_pb2 as pb

    raw = pb.Run(
        rid="run",
        start_time=run_pb2.UtcTimestamp(seconds_since_epoch=1, offset_nanoseconds=23),
        end_time=run_pb2.UtcTimestamp(),
        links=[pb.Link(url="a"), pb.Link(url="b", title="")],
        properties={"k": "v"},
    )
    raw.created_at.FromNanoseconds(123)
    run = Run._from_proto(mock_clients, raw)
    assert (run.start, run.end, run.created_at, run.author_rid) == (1_000_000_023, 0, 123, None)
    assert run.links == ({"url": "a"}, {"url": "b", "title": ""})
    raw.properties["k"] = "changed"
    assert run.properties == {"k": "v"}
    raw.ClearField("end_time")
    assert Run._from_proto(mock_clients, raw).end is None


def test_update_distinguishes_omission_from_empty_values(mock_run, mock_clients):
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.run.UpdateRun.return_value = pb.UpdateRunResponse(run=pb.Run(rid=mock_run.rid, title="new"))
    assert mock_run.update(name="new") is mock_run
    assert mock_run.name == "new"
    request = mock_clients.run.UpdateRun.call_args.args[0]
    assert request.rid == mock_run.rid
    for field in ("labels", "properties", "links", "end_time", "description"):
        assert not request.HasField(field)
    mock_run.update(labels=[], properties={}, links=[], description="", end=0)
    request = mock_clients.run.UpdateRun.call_args.args[0]
    for field in ("labels", "properties", "links", "end_time", "description"):
        assert request.HasField(field)
    assert not request.labels.labels
    assert not request.properties.properties
    assert not request.links.links


def test_add_dataset_preserves_exact_negative_offset(mock_run, mock_clients):
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_run.add_dataset("telemetry", "dataset-rid", series_tags={"mode": "test"}, offset=-1)
    request = mock_clients.run.AddDataSourcesToRun.call_args.args[0]
    assert request == pb.AddDataSourcesToRunRequest(
        run_rid=mock_run.rid,
        data_sources={
            "telemetry": pb.CreateRunDataSource(
                data_source=pb.DataSource(dataset="dataset-rid"),
                series_tags={"mode": "test"},
                offset=pb.Duration(seconds=-1, nanos=999_999_999),
            )
        },
    )
    mock_run.add_connection("live", "connection-rid")
    request = mock_clients.run.AddDataSourcesToRun.call_args.args[0]
    assert not request.data_sources["live"].HasField("offset")


def test_remove_sources_preserves_other_oneof_variants_and_absent_offset(mock_run, mock_clients):
    from nominal.protos.run.v1 import run_service_pb2 as pb

    raw = pb.Run(
        rid=mock_run.rid,
        data_sources={
            "telemetry": pb.RunDataSource(data_source=pb.DataSource(dataset="dataset")),
            "logs": pb.RunDataSource(data_source=pb.DataSource(log_set="logs"), series_tags={"k": "v"}),
        },
    )
    mock_clients.run.GetRun.return_value = pb.GetRunResponse(run=raw)
    mock_clients.run.UpdateRun.return_value = pb.UpdateRunResponse(run=raw)
    mock_run.remove_data_sources(data_sources=["dataset"])
    request = mock_clients.run.UpdateRun.call_args.args[0]
    assert set(request.data_sources.data_sources) == {"logs"}
    remaining = request.data_sources.data_sources["logs"]
    assert remaining.data_source.WhichOneof("data_source") == "log_set"
    assert remaining.series_tags == {"k": "v"}
    assert not remaining.HasField("offset")
    mock_run.remove_data_sources(ref_names=["telemetry", "logs"])
    request = mock_clients.run.UpdateRun.call_args.args[0]
    assert request.HasField("data_sources")
    assert not request.data_sources.data_sources


def test_remove_sources_rejects_empty_rid_before_rpc(mock_run, mock_clients):
    with pytest.raises(ValueError, match="empty"):
        mock_run.remove_data_sources(data_sources=[""])
    mock_clients.run.GetRun.assert_not_called()
    mock_clients.run.UpdateRun.assert_not_called()


def test_create_run_resolves_workspace_and_hydrates_result(mock_clients):
    from nominal.core.client import NominalClient
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.resolve_default_workspace_rid.return_value = "workspace"
    mock_clients.run.CreateRun.return_value = pb.CreateRunResponse(run=pb.Run(rid="created", title="result"))
    client = NominalClient(_clients=mock_clients)
    created = client.create_run(
        "name",
        1_000_000_007,
        0,
        assets=["asset"],
        links=["url", ("named", "title"), {"url": "empty", "title": ""}],
        attachments=iter(["attachment"]),
        properties={"key": "value"},
        labels=["label"],
    )
    assert isinstance(created, Run)
    assert created.rid == "created"
    request = mock_clients.run.CreateRun.call_args.args[0]
    assert request.workspace == "workspace"
    assert request.start_time.seconds_since_epoch == 1
    assert request.start_time.offset_nanoseconds == 7
    assert request.HasField("end_time")
    assert list(request.assets) == ["asset"]
    assert list(request.attachments) == ["attachment"]
    assert request.properties == {"key": "value"}
    assert list(request.labels) == ["label"]
    assert not request.links[0].HasField("title")
    assert request.links[1].title == "title"
    assert request.links[2].HasField("title")
    client.create_run("open", 0, None)
    assert not mock_clients.run.CreateRun.call_args.args[0].HasField("end_time")


def test_get_and_refresh_return_sdk_runs(mock_clients):
    from nominal.core.client import NominalClient
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.run.GetRun.return_value = pb.GetRunResponse(run=pb.Run(rid="run", title="initial"))
    run = NominalClient(_clients=mock_clients).get_run("run")
    mock_clients.run.GetRun.assert_called_once_with(pb.GetRunRequest(rid="run"))
    mock_clients.run.GetRun.return_value = pb.GetRunResponse(run=pb.Run(rid="run", title="updated"))
    assert run.name == "initial"
    assert run.refresh() is run
    assert run.name == "updated"


@pytest.mark.parametrize("archived", [False, True])
def test_archive_and_unarchive_leave_local_state_until_refresh(mock_run, mock_clients, archived):
    from nominal.protos.run.v1 import run_service_pb2 as pb

    if archived:
        mock_run.archive()
        mock_clients.run.ArchiveRun.assert_called_once_with(pb.ArchiveRunRequest(rid=mock_run.rid))
    else:
        mock_run.unarchive()
        mock_clients.run.UnarchiveRun.assert_called_once_with(pb.UnarchiveRunRequest(rid=mock_run.rid))
    mock_clients.run.GetRun.assert_not_called()
    assert not mock_run.is_archived


def test_attachment_updates_accept_generators(mock_run, mock_clients):
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_run.add_attachments(iter(["first", "second"]))
    mock_clients.run.UpdateRunAttachment.assert_called_with(
        pb.UpdateRunAttachmentRequest(rid=mock_run.rid, attachments_to_add=["first", "second"])
    )
    mock_run.remove_attachments(iter(["first"]))
    mock_clients.run.UpdateRunAttachment.assert_called_with(
        pb.UpdateRunAttachmentRequest(rid=mock_run.rid, attachments_to_remove=["first"])
    )


def test_dataset_scope_lookup_filters_oneof_and_rejects_multi_asset_runs(mock_run, mock_clients):
    from nominal.protos.run.v1 import run_service_pb2 as pb

    raw = pb.Run(
        assets=["asset"],
        asset_data_scopes=[
            pb.DataScope(data_scope_name="same", data_source=pb.DataSource(connection="connection")),
            pb.DataScope(data_scope_name="same", data_source=pb.DataSource(dataset="dataset"), series_tags={"k": "v"}),
        ],
    )
    mock_clients.run.GetRun.return_value = pb.GetRunResponse(run=raw)
    assert mock_run._lookup_dataset_scope("same") == ("dataset", {"k": "v"})
    assert mock_run._lookup_dataset_scope("missing") is None
    mock_clients.run.GetRun.return_value.run.assets.append("second")
    with pytest.raises(RuntimeError, match="multi-asset"):
        mock_run._lookup_dataset_scope("same")


def test_list_sources_uses_active_oneof(mock_run, mock_clients):
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.run.GetRun.return_value = pb.GetRunResponse(
        run=pb.Run(
            data_sources={
                "dataset": pb.RunDataSource(data_source=pb.DataSource(dataset="dataset")),
                "spatial": pb.RunDataSource(data_source=pb.DataSource(spatial="spatial")),
                "unset": pb.RunDataSource(),
            }
        )
    )
    assert mock_run._list_datasource_rids("dataset") == {"dataset": "dataset"}
    assert mock_run._list_datasource_rids() == {"dataset": "dataset", "spatial": "spatial"}


@pytest.mark.parametrize("archive_status", list(ArchiveStatusFilter))
def test_search_runs_paginates_and_preserves_query(mock_clients, archive_status):
    from nominal.core.client import NominalClient
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.run.SearchRuns.side_effect = [
        pb.SearchRunsResponse(results=[pb.Run(rid="first")], next_page_token="next"),
        pb.SearchRunsResponse(results=[pb.Run(rid="second")]),
    ]
    result = NominalClient(_clients=mock_clients).search_runs(search_text="test", archive_status=archive_status)
    assert [run.rid for run in result] == ["first", "second"]
    assert all(isinstance(run, Run) for run in result)
    first, second = [call.args[0] for call in mock_clients.run.SearchRuns.call_args_list]
    assert not first.HasField("next_page_token")
    assert second.next_page_token == "next"
    assert first.query == second.query
    assert first.query.all_of.queries[0].search_text == "test"
    assert first.sort.field == pb.START_TIME and first.sort.is_descending
    assert first.page_size == 100
    assert list(first.archived_statuses.archived_statuses) == archive_status.to_proto_archived_statuses()


def test_asset_run_pagination(mock_clients):
    from nominal.core._utils.pagination_tools import search_runs_by_asset_paginated
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.run.GetRunsByAsset.side_effect = [
        pb.GetRunsByAssetResponse(results=[pb.Run(rid="first")], next_page_token="next"),
        pb.GetRunsByAssetResponse(results=[pb.Run(rid="second")]),
    ]
    assert [run.rid for run in search_runs_by_asset_paginated(mock_clients.run, "asset")] == ["first", "second"]
    first, second = [call.args[0] for call in mock_clients.run.GetRunsByAsset.call_args_list]
    assert first == pb.GetRunsByAssetRequest(asset="asset")
    assert second == pb.GetRunsByAssetRequest(asset="asset", next_page_token="next")


@pytest.mark.parametrize(
    "assets, selected, error",
    [
        (["one"], None, None),
        (["one"], "different", "different asset"),
        (["one", "two"], None, "without specifying"),
        (["one", "two"], "two", None),
    ],
)
def test_data_review_builder_resolves_run_assets_over_grpc(mock_clients, assets, selected, error):
    from nominal.core.data_review import DataReviewBuilder
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.run.GetRun.return_value = pb.GetRunResponse(run=pb.Run(assets=assets))
    builder = DataReviewBuilder(_integration_rids=[], _requests=[], _tags=[], _clients=mock_clients)
    if error:
        with pytest.raises(ValueError, match=error):
            builder.execute_checklist("run", "checklist", asset=selected)
        assert not builder._requests
    else:
        assert builder.execute_checklist("run", "checklist", asset=selected) is builder
        assert builder._requests[0].run_rid == "run"
        assert builder._requests[0].asset_rid == selected
    mock_clients.run.GetRun.assert_called_once_with(pb.GetRunRequest(rid="run"))


def test_workbook_template_resolves_video_asset_from_grpc_run(mock_clients):
    from unittest.mock import patch

    from nominal_api import scout_chartdefinition_api as charts
    from nominal_api import scout_workbookcommon_api as common

    from nominal.core.workbook import Workbook, WorkbookType
    from nominal.core.workbook_template import WorkbookTemplate
    from nominal.protos.run.v1 import run_service_pb2 as pb

    mock_clients.template.get.return_value.content = common.WorkbookContent(
        channel_variables={},
        charts={
            "video": charts.VizDefinition(
                video=charts.VideoVizDefinition(
                    v1=charts.VideoVizDefinitionV1(comparison_run_groups=[], ref_name="camera")
                )
            )
        },
    )
    mock_clients.run.GetRun.return_value = pb.GetRunResponse(run=pb.Run(assets=["asset"]))
    template = WorkbookTemplate(
        rid="template",
        title="template",
        description="",
        labels=[],
        properties={},
        workbook_type=WorkbookType.WORKBOOK,
        _clients=mock_clients,
    )
    with patch.object(Workbook, "_from_conjure"):
        template.create_workbook(run="run")
    mock_clients.run.GetRun.assert_called_once_with(pb.GetRunRequest(rid="run"))
    request = mock_clients.notebook.create.call_args.args[1]
    source = request.content_v2.workbook.charts["video"].video.v1.datasource
    assert (source.asset_rid, source.run_rid, source.ref_name) == ("asset", "run", "camera")
