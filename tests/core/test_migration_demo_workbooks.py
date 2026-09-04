from unittest.mock import MagicMock

from nominal.experimental.migration.migration_cli import _update_demo_workbooks
from nominal.experimental.migration.resource_type import ResourceType
from nominal.protos.sandbox.v1 import sandbox_workspace_pb2

_WORKSPACE_RID = "ri.workspace.main.workspace.target"


def _make_target_client(existing_rids: list[str]) -> MagicMock:
    client = MagicMock()
    client.get_workspace.return_value.rid = _WORKSPACE_RID
    client._clients.sandbox_workspace.GetDemoWorkbooks.return_value = sandbox_workspace_pb2.GetDemoWorkbooksResponse(
        notebook_rids=existing_rids
    )
    return client


def _make_runner(new_rids: list[str]) -> MagicMock:
    runner = MagicMock()
    runner.migration_state.rid_mapping = {
        ResourceType.WORKBOOK.value: {f"ri.notebook.source.{i}": rid for i, rid in enumerate(new_rids)}
    }
    return runner


def test_update_demo_workbooks_appends_new_rids_after_existing_without_duplicates() -> None:
    """Newly migrated workbook RIDs are appended after the existing demo list, de-duplicated, order preserved."""
    client = _make_target_client(["ri.notebook.a", "ri.notebook.b"])
    runner = _make_runner(["ri.notebook.b", "ri.notebook.c"])

    _update_demo_workbooks(client, runner)

    stub = client._clients.sandbox_workspace
    get_request = stub.GetDemoWorkbooks.call_args.args[0]
    assert get_request.workspace_rid == _WORKSPACE_RID
    set_request = stub.SetDemoWorkbooks.call_args.args[0]
    assert set_request.workspace_rid == _WORKSPACE_RID
    assert list(set_request.request.notebook_rids) == ["ri.notebook.a", "ri.notebook.b", "ri.notebook.c"]


def test_update_demo_workbooks_skips_sandbox_calls_when_no_workbooks_migrated() -> None:
    """When the migration created no workbooks, the demo list is left untouched (no sandbox RPCs)."""
    client = _make_target_client([])
    runner = _make_runner([])

    _update_demo_workbooks(client, runner)

    client._clients.sandbox_workspace.GetDemoWorkbooks.assert_not_called()
    client._clients.sandbox_workspace.SetDemoWorkbooks.assert_not_called()
