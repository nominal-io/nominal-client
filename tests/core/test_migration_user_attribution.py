"""Checklist assignee mapping and data-review executor resolution during migration.

The checklist assignee is a request field (not a calling identity), so it must be translated
through MigrationContext.user_rid_mapping. Data-review created_by comes from the calling
identity, so executions must go through a client resolved from the *source data review*
rather than the checklist author's client.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.asset_migrator import AssetMigrator
from nominal.experimental.migration.migrator.checklist_migrator import ChecklistCopyOptions, ChecklistMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.parallel_migration_runner import run_parallel_migration
from nominal.experimental.migration.resource_type import ResourceType

_STACK = "cerulean-staging"


def _make_client(name: str) -> MagicMock:
    client = MagicMock(name=name)
    client._clients.workspace_rid = f"{name}-workspace"
    workspace = MagicMock()
    workspace.rid = f"{name}-workspace"
    client.get_workspace.return_value = workspace
    return client


def _make_source_checklist(*, assignee_rid: str) -> MagicMock:
    checklist = MagicMock()
    checklist.rid = f"ri.scout.{_STACK}.checklist.00000001-0000-0000-0000-000000000000"
    checklist.name = "source-checklist"
    checklist.description = "source description"
    api = MagicMock()
    api.commit.message = "source commit"
    api.checks = []
    api.checklist_variables = []
    api.metadata.properties = {}
    api.metadata.labels = []
    api.metadata.is_published = True
    api.metadata.assignee_rid = assignee_rid
    checklist._get_latest_api.return_value = api
    return checklist


def _copy_checklist(ctx: MigrationContext, source_checklist: MagicMock, options: ChecklistCopyOptions) -> MagicMock:
    with patch(
        "nominal.experimental.migration.migrator.checklist_migrator._create_checklist_with_content"
    ) as create_checklist:
        create_checklist.return_value = MagicMock(rid="dest-checklist-rid")
        ChecklistMigrator(ctx).copy_from(source_checklist, options)
    return create_checklist


def test_checklist_assignee_is_translated_through_user_rid_mapping() -> None:
    ctx = MigrationContext(
        destination_client=_make_client("destination"),
        migration_state=MigrationState(),
        user_rid_mapping={"source-user-rid": "dest-user-rid"},
    )
    source_checklist = _make_source_checklist(assignee_rid="source-user-rid")

    create_checklist = _copy_checklist(ctx, source_checklist, ChecklistCopyOptions())

    assert create_checklist.call_args.kwargs["assignee_rid"] == "dest-user-rid"


def test_checklist_assignee_falls_back_to_creating_user_when_unmapped() -> None:
    ctx = MigrationContext(
        destination_client=_make_client("destination"),
        migration_state=MigrationState(),
        user_rid_mapping={"other-user-rid": "dest-user-rid"},
    )
    source_checklist = _make_source_checklist(assignee_rid="source-user-rid")

    create_checklist = _copy_checklist(ctx, source_checklist, ChecklistCopyOptions())

    # None defers to _create_checklist_with_content's default: the creating user.
    assert create_checklist.call_args.kwargs["assignee_rid"] is None


def test_map_user_rid_warns_only_when_a_configured_mapping_misses(caplog: pytest.LogCaptureFixture) -> None:
    with_mapping = MigrationContext(
        destination_client=_make_client("destination"),
        migration_state=MigrationState(),
        user_rid_mapping={"other-user-rid": "dest-user-rid"},
    )
    without_mapping = MigrationContext(
        destination_client=_make_client("destination"),
        migration_state=MigrationState(),
    )

    with caplog.at_level("WARNING", logger="nominal.experimental.migration.migrator.context"):
        assert without_mapping.map_user_rid("source-user-rid") is None
        assert not caplog.records
        assert with_mapping.map_user_rid("source-user-rid") is None

    assert [r.levelname for r in caplog.records] == ["WARNING"]
    assert "source-user-rid" in caplog.records[0].getMessage()


def test_checklist_assignee_option_overrides_mapping() -> None:
    ctx = MigrationContext(
        destination_client=_make_client("destination"),
        migration_state=MigrationState(),
        user_rid_mapping={"source-user-rid": "dest-user-rid"},
    )
    source_checklist = _make_source_checklist(assignee_rid="source-user-rid")

    create_checklist = _copy_checklist(
        ctx, source_checklist, ChecklistCopyOptions(new_assignee_rid="override-user-rid")
    )

    assert create_checklist.call_args.kwargs["assignee_rid"] == "override-user-rid"


def test_parallel_migration_passes_user_rid_mapping_to_context(tmp_path) -> None:
    # The CLI always runs through run_parallel_migration, which builds its own MigrationContext —
    # the mapping must survive that path, not just MigrationRunner.run_migration.
    source_checklist = _make_source_checklist(assignee_rid="source-user-rid")
    runner = MigrationRunner(
        migration_resources=MigrationResources(
            source_assets={},
            source_standalone_templates=[],
            source_standalone_checklists=[source_checklist],
        ),
        dataset_config=MigrationDatasetConfig(include_dataset_files=False, preserve_dataset_uuid=True),
        destination_client=_make_client("destination"),
        user_rid_mapping={"source-user-rid": "dest-user-rid"},
        migration_state_path=tmp_path / "state.json",
    )

    with patch(
        "nominal.experimental.migration.migrator.checklist_migrator._create_checklist_with_content"
    ) as create_checklist:
        create_checklist.return_value = MagicMock(rid="dest-checklist-rid")
        run_parallel_migration(runner, max_workers=1)

    assert create_checklist.call_args.kwargs["assignee_rid"] == "dest-user-rid"


def test_data_review_is_executed_via_client_resolved_from_source_data_review() -> None:
    default_client = _make_client("default")
    review_client = _make_client("review")

    source_data_review = MagicMock()
    source_data_review.rid = f"ri.scout.{_STACK}.data-review.00000001-0000-0000-0000-000000000000"
    source_data_review.run_rid = f"ri.scout.{_STACK}.run.00000001-0000-0000-0000-000000000000"
    source_checklist = MagicMock(rid=f"ri.scout.{_STACK}.checklist.00000001-0000-0000-0000-000000000000")
    source_data_review.get_checklist.return_value = source_checklist

    source_asset = MagicMock()
    source_asset.rid = f"ri.scout.{_STACK}.asset.00000001-0000-0000-0000-000000000000"
    source_asset.search_data_reviews.return_value = [source_data_review]

    ctx = MigrationContext(
        destination_client=default_client,
        migration_state=MigrationState(),
        destination_client_resolver=lambda source_resource: review_client
        if source_resource.rid == source_data_review.rid
        else default_client,
    )
    ctx.migration_state.record_mapping(ResourceType.RUN, source_data_review.run_rid, "dest-run-rid")

    destination_checklist = MagicMock(rid="dest-checklist-rid")
    new_data_review = review_client.get_checklist.return_value.execute.return_value
    new_data_review.rid = "dest-data-review-rid"

    with patch("nominal.experimental.migration.migrator.asset_migrator.ChecklistMigrator") as checklist_migrator_cls:
        checklist_migrator_cls.return_value.copy_from.return_value = destination_checklist
        AssetMigrator(ctx)._copy_asset_checklists(source_asset)

    review_client.get_checklist.assert_called_once_with("dest-checklist-rid")
    review_client.get_checklist.return_value.execute.assert_called_once_with("dest-run-rid")
    # The checklist copied on the destination must not be executed with its own (author-bound) client.
    destination_checklist.execute.assert_not_called()
    default_client.get_checklist.assert_not_called()
    assert (
        ctx.migration_state.get_mapped_rid(ResourceType.DATA_REVIEW, source_data_review.rid) == "dest-data-review-rid"
    )


def test_data_review_execution_skips_refetch_when_clients_match() -> None:
    default_client = _make_client("default")

    source_data_review = MagicMock()
    source_data_review.rid = f"ri.scout.{_STACK}.data-review.00000002-0000-0000-0000-000000000000"
    source_data_review.run_rid = f"ri.scout.{_STACK}.run.00000002-0000-0000-0000-000000000000"
    source_data_review.get_checklist.return_value = MagicMock(
        rid=f"ri.scout.{_STACK}.checklist.00000002-0000-0000-0000-000000000000"
    )

    source_asset = MagicMock()
    source_asset.rid = f"ri.scout.{_STACK}.asset.00000002-0000-0000-0000-000000000000"
    source_asset.search_data_reviews.return_value = [source_data_review]

    ctx = MigrationContext(destination_client=default_client, migration_state=MigrationState())
    ctx.migration_state.record_mapping(ResourceType.RUN, source_data_review.run_rid, "dest-run-rid")

    destination_checklist = MagicMock(rid="dest-checklist-rid")
    destination_checklist._clients = default_client._clients
    destination_checklist.execute.return_value.rid = "dest-data-review-rid"

    with patch("nominal.experimental.migration.migrator.asset_migrator.ChecklistMigrator") as checklist_migrator_cls:
        checklist_migrator_cls.return_value.copy_from.return_value = destination_checklist
        AssetMigrator(ctx)._copy_asset_checklists(source_asset)

    default_client.get_checklist.assert_not_called()
    destination_checklist.execute.assert_called_once_with("dest-run-rid")
    assert (
        ctx.migration_state.get_mapped_rid(ResourceType.DATA_REVIEW, source_data_review.rid) == "dest-data-review-rid"
    )
