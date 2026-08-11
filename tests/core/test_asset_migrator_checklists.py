"""An unpublished checklist pinned by a data review used to raise out of `_copy_asset_checklists`
and abandon every asset stage after it.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.core.exceptions import NominalChecklistNotPublishedError
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.asset_migrator import AssetMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType

_STACK = "cerulean-staging"


def _make_context() -> MigrationContext:
    destination_client = MagicMock()
    destination_client._clients.workspace_rid = "ws-rid"
    return MigrationContext(destination_client=destination_client, migration_state=MigrationState())


def _make_data_review(n: int, *, published: bool) -> MagicMock:
    data_review = MagicMock()
    data_review.rid = f"ri.scout.{_STACK}.data-review.{n:08x}-0000-0000-0000-000000000000"
    data_review.checklist_rid = f"ri.scout.{_STACK}.checklist.{n:08x}-0000-0000-0000-000000000000"
    data_review.run_rid = f"ri.scout.{_STACK}.run.{n:08x}-0000-0000-0000-000000000000"
    if published:
        checklist = MagicMock()
        checklist.rid = data_review.checklist_rid
        data_review.get_checklist.return_value = checklist
    else:
        data_review.get_checklist.side_effect = NominalChecklistNotPublishedError(
            f"cannot get checklist {data_review.checklist_rid!r}: this version has not been published"
        )
    return data_review


def _make_asset(data_reviews: list[MagicMock]) -> MagicMock:
    asset = MagicMock()
    asset.rid = f"ri.scout.{_STACK}.asset.00000001-0000-0000-0000-000000000000"
    asset.search_data_reviews.return_value = data_reviews
    return asset


def test_unpublished_checklist_is_skipped_without_raising() -> None:
    ctx = _make_context()
    asset = _make_asset([_make_data_review(1, published=False)])

    AssetMigrator(ctx)._copy_asset_checklists(asset)

    assert [(skip.resource_type, skip.reason) for skip in ctx.migration_state.skipped_resources] == [
        (
            ResourceType.CHECKLIST.value,
            f"checklist version pinned by data review {asset.search_data_reviews.return_value[0].rid} is not published",
        )
    ]


def test_one_unpublished_checklist_does_not_stop_the_others() -> None:
    ctx = _make_context()
    unpublished = _make_data_review(1, published=False)
    published = _make_data_review(2, published=True)
    asset = _make_asset([unpublished, published])

    with patch("nominal.experimental.migration.migrator.asset_migrator.ChecklistMigrator") as checklist_migrator_cls:
        checklist_migrator_cls.return_value.copy_from.return_value = MagicMock()
        AssetMigrator(ctx)._copy_asset_checklists(asset)

    # The published checklist was still copied, and only the draft was recorded as skipped.
    checklist_migrator_cls.return_value.copy_from.assert_called_once()
    assert [(skip.resource_type, skip.source_rid) for skip in ctx.migration_state.skipped_resources] == [
        (ResourceType.CHECKLIST.value, unpublished.checklist_rid),
    ]
