"""Tests for the Migrator base class copy_from logging behavior (MDEV-86).

The migrator base module uses TypeVar(default=...) which requires Python 3.13+.
These tests are skipped on older Python versions.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType


@dataclass
class _FakeResource:
    rid: str
    name: str


@dataclass(frozen=True)
class _FakeCopyOptions(ResourceCopyOptions):
    pass


class _FakeMigrator(Migrator["_FakeResource", "_FakeCopyOptions"]):
    """Concrete migrator for testing."""

    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.ASSET

    def default_copy_options(self) -> _FakeCopyOptions:
        return _FakeCopyOptions()

    def _copy_from_impl(self, source: _FakeResource, options: _FakeCopyOptions) -> _FakeResource:
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, source.rid)
        if mapped_rid is not None:
            return _FakeResource(rid=mapped_rid, name=source.name)
        new = _FakeResource(rid=f"new-{source.rid}", name=source.name)
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new.rid)
        return new

    def _get_resource_name(self, resource: _FakeResource) -> str:
        return resource.name


def _make_context() -> MigrationContext:
    mock_client = MagicMock()
    mock_client._clients.workspace_rid = "ws-rid"
    mock_workspace = MagicMock()
    mock_workspace.rid = "ws-rid"
    mock_client.get_workspace.return_value = mock_workspace
    return MigrationContext(destination_client=mock_client, migration_state=MigrationState())


def test_copy_from_logs_new_created_for_new_resource(caplog: pytest.LogCaptureFixture) -> None:
    """When a resource is newly created, 'New * created' should be logged."""
    ctx = _make_context()
    migrator = _FakeMigrator(ctx)
    source = _FakeResource(rid="src-1", name="MyAsset")

    with caplog.at_level(logging.DEBUG):
        migrator.copy_from(source)

    assert any("New asset created" in record.message for record in caplog.records)


def test_copy_from_logs_found_for_already_mapped_resource(caplog: pytest.LogCaptureFixture) -> None:
    """When a resource was already migrated (mapped), 'Found' should be logged instead of 'New * created'."""
    ctx = _make_context()
    # Pre-populate migration state to simulate a previously migrated resource
    ctx.migration_state.record_mapping(ResourceType.ASSET, "src-1", "existing-rid")
    migrator = _FakeMigrator(ctx)
    source = _FakeResource(rid="src-1", name="MyAsset")

    with caplog.at_level(logging.DEBUG):
        migrator.copy_from(source)

    assert not any("New asset created" in record.message for record in caplog.records)
    assert any("Found asset" in record.message for record in caplog.records)
