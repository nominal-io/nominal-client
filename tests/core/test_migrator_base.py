"""Tests for the Migrator base class copy_from logging behavior (MDEV-86).

The migrator base module uses TypeVar(default=...) which requires Python 3.13+.
These tests are skipped on older Python versions.
"""

from __future__ import annotations

import concurrent.futures
import logging
import sys
import threading
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

    def _get_existing_destination_resource(self, destination_client: MagicMock, mapped_rid: str) -> _FakeResource:
        return _FakeResource(rid=mapped_rid, name="MyAsset")

    def _copy_from_impl(self, source: _FakeResource, options: _FakeCopyOptions) -> _FakeResource:
        existing = self.get_existing_destination_resource(source)
        if existing is not None:
            return _FakeResource(rid=existing.rid, name=source.name)
        new = _FakeResource(rid=f"new-{source.rid}", name=source.name)
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new.rid)
        return new

    def _get_resource_name(self, resource: _FakeResource) -> str:
        return resource.name


class _SingleflightFakeMigrator(_FakeMigrator):
    def __init__(
        self,
        ctx: MigrationContext,
        *,
        started: threading.Event | None = None,
        release: threading.Event | None = None,
        fail_once: threading.Event | None = None,
    ) -> None:
        super().__init__(ctx)
        self.started = started
        self.release = release
        self.fail_once = fail_once
        self.copy_count = 0
        self._copy_lock = threading.Lock()

    def use_singleflight(self) -> bool:
        return True

    def _copy_from_impl(self, source: _FakeResource, options: _FakeCopyOptions) -> _FakeResource:
        with self._copy_lock:
            self.copy_count += 1
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            self.release.wait(timeout=5)
        if self.fail_once is not None and self.fail_once.is_set():
            self.fail_once.clear()
            raise RuntimeError("boom")
        return super()._copy_from_impl(source, options)


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


def test_copy_from_singleflight_deduplicates_concurrent_calls() -> None:
    ctx = _make_context()
    source = _FakeResource(rid="src-1", name="MyAsset")
    started = threading.Event()
    release = threading.Event()
    migrator = _SingleflightFakeMigrator(ctx, started=started, release=release)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(migrator.copy_from, source)
        assert started.wait(timeout=5)
        future_b = executor.submit(migrator.copy_from, source)
        release.set()
        result_a = future_a.result(timeout=5)
        result_b = future_b.result(timeout=5)

    assert result_a.rid == "new-src-1"
    assert result_b.rid == "new-src-1"
    assert migrator.copy_count == 1
    assert ctx.migration_state.get_mapped_rid(ResourceType.ASSET, "src-1") == "new-src-1"


def test_copy_from_singleflight_retries_after_failure() -> None:
    ctx = _make_context()
    source = _FakeResource(rid="src-1", name="MyAsset")
    started = threading.Event()
    release = threading.Event()
    fail_once = threading.Event()
    fail_once.set()
    migrator = _SingleflightFakeMigrator(ctx, started=started, release=release, fail_once=fail_once)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(migrator.copy_from, source)
        assert started.wait(timeout=5)
        future_b = executor.submit(migrator.copy_from, source)
        release.set()
        with pytest.raises(RuntimeError, match="boom"):
            future_a.result(timeout=5)
        with pytest.raises(RuntimeError, match="boom"):
            future_b.result(timeout=5)

    release = threading.Event()
    retry_migrator = _SingleflightFakeMigrator(ctx, release=release)
    release.set()
    result = retry_migrator.copy_from(source)

    assert result.rid == "new-src-1"
    assert retry_migrator.copy_count == 1
    assert ctx.migration_state.get_mapped_rid(ResourceType.ASSET, "src-1") == "new-src-1"


def test_copy_from_singleflight_propagates_base_exception_to_waiters() -> None:
    ctx = _make_context()
    source = _FakeResource(rid="src-1", name="MyAsset")
    started = threading.Event()
    release = threading.Event()

    class _KeyboardInterruptMigrator(_SingleflightFakeMigrator):
        def _copy_from_impl(self, source: _FakeResource, options: _FakeCopyOptions) -> _FakeResource:
            if self.started is not None:
                self.started.set()
            if self.release is not None:
                self.release.wait(timeout=5)
            raise KeyboardInterrupt("stop")

    migrator = _KeyboardInterruptMigrator(ctx, started=started, release=release)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(migrator.copy_from, source)
        assert started.wait(timeout=5)
        future_b = executor.submit(migrator.copy_from, source)
        release.set()
        with pytest.raises(KeyboardInterrupt, match="stop"):
            future_a.result(timeout=5)
        with pytest.raises(KeyboardInterrupt, match="stop"):
            future_b.result(timeout=5)
