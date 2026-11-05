"""Retention migration: dataset policies, retention exemptions, and run locks."""

from __future__ import annotations

from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch

import pytest
import yaml

from nominal.core.retention import KeepForever, KeepUntilExpiry, RetentionExemption, _time_range_to_proto
from nominal.exceptions import NominalError, NominalFailedPreconditionError, NominalPermissionDeniedError
from nominal.experimental.migration import migration_cli
from nominal.experimental.migration.config.migration_data_config import AssetInclusionConfig, MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.asset_migrator import AssetMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.dataset_migrator import DatasetMigrator
from nominal.experimental.migration.migrator.retention_migrator import (
    _ALL_TIME_END,
    _ALL_TIME_START,
    RetentionMigrator,
)
from nominal.experimental.migration.parallel_migration_runner import run_parallel_migration
from nominal.experimental.migration.resource_type import ResourceType


class _Destination:
    """A destination tenant that records every retention write, in order, and can fail a chosen one."""

    def __init__(self) -> None:
        self.events: list[tuple[str, ...]] = []
        self.fail_on: dict[tuple[str, ...], Exception] = {}
        self.client = MagicMock()
        self.client.get_dataset.side_effect = self._dataset
        self.client.get_run.side_effect = self._run

    def _record(self, *event: str) -> None:
        self.events.append(event)
        if event in self.fail_on:
            raise self.fail_on[event]

    def _dataset(self, rid: str) -> MagicMock:
        dataset = MagicMock()
        dataset.upsert_retention_exemption.side_effect = lambda source_id, *_, **__: self._record(
            "exemption", rid, source_id
        )
        dataset.set_retention_policy.side_effect = lambda policy: self._record("policy", rid, str(policy.expiry_days))
        return dataset

    def _run(self, rid: str) -> MagicMock:
        run = MagicMock()
        run.lock.side_effect = lambda: self._record("lock", rid)
        return run


def _dataset(rid: str, *, policy=KeepForever(), exemptions=(), archived: bool = False) -> MagicMock:
    dataset = MagicMock(rid=rid, retention_policy=policy, is_archived=archived)
    dataset.list_retention_exemptions.return_value = list(exemptions)
    return dataset


def _run(rid: str, *, locked: bool = True, end: int | None = 200, archived: bool = False) -> MagicMock:
    return MagicMock(rid=rid, is_locked=locked, end=end, is_archived=archived)


def _exemption(dataset_rid: str, source_id: str) -> RetentionExemption:
    return RetentionExemption(dataset_rid, source_id, 100, 200, "why")


def _migrator(
    destination: _Destination | None, *, dry_run: bool = False, datasets=(), runs=()
) -> tuple[RetentionMigrator, MigrationState]:
    """A migrator whose destination has a mapping for every given dataset and run (named `dest-<source>`)."""
    state = MigrationState()
    for dataset in datasets:
        state.record_mapping(ResourceType.DATASET, dataset.rid, f"dest-{dataset.rid}")
    for run in runs:
        state.record_mapping(ResourceType.RUN, run.rid, f"dest-{run.rid}")
    ctx = MigrationContext(
        destination_client=None if destination is None else destination.client, migration_state=state, dry_run=dry_run
    )
    return RetentionMigrator(ctx), state


def test_copies_exemptions_then_locks_then_sets_policies() -> None:
    """Exemptions and locks land before any expiry policy, so no protected data is ever exposed to deletion."""
    destination = _Destination()
    datasets = [_dataset("ds-a", policy=KeepUntilExpiry(30), exemptions=[_exemption("ds-a", "manual")])]
    runs = [_run("run-1")]
    migrator, _ = _migrator(destination, datasets=datasets, runs=runs)

    migrator.copy_retention(datasets, runs)

    assert destination.events == [
        ("exemption", "dest-ds-a", "manual"),
        ("lock", "dest-run-1"),
        ("policy", "dest-ds-a", "30"),
    ]


def test_run_lock_exemptions_are_recreated_by_locking_not_copied() -> None:
    """A locked run's exemptions are named after the source run; copying them would duplicate the lock's own."""
    destination = _Destination()
    datasets = [_dataset("ds-a", exemptions=[_exemption("ds-a", "run-1"), _exemption("ds-a", "manual")])]
    runs = [_run("run-1")]
    migrator, _ = _migrator(destination, datasets=datasets, runs=runs)

    migrator.copy_retention(datasets, runs)

    assert ("exemption", "dest-ds-a", "run-1") not in destination.events
    assert ("exemption", "dest-ds-a", "manual") in destination.events
    assert ("lock", "dest-run-1") in destination.events


def test_exemption_from_an_unlocked_run_is_still_copied() -> None:
    """An exemption naming a run that is not locked is not recreated by any lock, so it must be copied."""
    destination = _Destination()
    datasets = [_dataset("ds-a", exemptions=[_exemption("ds-a", "run-1")])]
    runs = [_run("run-1", locked=False)]
    migrator, _ = _migrator(destination, datasets=datasets, runs=runs)

    migrator.copy_retention(datasets, runs)

    assert destination.events == [("exemption", "dest-ds-a", "run-1")]


def test_only_expiry_policies_are_copied() -> None:
    """A dataset with no policy already keeps its data forever; an unrecognized source policy is reported."""
    destination = _Destination()
    datasets = [
        _dataset("ds-forever"),
        _dataset("ds-unknown", policy=None),
        _dataset("ds-expiry", policy=KeepUntilExpiry(7)),
    ]
    migrator, state = _migrator(destination, datasets=datasets)

    migrator.copy_retention(datasets, [])

    assert destination.events == [("policy", "dest-ds-expiry", "7")]
    assert [(s.source_rid, s.resource_type) for s in state.skipped_resources] == [("ds-unknown", "DATASET")]


def test_archived_and_unmigrated_resources_are_left_alone() -> None:
    """Archived resources are not migrated, and a resource with no destination mapping has nothing to update."""
    destination = _Destination()
    archived = _dataset("ds-archived", policy=KeepUntilExpiry(1), archived=True)
    unmigrated = _dataset("ds-unmigrated", policy=KeepUntilExpiry(1))
    archived_run = _run("run-archived", archived=True)
    unmigrated_run = _run("run-unmigrated")
    migrator, _ = _migrator(destination)

    migrator.copy_retention([archived, unmigrated], [archived_run, unmigrated_run])

    assert destination.events == []


def test_locked_run_without_an_end_time_is_skipped_once() -> None:
    """The server cannot lock a run with no end time; it is reported once even across reruns, not retried."""
    destination = _Destination()
    runs = [_run("run-open", end=None)]
    migrator, state = _migrator(destination, runs=runs)

    migrator.copy_retention([], runs)
    migrator.copy_retention([], runs)

    assert destination.events == []
    assert [(s.source_rid, s.resource_type) for s in state.skipped_resources] == [("run-open", "RUN")]


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (NominalPermissionDeniedError("denied"), "organization admin"),
        (NominalFailedPreconditionError("not enabled"), "not be enabled"),
    ],
    ids=["permission-denied", "retention-not-enabled"],
)
def test_destination_refusal_stops_the_copy(error: Exception, message: str) -> None:
    """A permission or enablement failure would repeat for every resource, so the copy stops with its cause named."""
    destination = _Destination()
    destination.fail_on[("exemption", "dest-ds-a", "manual")] = error
    datasets = [
        _dataset("ds-a", exemptions=[_exemption("ds-a", "manual")]),
        _dataset("ds-b", exemptions=[_exemption("ds-b", "manual")]),
    ]
    migrator, state = _migrator(destination, datasets=datasets)

    with pytest.raises(NominalError, match=message):
        migrator.copy_retention(datasets, [])

    assert destination.events == [("exemption", "dest-ds-a", "manual")]
    assert state.skipped_resources == []


def test_other_failures_skip_the_resource_and_continue() -> None:
    """A failure specific to one dataset is recorded as a skip, and the remaining datasets are still copied."""
    destination = _Destination()
    destination.fail_on[("exemption", "dest-ds-a", "manual")] = NominalError("boom")
    datasets = [
        _dataset("ds-a", exemptions=[_exemption("ds-a", "manual")]),
        _dataset("ds-b", exemptions=[_exemption("ds-b", "manual")]),
    ]
    migrator, state = _migrator(destination, datasets=datasets)

    migrator.copy_retention(datasets, [])

    assert ("exemption", "dest-ds-b", "manual") in destination.events
    assert [(s.source_rid, s.resource_type) for s in state.skipped_resources] == [("ds-a", "DATASET")]


def test_source_read_failure_skips_the_dataset_instead_of_blaming_the_destination() -> None:
    """A source-side permission failure is only a skip: it must not abort the copy as a destination problem."""
    destination = _Destination()
    unreadable = _dataset("ds-a")
    unreadable.list_retention_exemptions.side_effect = NominalPermissionDeniedError("source denied")
    readable = _dataset("ds-b", exemptions=[_exemption("ds-b", "manual")])
    migrator, state = _migrator(destination, datasets=[unreadable, readable])

    migrator.copy_retention([unreadable, readable], [])

    assert destination.events == [("exemption", "dest-ds-b", "manual")]
    assert [(s.source_rid, s.resource_type) for s in state.skipped_resources] == [("ds-a", "DATASET")]


def test_exemption_from_a_locked_run_outside_the_migration_is_copied() -> None:
    """Only runs being re-locked at the destination replace their exemptions; any other run's must be copied."""
    destination = _Destination()
    datasets = [_dataset("ds-a", exemptions=[_exemption("ds-a", "run-elsewhere")])]
    migrator, _ = _migrator(destination, datasets=datasets)

    migrator.copy_retention(datasets, [])

    assert destination.events == [("exemption", "dest-ds-a", "run-elsewhere")]


@pytest.mark.parametrize("bound", [_ALL_TIME_START, _ALL_TIME_END], ids=["start", "end"])
def test_all_time_bounds_fit_the_servers_nanosecond_conversion(bound: int) -> None:
    """The server multiplies whole seconds by 1e9 in int64, so a bound whose seconds overflow is rejected."""
    seconds_and_nanos = _time_range_to_proto(bound, bound).start

    assert -(2**63) <= seconds_and_nanos.seconds * 10**9 <= 2**63 - 1
    assert -(2**63) <= seconds_and_nanos.seconds * 10**9 + seconds_and_nanos.nanos <= 2**63 - 1


def test_dry_run_reads_the_source_and_writes_nothing() -> None:
    """A dry run, which may have no destination client at all, only reports what it would copy."""
    datasets = [_dataset("ds-a", policy=KeepUntilExpiry(30), exemptions=[_exemption("ds-a", "manual")])]
    runs = [_run("run-1")]
    migrator, _ = _migrator(None, dry_run=True, datasets=datasets, runs=runs)

    with patch("nominal.experimental.migration.migrator.retention_migrator.logger") as logger:
        migrator.copy_retention(datasets, runs)

    messages = [call.args[0] % call.args[1:] for call in logger.info.call_args_list]
    assert messages == [
        "[DRY RUN] Would copy 1 retention exemption(s) of dataset ds-a",
        "[DRY RUN] Would lock run run-1",
        "[DRY RUN] Would set retention of dataset ds-a to 30 day(s)",
    ]


def _runner(tmp_path: Path, *, include_retention: bool) -> tuple[MigrationRunner, MagicMock, MagicMock, MagicMock]:
    standalone = MagicMock(rid="ds-standalone")
    scoped = MagicMock(rid="ds-scoped")
    run = MagicMock(rid="run-1")
    asset = MagicMock(rid="asset")
    asset.list_datasets.return_value = [("scope", scoped)]
    asset.list_runs.return_value = [run]
    runner = MigrationRunner(
        migration_resources=MigrationResources(
            source_assets={"asset": AssetResources(asset=asset, source_workbook_templates=[])},
            source_standalone_templates=[],
            source_standalone_datasets=[standalone],
        ),
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=False),
        asset_inclusion_config=AssetInclusionConfig(include_workbooks=False, include_retention=include_retention),
        destination_client=MagicMock(),
        migration_state_path=tmp_path / "state.json",
    )
    return runner, standalone, scoped, run


def _run_sequential(runner: MigrationRunner) -> None:
    runner.run_migration()


def _run_parallel(runner: MigrationRunner) -> None:
    run_parallel_migration(runner, max_workers=2)


@pytest.mark.parametrize("run_migration", [_run_sequential, _run_parallel], ids=["sequential", "parallel"])
def test_runners_copy_retention_for_every_migrated_dataset_and_run(
    tmp_path: Path, run_migration: Callable[[MigrationRunner], None]
) -> None:
    """Both runners hand the retention pass every asset-scoped and standalone dataset, plus the assets' runs."""
    runner, standalone, scoped, run = _runner(tmp_path, include_retention=True)

    with (
        patch.object(AssetMigrator, "copy_from"),
        patch.object(DatasetMigrator, "copy_from"),
        patch.object(RetentionMigrator, "copy_retention") as copy_retention,
    ):
        run_migration(runner)

    (datasets, runs), _ = copy_retention.call_args
    assert sorted(dataset.rid for dataset in datasets) == ["ds-scoped", "ds-standalone"]
    assert [r.rid for r in runs] == ["run-1"]


def test_runners_leave_runs_out_when_runs_are_not_migrated(tmp_path: Path) -> None:
    """Runs that were not migrated have no destination run to lock, so they are not offered to the retention pass."""
    runner, *_ = _runner(tmp_path, include_retention=True)
    runner.asset_inclusion_config = AssetInclusionConfig(
        include_workbooks=False, include_runs=False, include_retention=True
    )

    with patch.object(RetentionMigrator, "copy_retention") as copy_retention:
        runner.migrate_retention(MagicMock())

    (_, runs), _ = copy_retention.call_args
    assert runs == []


@pytest.mark.parametrize("run_migration", [_run_sequential, _run_parallel], ids=["sequential", "parallel"])
def test_runners_skip_retention_unless_included(
    tmp_path: Path, run_migration: Callable[[MigrationRunner], None]
) -> None:
    """Retention needs destination admin access, so it is off unless the config includes it."""
    runner, *_ = _runner(tmp_path, include_retention=False)

    with (
        patch.object(AssetMigrator, "copy_from"),
        patch.object(DatasetMigrator, "copy_from"),
        patch.object(RetentionMigrator, "copy_retention") as copy_retention,
    ):
        run_migration(runner)

    copy_retention.assert_not_called()


@pytest.mark.parametrize(("raw", "expected"), [({}, False), ({"include_retention": True}, True)])
def test_config_include_retention_defaults_off(tmp_path: Path, raw: dict, expected: bool) -> None:
    """include_retention is opt-in, since it changes who can run the migration."""
    config_path = tmp_path / "migration.yml"
    config_path.write_text(
        yaml.safe_dump(
            {"migration": {"name": "m", "include_dataset_files": False, "preserve_dataset_uuid": True, **raw}}
        ),
        encoding="utf-8",
    )

    _, _, _, inclusion, *_ = migration_cli._load_migration_config(MagicMock(), config_path)

    assert inclusion.include_retention is expected
