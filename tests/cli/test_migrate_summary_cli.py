"""End-to-end tests for `nom migrate summary` (offline; no profile or token required)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.dry_run import would_create_message
from nominal.experimental.migration.migration_cli import migrate_cmd
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import ResourceType


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _write_config(tmp_path: Path, name: str = "cfg") -> Path:
    path = tmp_path / f"{name}.yml"
    path.write_text(
        f"migration:\n  name: {name}\n  source_asset_rids:\n    - asset_rid: ri.a.1\n",
        encoding="utf-8",
    )
    return path


def test_from_config(runner: CliRunner, tmp_path: Path) -> None:
    config = _write_config(tmp_path)
    result = runner.invoke(migrate_cmd, ["summary", "--from-config", str(config)])
    assert result.exit_code == 0, result.output
    assert "| assets | 1 |" in result.output


def test_from_config_repeatable(runner: CliRunner, tmp_path: Path) -> None:
    configs = [_write_config(tmp_path, f"cfg{i}") for i in range(2)]
    args = ["summary"]
    for config in configs:
        args += ["--from-config", str(config)]
    result = runner.invoke(migrate_cmd, args)
    assert result.exit_code == 0, result.output
    assert "cfg0" in result.output
    assert "cfg1" in result.output


def test_from_log(runner: CliRunner, tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    log.write_text(would_create_message(ResourceType.ASSET) % ("A", "r"), encoding="utf-8")
    result = runner.invoke(migrate_cmd, ["summary", "--from-log", str(log)])
    assert result.exit_code == 0, result.output
    assert "| asset | 1 |" in result.output


def test_from_state(runner: CliRunner, tmp_path: Path) -> None:
    state = MigrationState()
    state.record_mapping(ResourceType.RUN, "a", "b")
    path = tmp_path / "state.json"
    path.write_text(state.to_json(), encoding="utf-8")
    result = runner.invoke(migrate_cmd, ["summary", "--from-state", str(path)])
    assert result.exit_code == 0, result.output
    assert "| run | 1 |" in result.output


def test_output_appends(runner: CliRunner, tmp_path: Path) -> None:
    config = _write_config(tmp_path)
    output = tmp_path / "summary.md"
    output.write_text("existing\n", encoding="utf-8")
    result = runner.invoke(migrate_cmd, ["summary", "--from-config", str(config), "--output", str(output)])
    assert result.exit_code == 0, result.output
    contents = output.read_text(encoding="utf-8")
    assert contents.startswith("existing\n")
    assert "| assets | 1 |" in contents


def test_no_source_is_usage_error(runner: CliRunner) -> None:
    result = runner.invoke(migrate_cmd, ["summary"])
    assert result.exit_code == 2
    assert "exactly one of" in result.output


def test_two_sources_is_usage_error(runner: CliRunner, tmp_path: Path) -> None:
    config = _write_config(tmp_path)
    log = tmp_path / "run.log"
    log.write_text("", encoding="utf-8")
    result = runner.invoke(migrate_cmd, ["summary", "--from-config", str(config), "--from-log", str(log)])
    assert result.exit_code == 2
    assert "exactly one of" in result.output
