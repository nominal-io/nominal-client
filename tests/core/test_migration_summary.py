"""Tests for the offline migration summary helpers."""

from __future__ import annotations

import sys
from pathlib import Path

import click
import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.dry_run import DRY_RUN_PREFIX, dry_run_create_pattern, would_create_message
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migration_summary import (
    build_summary,
    load_migration_block,
    render_summary_table,
    summarize_config,
    summarize_log,
    summarize_state,
)
from nominal.experimental.migration.resource_type import ResourceType, resource_label


def test_dry_run_prefix_is_stable() -> None:
    """External consumers grep run logs for this marker — changing it is a breaking change."""
    assert DRY_RUN_PREFIX == "[DRY RUN]"


class TestRenderSummaryTable:
    def test_empty_counts(self) -> None:
        table = render_summary_table("Empty", {})
        assert "| _(none)_ | 0 |" in table
        assert "**Total**" not in table

    def test_rows_sorted_with_total(self) -> None:
        table = render_summary_table("T", {"run": 2, "asset": 3})
        lines = table.splitlines()
        assert lines.index("| asset | 3 |") < lines.index("| run | 2 |")
        assert "| **Total** | **5** |" in table


class TestSummarizeConfig:
    def _write(self, tmp_path: Path, content: str) -> Path:
        path = tmp_path / "config.yml"
        path.write_text(content, encoding="utf-8")
        return path

    def test_list_form_with_per_asset_templates(self, tmp_path: Path) -> None:
        path = self._write(
            tmp_path,
            """
migration:
  name: list-form
  source_asset_rids:
    - asset_rid: ri.a.1
      workbook_template_rids: [ri.t.1, ri.t.2]
    - asset_rid: ri.a.2
  standalone_workbook_template_rids: [ri.t.3]
  standalone_checklist_rids: [ri.c.1, ri.c.2]
""",
        )
        title, counts = summarize_config(path)
        assert "list-form" in title
        assert counts == {
            "assets": 2,
            "asset workbook templates": 2,
            "standalone workbook templates": 1,
            "standalone checklists": 2,
        }

    def test_map_form(self, tmp_path: Path) -> None:
        path = self._write(
            tmp_path,
            """
migration:
  name: map-form
  source_assets:
    ri.a.1:
      workbook_template_rids: [ri.t.1]
    ri.a.2: {}
""",
        )
        _, counts = summarize_config(path)
        assert counts == {"assets": 2, "asset workbook templates": 1}

    def test_no_assets(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "migration:\n  name: empty\n")
        _, counts = summarize_config(path)
        assert counts == {"assets": 0}

    def test_both_asset_forms_raises(self, tmp_path: Path) -> None:
        """`nom migrate copy` rejects configs with both forms; the summary must too."""
        path = self._write(
            tmp_path,
            """
migration:
  name: both-forms
  source_asset_rids: []
  source_assets:
    ri.a.1: {}
""",
        )
        with pytest.raises(click.UsageError):
            summarize_config(path)

    def test_missing_migration_key_raises(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "not_migration: {}\n")
        with pytest.raises(click.UsageError):
            summarize_config(path)

    def test_non_mapping_migration_raises(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "migration: [1, 2]\n")
        with pytest.raises(click.UsageError):
            summarize_config(path)


class TestLoadMigrationBlock:
    def test_returns_migration_mapping(self) -> None:
        assert load_migration_block({"migration": {"name": "x"}}) == {"name": "x"}

    def test_rejects_non_mapping_document(self) -> None:
        with pytest.raises(click.UsageError):
            load_migration_block(["migration"])


class TestSummarizeLog:
    def test_counts_every_resource_label(self, tmp_path: Path) -> None:
        """Every resource type's would-create line round-trips through the shared pattern."""
        lines = [would_create_message(rt) % (f"name-{rt.value}", f"rid-{rt.value}") for rt in ResourceType]
        log = tmp_path / "run.log"
        log.write_text("\n".join(lines), encoding="utf-8")
        _, counts = summarize_log(log)
        assert counts == {resource_label(rt): 1 for rt in ResourceType}

    def test_multiword_label_not_truncated(self, tmp_path: Path) -> None:
        """Regression: 'workbook template' lines must not be miscounted as 'workbook'."""
        log = tmp_path / "run.log"
        log.write_text(would_create_message(ResourceType.WORKBOOK_TEMPLATE) % ("T", "r"), encoding="utf-8")
        _, counts = summarize_log(log)
        assert counts == {"workbook template": 1}

    def test_non_create_dry_run_lines_ignored(self, tmp_path: Path) -> None:
        log = tmp_path / "run.log"
        log.write_text(
            "\n".join(
                [
                    f"{DRY_RUN_PREFIX} Would add 3 attachment(s) to asset 'A'",
                    f"{DRY_RUN_PREFIX} Would copy dataset file f1 to destination",
                    f"{DRY_RUN_PREFIX} Would execute checklist 'C' against run r1",
                    f"{DRY_RUN_PREFIX} Skipping migration state write to state.json",
                    "Copying events for asset A (rid: r1)",
                ]
            ),
            encoding="utf-8",
        )
        _, counts = summarize_log(log)
        assert counts == {}

    def test_pattern_matches_amid_log_formatting(self) -> None:
        line = f"2026-07-08 12:00:00 INFO {would_create_message(ResourceType.ASSET) % ('A', 'r')}"
        match = dry_run_create_pattern().search(line)
        assert match is not None
        assert match.group("type") == "asset"


class TestSummarizeState:
    def test_counts_rid_mappings_snake_case(self, tmp_path: Path) -> None:
        state = MigrationState()
        state.record_mapping(ResourceType.ASSET, "a1", "b1")
        state.record_mapping(ResourceType.ASSET, "a2", "b2")
        state.record_mapping(ResourceType.WORKBOOK_TEMPLATE, "t1", "t2")
        path = tmp_path / "state.json"
        path.write_text(state.to_json(), encoding="utf-8")
        _, counts = summarize_state(path)
        assert counts == {"asset": 2, "workbook template": 1}

    def test_counts_rid_mappings_camel_case(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text('{"ridMapping": {"RUN": {"a": "b"}}}', encoding="utf-8")
        _, counts = summarize_state(path)
        assert counts == {"run": 1}


class TestBuildSummary:
    def test_requires_exactly_one_source(self, tmp_path: Path) -> None:
        with pytest.raises(click.UsageError):
            build_summary((), None, None)
        log = tmp_path / "run.log"
        log.write_text("", encoding="utf-8")
        state = tmp_path / "state.json"
        state.write_text("{}", encoding="utf-8")
        with pytest.raises(click.UsageError):
            build_summary((), log, state)

    def test_invalid_json_becomes_click_exception(self, tmp_path: Path) -> None:
        state = tmp_path / "state.json"
        state.write_text("not-json", encoding="utf-8")
        with pytest.raises(click.ClickException):
            build_summary((), None, state)

    def test_multiple_configs_render_one_section_each(self, tmp_path: Path) -> None:
        paths = []
        for i in range(2):
            path = tmp_path / f"c{i}.yml"
            path.write_text(f"migration:\n  name: cfg-{i}\n", encoding="utf-8")
            paths.append(path)
        rendered = build_summary(tuple(paths), None, None)
        assert "cfg-0" in rendered
        assert "cfg-1" in rendered
