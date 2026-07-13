"""Produce a human-readable summary of a migration as a markdown table.

Three modes, all fully offline (no profiles, no tokens, no network):

* from a config YAML — static counts (assets, workbook templates, standalone checklists) for a
  quick size check before running a migration.
* from a captured ``nom migrate copy --dry-run`` log — tallies the ``[DRY RUN] Would create``
  lines by resource type, using the same shared vocabulary the migrators log with
  (see :mod:`nominal.experimental.migration.dry_run`).
* from a migration-state JSON — counts old->new RID mappings by resource type to report what a
  real (non-dry-run) migration actually created.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

import click
import yaml

from nominal.experimental.migration.dry_run import dry_run_create_pattern
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import format_resource_label


def load_migration_block(raw: Any) -> dict[str, Any]:
    """Return the top-level ``migration`` mapping of a parsed config, or raise a usage error."""
    if not isinstance(raw, dict) or "migration" not in raw:
        raise click.UsageError("Config must be a mapping with a top-level 'migration' key.")

    m = raw["migration"]
    if not isinstance(m, dict):
        raise click.UsageError("'migration' must be a mapping.")

    return m


def render_summary_table(title: str, counts: Mapping[str, int]) -> str:
    """Render resource-type counts as a markdown table with a total row."""
    lines = [f"## {title}", "", "| Resource type | Count |", "| --- | ---: |"]
    if not counts:
        lines.append("| _(none)_ | 0 |")
    else:
        for resource_type in sorted(counts):
            lines.append(f"| {resource_type} | {counts[resource_type]} |")
        lines.append(f"| **Total** | **{sum(counts.values())}** |")
    return "\n".join(lines) + "\n"


def summarize_config(path: Path) -> tuple[str, dict[str, int]]:
    """Static counts from a config YAML, without contacting any tenant."""
    with path.open("r", encoding="utf-8") as f:
        raw: Any = yaml.safe_load(f)
    migration = load_migration_block(raw)

    counts: dict[str, int] = {}

    source_asset_rids = migration.get("source_asset_rids")
    source_assets = migration.get("source_assets")
    # Mirror _load_asset_resources: whatever `nom migrate copy` rejects, the size check must
    # reject too rather than silently reporting misleading counts.
    if source_asset_rids is not None and source_assets is not None:
        raise click.UsageError("Provide only one of 'migration.source_asset_rids' or 'migration.source_assets'.")
    if source_asset_rids is not None and not isinstance(source_asset_rids, list):
        raise click.UsageError("'migration.source_asset_rids' must be a list.")
    if source_assets is not None and (not isinstance(source_assets, dict) or not source_assets):
        raise click.UsageError("'migration.source_assets' must be a non-empty mapping.")
    template_total = 0
    if isinstance(source_asset_rids, list):
        counts["assets"] = len(source_asset_rids)
        entries: list[Any] = source_asset_rids
    elif isinstance(source_assets, dict):
        counts["assets"] = len(source_assets)
        entries = list(source_assets.values())
    else:
        counts["assets"] = 0
        entries = []
    for entry in entries:
        if isinstance(entry, dict) and isinstance(entry.get("workbook_template_rids"), list):
            template_total += len(entry["workbook_template_rids"])

    if template_total:
        counts["asset workbook templates"] = template_total

    standalone_templates = migration.get("standalone_workbook_template_rids")
    if isinstance(standalone_templates, list):
        counts["standalone workbook templates"] = len(standalone_templates)

    standalone_checklists = migration.get("standalone_checklist_rids")
    if isinstance(standalone_checklists, list):
        counts["standalone checklists"] = len(standalone_checklists)

    name = migration.get("name") or path.name
    return f"Config size check: {name} ({path.name})", counts


def summarize_log(path: Path) -> tuple[str, dict[str, int]]:
    """Tally dry-run "would create" lines from a captured ``nom migrate copy --dry-run`` log."""
    pattern = dry_run_create_pattern()
    counts: Counter[str] = Counter()
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                counts[match.group("type").lower()] += 1
    return "Dry-run summary — resources that would be created", dict(counts)


def summarize_state(path: Path) -> tuple[str, dict[str, int]]:
    """Count old->new RID mappings per resource type from a migration-state JSON file."""
    state = MigrationState.from_json(path.read_text(encoding="utf-8"))
    counts = {
        format_resource_label(resource_type): len(mapping)
        for resource_type, mapping in state.rid_mapping.items()
        if isinstance(mapping, dict)
    }
    return "Migration summary — resources created", counts


def build_summary(
    config_paths: tuple[Path, ...],
    log_path: Path | None,
    state_path: Path | None,
) -> str:
    """Build the full markdown summary for exactly one of the three source modes.

    Raises:
        click.UsageError: if not exactly one source mode is provided.
    """
    provided = sum([bool(config_paths), log_path is not None, state_path is not None])
    if provided != 1:
        raise click.UsageError("Provide exactly one of --from-config, --from-log, or --from-state.")

    sections: list[str] = []
    try:
        if config_paths:
            for config_path in config_paths:
                title, counts = summarize_config(config_path)
                sections.append(render_summary_table(title, counts))
        elif log_path is not None:
            title, counts = summarize_log(log_path)
            sections.append(render_summary_table(title, counts))
        elif state_path is not None:
            title, counts = summarize_state(state_path)
            sections.append(render_summary_table(title, counts))
    except (OSError, ValueError, yaml.YAMLError, json.JSONDecodeError) as exc:
        raise click.ClickException(f"could not build migration summary: {exc}") from exc

    return "\n".join(sections)
