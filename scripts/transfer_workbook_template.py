r"""Transfer a single workbook template from one Nominal workspace to another.

A thin, single-purpose wrapper around the experimental migration module
(``nominal.experimental.migration``). It clones exactly one standalone workbook
template — its metadata (title, description, labels, properties), layout,
content, and any attachments referenced by that content — from a source
workspace into a destination workspace.

"Workspace" here is whatever workspace each named config profile is bound to, so
this works both across tenants and between two workspaces of the same tenant:
for the latter, create two profiles that share a base URL and token but point at
different workspace RIDs.

One-time setup (one profile per workspace)::

    nom config profile add SOURCE \\
        --base-url <base-url> --workspace-rid <src-workspace-rid> --token <api-key>
    nom config profile add DEST \\
        --base-url <base-url> --workspace-rid <dst-workspace-rid> --token <api-key>

Usage::

    python scripts/transfer_workbook_template.py \\
        --source-profile SOURCE \\
        --destination-profile DEST \\
        --template-rid ri.scout.<stack>.notebook-template.<uuid>

Add ``--dry-run`` to log what would be created without writing anything. Pass
``--migration-state-path`` to make re-runs idempotent: an already-transferred
template is reused instead of being duplicated. By default a ``migration_state.json``
file is written to the current directory (matching ``nom migrate copy``).
"""

from __future__ import annotations

import logging
from pathlib import Path

import click

from nominal.core import NominalClient
from nominal.experimental.migration.config.migration_data_config import (
    AssetInclusionConfig,
    MigrationDatasetConfig,
)
from nominal.experimental.migration.config.migration_resources import MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger("transfer_workbook_template")


@click.command()
@click.option(
    "--source-profile",
    required=True,
    help="Named config profile for the source workspace (see `nom config profile add`).",
)
@click.option(
    "--destination-profile",
    required=True,
    help="Named config profile for the destination workspace (see `nom config profile add`).",
)
@click.option(
    "--template-rid",
    required=True,
    help="RID of the workbook template to transfer, e.g. ri.scout.<stack>.notebook-template.<uuid>.",
)
@click.option(
    "--migration-state-path",
    "migration_state_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Path to load/save migration state JSON for resumable, idempotent runs. "
        "Defaults to 'migration_state.json' in the current directory."
    ),
)
@click.option(
    "--trust-store-path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to a CA bundle for TLS. Defaults to certifi's trust store.",
)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Log what would be created without writing anything to the destination or state file.",
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase log verbosity (-v for per-resource debug logging).",
)
def main(
    source_profile: str,
    destination_profile: str,
    template_rid: str,
    migration_state_path: Path | None,
    trust_store_path: Path | None,
    dry_run: bool,
    verbose: int,
) -> None:
    """Clone a single standalone workbook template into the destination workspace."""
    _configure_logging(verbose)

    trust_store = str(trust_store_path) if trust_store_path is not None else None
    source_client = NominalClient.from_profile(source_profile, trust_store_path=trust_store)
    destination_client = NominalClient.from_profile(destination_profile, trust_store_path=trust_store)

    try:
        template = source_client.get_workbook_template(template_rid)
    except Exception as exc:
        raise click.ClickException(
            f"Could not load workbook template '{template_rid}' from source profile '{source_profile}': {exc}"
        ) from exc

    source_workspace = source_client.get_workspace()
    destination_workspace = destination_client.get_workspace()
    if source_workspace.rid == destination_workspace.rid:
        logger.warning(
            "Source and destination resolve to the same workspace (%s); this will create a duplicate template.",
            destination_workspace.rid,
        )

    logger.info("Transferring workbook template '%s' (%s)", template.title, template.rid)
    logger.info("  from workspace %s (profile '%s')", source_workspace.rid, source_profile)
    logger.info("  to   workspace %s (profile '%s')", destination_workspace.rid, destination_profile)

    # A standalone template involves no assets, runs, datasets, etc.
    resources = MigrationResources(source_assets={}, source_standalone_templates=[template])

    runner = MigrationRunner(
        migration_resources=resources,
        # Dataset options are irrelevant for a standalone template (no datasets are touched),
        # but MigrationRunner requires a config object.
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=False, include_dataset_files=False),
        destination_client=destination_client,
        # No assets are involved, so disable every per-asset child-resource pass (including the
        # deferred-workbook step) to keep this focused on the single template.
        asset_inclusion_config=AssetInclusionConfig(
            include_video=False,
            include_runs=False,
            include_events=False,
            include_attachments=False,
            include_checklists=False,
            include_workbooks=False,
        ),
        migration_state_path=migration_state_path,
        dry_run=dry_run,
    )
    runner.run_migration()

    if dry_run:
        click.echo(
            f"[dry-run] Would transfer template '{template.title}' ({template.rid}) "
            f"to workspace {destination_workspace.rid}. Nothing was written."
        )
        return

    new_rid = runner.migration_state.get_mapped_rid(ResourceType.WORKBOOK_TEMPLATE, template.rid)
    if new_rid is None:
        raise click.ClickException("Migration completed but no destination RID was recorded for the template.")

    new_template = destination_client.get_workbook_template(new_rid)
    click.echo("Workbook template transferred successfully.")
    click.echo(f"  source RID:      {template.rid}")
    click.echo(f"  destination RID: {new_rid}")
    click.echo(f"  destination URL: {new_template.nominal_url}")


def _configure_logging(verbose: int) -> None:
    """Configure root logging so migration progress is visible; -v enables debug output."""
    level = logging.DEBUG if verbose >= 1 else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")


if __name__ == "__main__":
    main()
