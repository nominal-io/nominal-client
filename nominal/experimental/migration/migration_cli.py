from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Sequence

import click
import yaml
from nominal_api.scout_sandbox_api import SandboxWorkspaceService, SetDemoWorkbooksRequest

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core import ArchiveStatusFilter, Asset, NominalClient, Workbook
from nominal.experimental import as_user
from nominal.experimental.migration.config.migration_data_config import AssetInclusionConfig, MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_decorators import migration_client_options
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migrator.context import DestinationClientResolver
from nominal.experimental.migration.parallel_migration_runner import run_parallel_migration
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger(__name__)

UnmappedSourceUserBehavior = Literal["service_user"]


@dataclass(frozen=True)
class ImpersonationConfig:
    enabled: bool
    source_to_destination_user_rids: dict[str, str]
    unmapped_source_user_behavior: UnmappedSourceUserBehavior = "service_user"


def _parse_asset_entry(
    source_client: NominalClient,
    entry: Any,
    index: int,
    *,
    asset_rid: str | None = None,
    path_label: str,
) -> AssetResources:
    if not isinstance(entry, dict):
        raise click.UsageError(f"'{path_label}' must be a mapping.")

    if asset_rid is None:
        rid = entry.get("asset_rid")
        if not isinstance(rid, str) or not rid.strip():
            raise click.UsageError(f"'{path_label}.asset_rid' must be a non-empty string.")
    else:
        rid = asset_rid
        if "asset_rid" in entry and entry.get("asset_rid") != rid:
            raise click.UsageError(f"'migration.source_assets' entry {index} asset_rid does not match map key '{rid}'.")

    asset_resource = source_client.get_asset(rid)
    if not asset_resource:
        raise click.UsageError(f"Asset with RID '{rid}' not found in source client.")

    template_rids = entry.get("workbook_template_rids")
    if template_rids is None:
        template_rids = []
    elif not isinstance(template_rids, list) or not all(isinstance(t, str) and t.strip() for t in template_rids):
        raise click.UsageError(f"'{path_label}.workbook_template_rids' must be a list of strings.")

    templates = []
    for t in template_rids:
        template_resource = source_client.get_workbook_template(t)
        if not template_resource:
            raise click.UsageError(f"Workbook Template with RID '{t}' not found in source client.")
        templates.append(template_resource)

    return AssetResources(asset=asset_resource, source_workbook_templates=templates)


def _load_migration_block(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or "migration" not in raw:
        raise click.UsageError("Config must be a mapping with a top-level 'migration' key.")

    m = raw["migration"]
    if not isinstance(m, dict):
        raise click.UsageError("'migration' must be a mapping.")

    return m


def _require_non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise click.UsageError(f"'{label}' must be a non-empty string.")
    return value


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise click.UsageError(f"'{label}' must be a boolean.")
    return value


def _optional_bool(value: Any, label: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise click.UsageError(f"'{label}' must be a boolean.")
    return value


def _load_asset_resources(
    source_client: NominalClient,
    asset_rids: Any,
    source_assets: Any,
) -> dict[str, AssetResources]:
    if asset_rids is not None and source_assets is not None:
        raise click.UsageError("Provide only one of 'migration.source_asset_rids' or 'migration.source_assets'.")

    if source_assets is None:
        if not isinstance(asset_rids, list) or not asset_rids:
            raise click.UsageError("'migration.source_asset_rids' must be a non-empty list.")
        return _load_asset_resources_from_list(source_client, asset_rids)

    if not isinstance(source_assets, dict) or not source_assets:
        raise click.UsageError("'migration.source_assets' must be a non-empty mapping.")
    return _load_asset_resources_from_map(source_client, source_assets)


def _load_asset_resources_from_list(source_client: NominalClient, asset_rids: list[Any]) -> dict[str, AssetResources]:
    asset_resources_by_rid: dict[str, AssetResources] = {}
    for i, entry in enumerate(asset_rids):
        asset_resource = _parse_asset_entry(source_client, entry, i, path_label=f"migration.source_asset_rids[{i}]")
        rid = asset_resource.asset.rid
        if rid in asset_resources_by_rid:
            raise click.UsageError(f"Duplicate asset RID '{rid}' in migration.source_asset_rids.")
        asset_resources_by_rid[rid] = asset_resource
    return asset_resources_by_rid


def _load_asset_resources_from_map(
    source_client: NominalClient, source_assets: dict[Any, Any]
) -> dict[str, AssetResources]:
    asset_resources_by_rid: dict[str, AssetResources] = {}
    for rid, entry in source_assets.items():
        if not isinstance(rid, str) or not rid.strip():
            raise click.UsageError("'migration.source_assets' keys must be non-empty strings.")
        if not isinstance(entry, dict):
            raise click.UsageError(f"'migration.source_assets.{rid}' must be a mapping.")
        asset_resource = _parse_asset_entry(
            source_client,
            entry,
            0,
            asset_rid=rid,
            path_label=f"migration.source_assets.{rid}",
        )
        if rid in asset_resources_by_rid:
            raise click.UsageError(f"Duplicate asset RID '{rid}' in migration.source_assets.")
        asset_resources_by_rid[rid] = asset_resource
    return asset_resources_by_rid


def _load_standalone_templates(source_client: NominalClient, template_rids: Any) -> list[Any]:
    if template_rids is None:
        return []

    if not isinstance(template_rids, list) or not all(isinstance(t, str) and t.strip() for t in template_rids):
        raise click.UsageError("'migration.standalone_workbook_template_rids' must be a list of strings.")

    templates = []
    for t in template_rids:
        template_resource = source_client.get_workbook_template(t)
        if not template_resource:
            raise click.UsageError(f"Workbook Template with RID '{t}' not found in source client.")
        templates.append(template_resource)
    return templates


def load_impersonation_config(raw: Any) -> ImpersonationConfig | None:
    if raw is None:
        return None

    if not isinstance(raw, dict):
        raise click.UsageError("'migration.impersonation' must be a mapping.")

    enabled = raw.get("enabled", False)
    if not isinstance(enabled, bool):
        raise click.UsageError("'migration.impersonation.enabled' must be a boolean.")
    if not enabled:
        return None

    unmapped_source_user_behavior = raw.get("unmapped_source_user_behavior", "service_user")
    if unmapped_source_user_behavior != "service_user":
        raise click.UsageError("'migration.impersonation.unmapped_source_user_behavior' must be 'service_user'.")

    raw_mapping = raw.get("source_to_destination_user_rids")
    if not isinstance(raw_mapping, dict) or not raw_mapping:
        raise click.UsageError("'migration.impersonation.source_to_destination_user_rids' must be a non-empty mapping.")

    source_to_destination_user_rids: dict[str, str] = {}
    for source_user_rid, destination_user_rid in raw_mapping.items():
        if not isinstance(source_user_rid, str) or not source_user_rid.strip():
            raise click.UsageError(
                "'migration.impersonation.source_to_destination_user_rids' keys must be non-empty strings."
            )
        if not isinstance(destination_user_rid, str) or not destination_user_rid.strip():
            raise click.UsageError(
                "'migration.impersonation.source_to_destination_user_rids' values must be non-empty strings."
            )
        source_to_destination_user_rids[source_user_rid] = destination_user_rid

    return ImpersonationConfig(
        enabled=True,
        source_to_destination_user_rids=source_to_destination_user_rids,
        unmapped_source_user_behavior=unmapped_source_user_behavior,
    )


def get_source_user_rid(source_resource: Any) -> str | None:
    """Resolve the source user RID for impersonation.

    This is intentionally narrow and ordered rather than a broad alias sweep:
    prefer stable top-level creator fields on the resource wrapper, then fall back
    to the latest API object and its metadata if needed.
    """
    user_rid = _extract_user_rid_from_object(source_resource)
    if user_rid is not None:
        return user_rid

    latest_api_getter = getattr(source_resource, "_get_latest_api", None)
    if not callable(latest_api_getter):
        return None

    try:
        latest_api = latest_api_getter()
    except Exception:
        logger.debug("Unable to fetch latest API object while resolving source user RID.", exc_info=True)
        return None

    user_rid = _extract_user_rid_from_object(latest_api)
    if user_rid is not None:
        return user_rid

    return _extract_user_rid_from_object(getattr(latest_api, "metadata", None))


def build_destination_client_resolver(
    destination_client: NominalClient,
    impersonation_config: ImpersonationConfig | None,
) -> DestinationClientResolver | None:
    if impersonation_config is None or not impersonation_config.enabled:
        return None
    return ImpersonatingDestinationClientResolver(destination_client, impersonation_config)


class ImpersonatingDestinationClientResolver:
    def __init__(self, destination_client: NominalClient, impersonation_config: ImpersonationConfig) -> None:
        """Create a destination client resolver backed by impersonated client caching."""
        self._destination_client = destination_client
        self._impersonation_config = impersonation_config
        self._impersonated_clients_by_user_rid: dict[str, NominalClient] = {}
        self._lock = threading.Lock()

    def __call__(self, source_resource: Any) -> NominalClient:
        source_user_rid = get_source_user_rid(source_resource)
        source_rid = getattr(source_resource, "rid", None)
        source_type = type(source_resource).__name__

        if source_user_rid is None:
            logger.debug(
                "Using destination service user for %s (rid: %s): source user RID could not be determined.",
                source_type,
                source_rid,
            )
            return self._destination_client

        destination_user_rid = self._impersonation_config.source_to_destination_user_rids.get(source_user_rid)
        if destination_user_rid is None:
            logger.debug(
                "Using destination service user for %s (rid: %s): no destination mapping for source user %s.",
                source_type,
                source_rid,
                source_user_rid,
            )
            return self._destination_client

        with self._lock:
            if destination_user_rid not in self._impersonated_clients_by_user_rid:
                logger.debug(
                    "Creating impersonated destination client for source user %s -> destination user %s.",
                    source_user_rid,
                    destination_user_rid,
                )
                self._impersonated_clients_by_user_rid[destination_user_rid] = as_user(
                    self._destination_client, destination_user_rid
                )

        logger.debug(
            "Using impersonated destination client for %s (rid: %s): source user %s -> destination user %s.",
            source_type,
            source_rid,
            source_user_rid,
            destination_user_rid,
        )
        return self._impersonated_clients_by_user_rid[destination_user_rid]


def _extract_user_rid_from_object(value: Any) -> str | None:
    if value is None:
        return None

    for attribute_name in ("created_by_rid", "author_rid"):
        nested_value = getattr(value, attribute_name, None)
        if isinstance(nested_value, str) and nested_value.strip():
            return nested_value

    for attribute_name in ("created_by", "author"):
        nested_value = getattr(value, attribute_name, None)
        nested_user_rid = _extract_user_rid_from_identity(nested_value)
        if nested_user_rid is not None:
            return nested_user_rid

    return None


def _extract_user_rid_from_identity(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value

    if value is None:
        return None

    for attribute_name in ("user_rid", "rid"):
        nested_value = getattr(value, attribute_name, None)
        if isinstance(nested_value, str) and nested_value.strip():
            return nested_value

    return None


def _load_migration_config(
    source_client: NominalClient, config_path: Path
) -> tuple[str, MigrationResources, MigrationDatasetConfig, AssetInclusionConfig, bool, ImpersonationConfig | None]:
    with config_path.open("r", encoding="utf-8") as f:
        raw: Any = yaml.safe_load(f)

    m = _load_migration_block(raw)

    name = _require_non_empty_string(m.get("name"), "migration.name")
    include_dataset_files = _require_bool(m.get("include_dataset_files"), "migration.include_dataset_files")
    preserve_dataset_uuid = _require_bool(m.get("preserve_dataset_uuid"), "migration.preserve_dataset_uuid")

    set_to_demo_workbook_raw = m.get("set_to_demo_workbook", False)
    if not isinstance(set_to_demo_workbook_raw, bool):
        raise click.UsageError("'migration.set_to_demo_workbook' must be a boolean.")
    set_to_demo_workbook: bool = set_to_demo_workbook_raw

    asset_inclusion_config = AssetInclusionConfig(
        include_video=_optional_bool(m.get("include_video"), "migration.include_video", default=True),
        include_runs=_optional_bool(m.get("include_runs"), "migration.include_runs", default=True),
        include_events=_optional_bool(m.get("include_events"), "migration.include_events", default=True),
        include_attachments=_optional_bool(m.get("include_attachments"), "migration.include_attachments", default=True),
        include_checklists=_optional_bool(m.get("include_checklists"), "migration.include_checklists", default=True),
        include_workbooks=_optional_bool(m.get("include_workbooks"), "migration.include_workbooks", default=True),
    )

    asset_resources_by_rid = _load_asset_resources(
        source_client,
        m.get("source_asset_rids"),
        m.get("source_assets"),
    )

    standalone_workbook_templates = _load_standalone_templates(
        source_client,
        m.get("standalone_workbook_template_rids"),
    )
    impersonation_config = load_impersonation_config(m.get("impersonation"))

    dataset_config = MigrationDatasetConfig(
        preserve_dataset_uuid=preserve_dataset_uuid,
        include_dataset_files=include_dataset_files,
    )

    return (
        name,
        MigrationResources(
            source_assets=asset_resources_by_rid,
            source_standalone_templates=standalone_workbook_templates,
        ),
        dataset_config,
        asset_inclusion_config,
        set_to_demo_workbook,
        impersonation_config,
    )


def _validate_demo_workbook_metadata(source_client: NominalClient, migration_resources: MigrationResources) -> bool:
    """Validate that all source workbooks have non-empty title, description, and labels.

    Returns True if all workbooks pass validation, False otherwise.
    """
    violations: list[str] = []
    for asset_resources in migration_resources.source_assets.values():
        source_asset = asset_resources.asset
        workbooks = source_asset.search_workbooks(include_drafts=True)
        for workbook in workbooks:
            raw_notebook = workbook._get_latest_api()
            metadata = raw_notebook.metadata
            problems = []
            if not workbook.title or not workbook.title.strip():
                problems.append("empty title")
            if not workbook.description or not workbook.description.strip():
                problems.append("empty description")
            if not metadata.labels:
                problems.append("no labels")
            if problems:
                violations.append(
                    f"Workbook '{workbook.title}' (rid: {workbook.rid}) on asset '{source_asset.name}' "
                    f"(rid: {source_asset.rid}): {', '.join(problems)}"
                )

    if violations:
        for v in violations:
            logger.error("Demo workbook validation failed: %s", v)
        return False
    return True


def _create_sandbox_workspace_service(target_client: NominalClient) -> SandboxWorkspaceService:
    """Create a SandboxWorkspaceService by reusing the connection details from the target client."""
    existing_service = target_client._clients.workspace
    return SandboxWorkspaceService(
        requests_session=existing_service._requests_session,
        uris=existing_service._uris,
        _connect_timeout=existing_service._connect_timeout,
        _read_timeout=existing_service._read_timeout,
        _verify=existing_service._verify,
    )


def _update_demo_workbooks(target_client: NominalClient, runner: MigrationRunner) -> None:
    """After migration, append newly created workbook RIDs to the sandbox demo workbooks list."""
    new_workbook_rids = list(runner.migration_state.rid_mapping.get(ResourceType.WORKBOOK.value, {}).values())
    if not new_workbook_rids:
        logger.info("No new workbooks were created; skipping demo workbook update.")
        return

    workspace_rid = target_client.get_workspace().rid
    auth_header = target_client._clients.auth_header
    sandbox_service = _create_sandbox_workspace_service(target_client)

    existing_response = sandbox_service.get_demo_workbooks(auth_header, workspace_rid)
    existing_rids = existing_response.notebook_rids

    combined_rids = list(dict.fromkeys(existing_rids + new_workbook_rids))
    sandbox_service.set_demo_workbooks(auth_header, SetDemoWorkbooksRequest(notebook_rids=combined_rids), workspace_rid)
    logger.info(
        "Updated demo workbooks: %d existing + %d new = %d total",
        len(existing_rids),
        len(new_workbook_rids),
        len(combined_rids),
    )


@click.group(name="migrate", help="Commands to migrate resources between Nominal tenants.")
def migrate_cmd() -> None:
    pass


@migrate_cmd.command(name="copy", help="Copy resources from a source tenant to a destination tenant.")
@migration_client_options
@global_options
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to the migration config YAML file.",
)
@click.option(
    "--migration-state-path",
    "migration_state_path",
    required=False,
    default=None,
    type=click.Path(path_type=Path),
    help="Path to load/save migration state JSON for resumable migrations. Defaults to 'migration_state.json'.",
)
@click.option(
    "--max-workers",
    default=1,
    show_default=True,
    type=click.IntRange(min=1),
    help="Maximum number of top-level asset/template migrations to run concurrently.",
)
def copy(
    clients: tuple[NominalClient, NominalClient],
    config_path: Path,
    migration_state_path: Path | None,
    max_workers: int,
) -> None:
    source_client, target_client = clients
    logger.info("Loading migration config from: %s", config_path)
    (
        name,
        migration_resources,
        dataset_config,
        asset_inclusion_config,
        set_to_demo_workbook,
        impersonation_config,
    ) = _load_migration_config(source_client, config_path)

    if set_to_demo_workbook:
        workspace = target_client.get_workspace()
        if workspace.id != "sandbox":
            logger.error(
                "set_to_demo_workbook is true but destination workspace displayable id is '%s', expected 'sandbox'. "
                "Skipping migration.",
                workspace.id,
            )
            return
        if not _validate_demo_workbook_metadata(source_client, migration_resources):
            logger.error("Demo workbook validation failed. Skipping migration.")
            return

    logger.info(
        "Processing migration config: %s (source_assets=%d, source_standalone_templates=%d)",
        name,
        len(migration_resources.source_assets),
        len(migration_resources.source_standalone_templates),
    )
    destination_client_resolver = build_destination_client_resolver(target_client, impersonation_config)
    if destination_client_resolver is not None:
        logger.info("Destination impersonation is enabled for this migration config.")

    runner = MigrationRunner(
        migration_resources=migration_resources,
        dataset_config=dataset_config,
        asset_inclusion_config=asset_inclusion_config,
        destination_client=target_client,
        destination_client_resolver=destination_client_resolver,
        migration_state_path=migration_state_path,
    )
    run_parallel_migration(runner, max_workers=max_workers)

    if set_to_demo_workbook:
        _update_demo_workbooks(target_client, runner)


def _categorize_workbooks(workbooks: Sequence[Workbook]) -> tuple[set[str], set[str]]:
    single: set[str] = set()
    multi: set[str] = set()
    for workbook in workbooks:
        rids = workbook.run_rids or workbook.asset_rids
        if rids:
            (single if len(rids) == 1 else multi).add(workbook.rid)
    return single, multi


@migrate_cmd.command(name="prep", help="Count in-scope and out-of-scope resources and generate a migration config.")
@client_options
@global_options
@click.option(
    "--name",
    "migration_name",
    required=True,
    help="Name for the migration (written into the generated config file).",
)
@click.option(
    "--output",
    "output_path",
    required=True,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Path where the generated migration config YAML will be written.",
)
def prep(client: NominalClient, migration_name: str, output_path: Path) -> None:
    """Count resources on a tenant to determine migration scope and generate a starter config."""
    logger.info("In-scope migration numbers:")

    runs = client.search_runs()
    logger.info("  Total runs: %d", len(runs))

    workbooks = client.search_workbooks(include_drafts=True)
    workbooks_with_single_asset_run, workbooks_with_multi_asset_run = _categorize_workbooks(workbooks)

    logger.info("  Workbooks with single asset/run: %d", len(workbooks_with_single_asset_run))

    manually_created_assets: Sequence[Asset] = client.search_assets()
    logger.info("  Manually-created assets: %d", len(manually_created_assets))

    manually_created_asset_rids = {a.rid for a in manually_created_assets}
    all_assets: set[str] = set(manually_created_asset_rids)
    auto_created_assets: set[str] = set()

    for run in runs:
        asset_rid = run.assets[0]
        if asset_rid not in manually_created_asset_rids:
            auto_created_assets.add(asset_rid)
            all_assets.add(asset_rid)

    logger.info("  Auto-created assets: %d", len(auto_created_assets))
    logger.info("  All assets: %d", len(all_assets))

    datasets = client.search_datasets()
    logger.info("  Total datasets: %d", len(datasets))

    datasets_with_assets: set[str] = set()
    videos = client.search_videos()

    logger.info("  Total videos: %d", len(videos))

    channel_count = 0

    for asset_rid in all_assets:
        asset = client.get_asset(asset_rid)
        for _data_scope, dataset in asset.list_datasets():
            datasets_with_assets.add(dataset.rid)
            channel_count += sum(1 for _ in dataset.search_channels())

    logger.info("  Datasets with assets: %d", len(datasets_with_assets))
    logger.info("  Total channels: %d", channel_count)

    orphaned_datasets: set[str] = {d.rid for d in datasets if d.rid not in datasets_with_assets}

    logger.info("Out-of-scope migration numbers:")
    logger.info("  Workbooks with multiple asset/runs: %d", len(workbooks_with_multi_asset_run))
    logger.info("  Orphaned datasets: %d", len(orphaned_datasets))

    streaming_checklists = client.list_streaming_checklists()
    logger.info("  Streaming checklists: %d", len(streaming_checklists))

    containerized_extractors = client.search_containerized_extractors()
    logger.info("  Containerized extractors: %d", len(containerized_extractors))

    workbook_templates = client.search_workbook_templates(archive_status=ArchiveStatusFilter.NOT_ARCHIVED)
    logger.info("  Workbook templates (non-archived): %d", len(workbook_templates))

    config = {
        "migration": {
            "name": migration_name,
            "include_dataset_files": False,
            "preserve_dataset_uuid": True,
            "include_video": True,
            "include_runs": True,
            "include_events": True,
            "include_attachments": True,
            "include_checklists": True,
            "include_workbooks": True,
            "set_to_demo_workbook": False,
            "source_asset_rids": [{"asset_rid": rid} for rid in sorted(all_assets)],
            "standalone_workbook_template_rids": [t.rid for t in workbook_templates],
        }
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    logger.info("Wrote migration config to %s", output_path)
