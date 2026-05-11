# Resource Migration

> **NOTE:** This is an experimental tool; consult your Nominal representative before using.

A CLI to copy resources from one Nominal tenant to another.

## Features supported

1. CSV/Parquet Dataset file cloning
2. Asset cloning
3. Workbook Template cloning
4. Workbook cloning
5. Event cloning
6. Video cloning
7. Checklist cloning
8. Run cloning

## One-time setup

Install the `nominal` package (>=1.135.0):

```sh
pip install "nominal>=1.135.0"
```

Add a named profile for each tenant you want to connect to. Use the base URL for your Nominal deployment
(e.g. `https://api.nominal.io/api` for standard cloud, `https://api.gov.nominal.io/api` for GovCloud):

```sh
nom config profile add <profile-name> \
  --base-url <your-nominal-base-url> \
  --workspace-rid <workspace-rid> \
  --token <api-key>
```

## Commands

**`nom migrate prep`** — count in-scope resources and generate a starter config:

```sh
nom migrate prep \
  --source-profile SOURCE \
  --name my_migration \
  --output my_migration.yml
```

**`nom migrate copy`** — copy resources from a source tenant to a destination tenant:

```sh
nom migrate copy \
  --source-profile SOURCE \
  --destination-profile DEST \
  --config my_migration.yml
```

Optional flags:
- `--migration-state-path <path>` — path to load/save migration state JSON for resumable migrations.
  - Defaults to `migration_state.json` in the current directory.
  - On re-run, already-migrated resources are skipped so it is safe to resume after a failure.
  - Previous state files are automatically versioned (e.g. `migration_state.json` → `migration_state_v2.json`) so no history is lost.
- `--max-workers <n>` — number of assets/templates to migrate concurrently (default: 1). Start with 2–4
  workers and adjust based on performance and API rate limits.

## Understanding the config YAML

The config YAML determines what assets and standalone workbook templates to migrate. All boolean fields
use YAML `true`/`false` values.

```yaml
migration:
  name: "my migration"
  include_dataset_files: false   # true to copy dataset file contents
  preserve_dataset_uuid: true    # true to keep dataset UUIDs identical across tenants
  source_asset_rids:
    - asset_rid: ri.scout.main.asset.<uuid>
  standalone_workbook_template_rids:
    - ri.scout.main.template.<uuid>
  impersonation:                 # optional: create resources as mapped destination users
    enabled: true
    unmapped_source_user_behavior: service_user
    source_to_destination_user_rids:
      ri.authn.source.user.<id>: ri.authn.dest.user.<id>
```

How it works:
1. For each source asset RID in `source_asset_rids`, the script will copy the asset and all linked resources — runs, workbooks, datasets, events, videos, and checklists — into the destination tenant.
2. For each workbook template RID in `standalone_workbook_template_rids`, the script will copy the workbook template into the destination tenant.

Misc. configs:
1. `include_dataset_files` — if `true`, copies all dataset files attached to a dataset into the destination. Typically `true` for demo hydration and `false` for tenant migration (which relies on a separate Clickhouse backup).
2. `preserve_dataset_uuid` — if `true`, the dataset UUID is the same between source and destination. Typically `false` for demo hydration and `true` for tenant migration.
3. `impersonation` — optional block for creating migrated resources on behalf of mapped destination users. When enabled:
   - `source_to_destination_user_rids` maps source user RIDs to destination user RIDs.
   - The destination profile should be a service user with permission to impersonate destination users.
   - Resources whose source user has no mapping are created as the destination service user.

## Resumable Migrations

The `nom migrate copy` command supports resumable migrations via the optional `--migration-state-path` flag.

- On each run, a JSON file is written to the specified path recording the old→new RID mappings for every successfully migrated resource.
- If the state file already exists from a previous run, already-migrated resources are automatically skipped, so it is safe to re-run after a failure without duplicating resources.
- Previous state files are automatically versioned (e.g. `migration_state.json` → `migration_state_v2.json`) so no history is lost.

Example — run with an explicit state path:

```sh
nom migrate copy \
  --source-profile SOURCE \
  --destination-profile DEST \
  --config my_migration.yml \
  --migration-state-path ./my_migration_state.json \
  -vv
```

Example — run with parallel top-level migration workers:

```sh
nom migrate copy \
  --source-profile SOURCE \
  --destination-profile DEST \
  --config my_migration.yml \
  --max-workers 4 \
  -vv
```
