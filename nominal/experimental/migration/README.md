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
  --profile SOURCE \
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
- `--dry-run` — log what would be created without writing anything to the destination tenant or state file.

**`nom migrate summary`** — summarize a migration as a markdown table. Fully offline: no profiles or
tokens required. Provide exactly one source:

```sh
# Offline size check from one or more config files (repeat the flag per file)
nom migrate summary --from-config my_migration.yml

# Tally what a dry run would create from a captured log
# (the "[DRY RUN] Would create ..." lines are INFO-level, so run copy with -v or -vv)
nom migrate copy ... --dry-run -vv 2>&1 | tee dry_run.log
nom migrate summary --from-log dry_run.log

# Count what a real migration created, from its state file
nom migrate summary --from-state migration_state.json
```

Pass `--output <path>` to also append the markdown to a file (e.g. `$GITHUB_STEP_SUMMARY` in CI).

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
  standalone_checklist_rids:     # optional: clone checklist definitions, no asset/run required
    - ri.scout.main.checklist.<uuid>
  impersonation:                 # optional: create resources as mapped destination users
    enabled: true
    unmapped_source_user_behavior: service_user
    source_to_destination_user_rids:
      ri.authn.source.user.<id>: ri.authn.dest.user.<id>
```

How it works:
1. For each source asset RID in `source_asset_rids`, the script will copy the asset and all linked resources — runs, workbooks, datasets, events, videos, and checklists — into the destination tenant.
2. For each workbook template RID in `standalone_workbook_template_rids`, the script will copy the workbook template into the destination tenant.
3. For each checklist RID in `standalone_checklist_rids`, the script will clone the checklist definition into the destination tenant. No run or execution is involved — the checklist's channel references resolve at execution time against whatever run it is later run against (the run must expose matching data-source ref names and channel names, plus tags if any).

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

## Syncing missing channel data (`nom migrate sync-channels`)

`sync-channels` backfills the channel **data** an existing destination dataset is missing relative
to a source dataset, over a time window. For every source channel it compares per-bucket data
counts in the source vs. the destination (a channel absent in the destination, or present but empty
over the window, both read as "missing"), exports only the short time-ranges from the source via the
fast presigned export path, and streams the points into the destination (auto-creating series as
needed).

It then waits for ingestion to settle, re-checks, and re-streams anything still short
(`--max-retries`). Detection is idempotent, so a re-run simply syncs whatever is still missing — no
state file is kept. Anything that cannot be filled is logged with its channel, tags, and time-slice.

> **Notes:** only `DOUBLE`/`INT`/`STRING` channels are synced. Each channel is treated as a single
> series under the optional `--tag` filter (extra tag dimensions are not enumerated). Streaming is
> append-only, so a *partially* present bucket is re-streamed in full; the common empty-bucket case
> never duplicates.

Example:

```sh
nom migrate sync-channels \
  --source-profile SOURCE \
  --destination-profile DEST \
  --source-dataset-rid ri.catalog.main.dataset.<uuid> \
  --destination-dataset-rid ri.catalog.main.dataset.<uuid> \
  --start 2024-01-01T00:00:00Z \
  --end 2024-01-02T00:00:00Z \
  --bucket-seconds 3600 \
  --tag site=daq \
  --max-retries 2 \
  -vv
```
