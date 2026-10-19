# E2E scheduling

The general CI job uses four pytest-xdist workers with `--dist=loadgroup`.
`just test-e2e PROFILE` uses the same scheduling. Migration has its own recipe
and stays serial; the separate AWS/Azure filename jobs also stay serial.

Tests sharing session-scoped backend resources must stay on one worker:

- `search`: the search module shares active and archived search resources.
- `derived`: the derived-dataset module shares its ingested source datasets.
- `readonly_dataset`: channel, pandas, and markings tests share `ingested_dataset`.

Add the corresponding `xdist_group` mark when adding a consumer of these fixtures.
Session fixtures are per worker, so splitting a group duplicates setup and cleanup.
Other tests use independent resources and can run on any worker. Ingestion order
within fixtures is unchanged.

For serial diagnosis, run `uv run pytest tests/e2e --ignore=tests/e2e/migration
--profile PROFILE --no-cov -n 0` (on one line).
