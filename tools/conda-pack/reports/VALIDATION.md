# Prototype validation

Validated on 2026-11-02. This is prototype evidence, not deployment acceptance or a supported-platform guarantee.

## Windows bundle

[SDK repository workflow run](https://github.com/nominal-io/nominal-client/actions/runs/41490580563), source commit `dd8fa3b`:

- Windows x86-64, Python 3.12: build passed.
- Fresh-host installation and relocation from a read-only VHD passed.
- Functional checks and notebook execution with the offline firewall rule active passed.
- Source-media write probe failed as expected, and source hashes remained unchanged.
- Evidence artifact upload passed after restoring networking and clearing DNS cache.
- Staging e2e was not run. Deployment acceptance remains unverified.

The bundle and evidence are downloadable from the workflow run. Prototype Actions artifacts expire after 14 days.

## Local checks

- `just verify`: 1,886 tests passed, one skipped; formatting, mypy, and Ruff passed.
- Standalone tool: four archive-safety and checksum tests passed using its own pytest configuration.
- `actionlint .github/workflows/conda-pack.yml`: passed.
- Rebuilt SDK wheel and source distribution: the builder and its README are excluded by explicit `tools/**` packaging exclusions.
- Root SDK lockfile and SDK publication workflows are unchanged.

The final follow-up changes add explicit SDK distribution exclusions and consistent standalone import grouping. They do not change the Windows runtime payload or offline verification procedure tested in the workflow above.

The compatibility availability report is carried forward from the original prototype. Its resolution results are separate from Windows execution evidence; other platforms and Python versions have not passed execution validation here.
