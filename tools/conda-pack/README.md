# Offline Conda prototype

Build a self-contained Conda environment on a connected machine and install it without downloads on a matching offline machine. This is a prototype. Consult reports for measured validation; a successful build alone does not establish offline or deployment compatibility.

## Build locally

Prerequisites: uv 0.9.17 or newer, Git, and Conda (Anaconda or Miniforge). The runtime uses conda-forge; Python dependencies come from PyPI. No changes are made to Anaconda base. Conda is used to create the requested runtime, while uv manages Python dependencies.

From the repository root:

```sh
uv run --project tools/conda-pack nominal-bundle build
uv run --project tools/conda-pack nominal-bundle build --sdk-version 1.180.1 --python 3.12
```

Builder dependencies use a dedicated lockfile and a seven-day resolution cooldown. Bundle dependencies resolve independently with explicit package-index settings and continue to select current compatible stable releases, without inheriting builder-project settings or using the parent repository lockfile. The explicit SDK version is optional. The first example always selects the latest stable SDK reported by PyPI. Transitive constraints can require older dependency versions; the inventory records the selected versions. Compiling packages from source is disabled.

Output: `dist/conda-pack/nominal-VERSION-PLATFORM-pyVERSION-rREVISION/`. Existing directories are never overwritten; choose another output directory or increment `--revision`. Preserve the complete directory, including checksums and setup scripts. Conda archives contain platform-specific native binaries and must be built on matching hosts.

## Build Windows from another host

```sh
uv run --project tools/conda-pack nominal-bundle ci --platform win-64 --ref codex/conda-pack-prototype
```

Requires authenticated `gh` access and the workflow registered on the default branch. Before merge, the prototype branch's path-scoped push trigger bootstraps Windows validation. After merge use `--ref main`. The CLI correlates dispatches through a unique request ID, waits for all workflow jobs, then downloads artifacts. GitHub's default-branch workflow registration restriction applies even when dispatching another ref.

Manual Actions inputs support Python 3.11 through 3.15, SDK version, and host platform. Availability is checked by attempting the build; unsupported combinations fail rather than downgrade. Python 3.15 is experimental. Non-Windows manual builds produce archives but are not declared offline validated by this Windows-first workflow.

## Offline installation

Assumptions: the machine is Windows x86-64 for the initial bundle, user-owned environments containing PyPI packages are permitted, and an Anaconda Python 3.12 or newer is available to bootstrap setup. Confirm these policy assumptions for each deployment.

Copy the whole bundle directory to approved transfer media. From Anaconda Prompt:

```powershell
powershell -File X:\bundle\setup.ps1 -Destination C:\Users\YOUR_NAME\nominal-offline
```

Alternatively run `python X:\bundle\install.py --destination C:\Users\YOUR_NAME\nominal-offline` directly. This requires no uv, Conda package installation, downloads, or elevated privileges. The destination must be new, writable, and outside the source directory. Setup checks SHA-256 hashes, extracts, repairs prefixes, and executes functional and notebook checks. Compare the checksum file with a trusted copy before running setup: bundled checksums detect corruption, not malicious replacement of the whole bundle.

Setup reads source media and writes only to the destination. It can read from a read-only encrypted HDD. Disk encryption and approved media transfer remain operator responsibilities. Do not move the installed environment after relocation; install again from the original archive into a fresh path instead.

Start notebooks with the installed environment's Python:

```powershell
C:\Users\YOUR_NAME\nominal-offline\environment\python.exe C:\Users\YOUR_NAME\nominal-offline\launch.py
```

For Anaconda Prompt use, `conda activate C:\Users\YOUR_NAME\nominal-offline\environment` selects the interpreter. Use `launch.py` for notebooks so configuration, cache, runtime, and output paths stay in the writable installation. Put API configuration outside source media; never add tokens to the bundle.

`uv run --project tools/conda-pack nominal-bundle verify PATH_TO_BUNDLE --destination NEW_PATH` performs the same installation and checks on a connected build host. This command does not itself block networking. The Windows validation job separately enforces internet egress blocking and uses a read-only VHD source on a fresh runner. Outputs distinguish functional checks from externally enforced isolation.

## Staging e2e

The opt-in workflow input `staging_e2e` runs the existing non-migration SDK e2e tests from the matching published tag. It requires repository secrets `E2E_NOMINAL_STAGING_API_KEY` and `E2E_NOMINAL_STAGING_WORKSPACE_RID`. The suite creates test resources in staging and uses its existing cleanup behavior. Production migration tests are excluded.

Test-only packages live outside the environment; constraints and a duplicate-distribution check prevent them from replacing bundled dependencies. The test checkout's SDK source is never installed or placed on the test import path. Staging test credentials are supplied after packing on a separate runner and never enter customer artifacts. Staging e2e requires network access and is distinct from offline checks.

## Compatibility and tests

```sh
uv run --project tools/conda-pack pytest -c tools/conda-pack/pyproject.toml tools/conda-pack/tests -q
uv run --project tools/conda-pack python tools/conda-pack/scripts/compatibility.py --output tools/conda-pack/reports/compatibility.json
```

The availability probe checks Conda's current runtime index and performs binary-only resolution for Python 3.11 through 3.15 against the eight targets in the streaming wheel workflow. Resolution success is not execution success. Check the report's independent runtime, binary resolution, offline execution, and staging e2e fields.

Prototype artifacts expire after 14 days. Download and retain validated deliveries through the approved process; this workflow does not publish GitHub Releases or change SDK publication behavior.
