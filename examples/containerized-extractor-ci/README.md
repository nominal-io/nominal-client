# Containerized Extractor CI Examples

These examples show a generic image create/update flow for Nominal containerized extractors.

They are templates. Copy the workflow for your CI provider into your extractor repository, then set provider secrets and variables.

## Required CI Values

Store these as CI secrets or protected variables:

| Name | Purpose |
| --- | --- |
| `NOMINAL_API_KEY` | Nominal API token |
| `NOMINAL_API_URL` | Nominal API base URL |
| `NOMINAL_WORKSPACE_RID` | Target workspace RID |

Set `NOMINAL_SDK_REF` or the GitHub workflow input `nominal-sdk-ref` to the Nominal Python SDK branch, tag, or commit SHA you want to test.

## Flow

1. Build one Docker tarball per platform.
2. Use immutable tags shaped like `<branch>-<short-sha>-<platform>`.
3. Install the Nominal Python SDK from a Git ref.
4. Upsert the extractor by exact name (create, recovering from concurrent creation via `NominalAlreadyExistsError`).
5. Register each image tarball under its tag. Re-runs skip the upload when the tag is already registered with the same contract; a concurrent registration of the same tag is detected and reused.
6. Activate the image for the selected runtime platform with `set_active_image` (which verifies the image is `READY`).

The examples do not push to any external registry. They upload Docker tarballs through the Nominal SDK, and registration pushes the image into Nominal's own registry within the `register_image` call.

The GitLab template assumes a runner that permits privileged Docker commands for Docker-in-Docker and QEMU setup.

## Registry Semantics

Nominal container image tags are **immutable**: registering an already-registered `(extractor, tag)` raises `NominalAlreadyExistsError`. Use a new tag for every source revision — the sample workflows generate tags from the Git branch, short commit SHA, and platform, so re-runs of the same revision reuse the registered image instead of re-uploading.

Only one container image is active on a containerized extractor at a time. The sample workflows register one image per platform, then activate the platform selected by `ACTIVE_PLATFORM`. Set it to the runtime platform for the target Nominal deployment, usually `linux/amd64`. When registering exactly one image, that platform is the default.

Older registry deployments may reject repeated uploads of a Docker layer that already exists in the workspace. Set `squash_before_registering` to `true` in the config, or pass `--squash-before-registering`, to flatten the Docker archive to a single layer before upload (current deployments accept shared layers, so this is normally unnecessary). Squashing uses local Docker load, container export, image import, and image save; it requires the Docker CLI and removes layer sharing/history from the uploaded archive.

## Files

| File | Use |
| --- | --- |
| `github-actions.yml` | Copy to `.github/workflows/deploy-containerized-extractor.yml` |
| `gitlab-ci.yml` | Copy to `.gitlab-ci.yml` or include from an existing GitLab pipeline |
| `deploy_containerized_extractor.py` | CI helper composing SDK client primitives |
| `extractor-config.example.json` | Generic extractor contract |

## Notes

- `output_format` defaults to `MANIFEST` in the example config.
- Timestamp metadata uses absolute epoch microseconds; it becomes the image's `default_timestamp_metadata`, which individual ingests (and, for MANIFEST extractors, individual manifest outputs) may override.
- File suffixes should omit leading dots.
- Set `ACTIVE_PLATFORM` to the platform that should run in the target deployment.
- Docker Buildx exports one Docker archive per platform because tarball upload workflows should not depend on an external container registry.
