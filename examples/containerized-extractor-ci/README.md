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
4. Upsert the extractor by exact name.
5. Register each image tarball with `reuse_existing=True`.
6. Wait until the image is `READY`.
7. Activate the selected runtime platform.

The examples do not push to any external registry. They upload Docker tarballs through the Nominal SDK.

The GitLab template assumes a runner that permits privileged Docker commands for Docker-in-Docker and QEMU setup.

## Current Registry Limits

Nominal container image tags are immutable. Use a new tag for every source revision. The sample workflows generate
tags from the Git branch, short commit SHA, and platform.

Only one container image RID is active on a containerized extractor. The sample workflows register one image per
platform, then activate the platform selected by `ACTIVE_PLATFORM`. Set it to the runtime platform for the target
Nominal deployment, usually `linux/amd64`. When registering exactly one image, the helper activates that image by
default.

Some registry deployments can reject repeated uploads of a Docker layer that already exists in the workspace. Set
`squash_before_registering` to `true` in the config, or pass `--squash-before-registering`, to make the SDK flatten
the Docker archive before upload. This uses Docker load, container export, image import, and image save locally. It
requires the Docker CLI and removes layer sharing/history from the uploaded archive.

Use these sample workflows when setting up GitHub or GitLab repositories that publish Nominal registry container images
and create or update containerized extractors.

## Files

| File | Use |
| --- | --- |
| `github-actions.yml` | Copy to `.github/workflows/deploy-containerized-extractor.yml` |
| `gitlab-ci.yml` | Copy to `.gitlab-ci.yml` or include from an existing GitLab pipeline |
| `deploy_containerized_extractor.py` | CI helper using SDK client primitives |
| `extractor-config.example.json` | Generic extractor contract |

## Notes

- `output_format` defaults to `MANIFEST` in the example config.
- Timestamp metadata uses absolute epoch microseconds.
- File suffixes should omit leading dots.
- Set `ACTIVE_PLATFORM` to the platform that should run in the target deployment.
- Docker Buildx exports one Docker archive per platform because tarball upload workflows should not depend on an external container registry.
