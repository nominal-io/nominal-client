"""Deploy Nominal containerized extractor images from Docker tarballs in CI.

Composes SDK primitives into an idempotent deploy flow:

1. Upsert the extractor by exact name (create, recovering from a concurrent create via
   `NominalAlreadyExistsError`).
2. For each platform tarball, register it under an immutable tag — skipping the upload entirely
   when an image with that tag is already registered (safe CI re-runs).
3. Activate the image for the selected runtime platform.

Tags are immutable in Nominal's registry: the workflows generate them from the Git branch, short
commit SHA, and platform, so every source revision registers fresh tags and re-runs of the same
revision reuse them.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import tempfile
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from os import environ
from pathlib import Path
from typing import Any, Sequence

from nominal import ts
from nominal.core import (
    ContainerImage,
    ContainerizedExtractor,
    FileExtractionInput,
    FileExtractionParameter,
    FileOutputFormat,
    NominalClient,
    TimestampMetadata,
)
from nominal.core.exceptions import NominalAlreadyExistsError

logger = logging.getLogger(__name__)
_TAG_COMPONENT_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class ImageSpec:
    platform: str
    tarball: Path


@dataclass(frozen=True)
class ExtractorContract:
    """The image execution contract parsed from the JSON config."""

    inputs: Sequence[FileExtractionInput]
    parameters: Sequence[FileExtractionParameter]
    output_format: FileOutputFormat
    timestamp_column: str
    timestamp_type: ts.TypedTimestampType


def _safe_platform(platform: str) -> str:
    return platform.replace("/", "-")


def _safe_tag_component(value: str) -> str:
    sanitized = _TAG_COMPONENT_PATTERN.sub("-", value).strip(".-")
    if not sanitized:
        raise ValueError("image tag prefix must contain at least one Docker tag character")
    return sanitized


def _parse_image_spec(value: str) -> ImageSpec:
    platform, separator, tarball = value.partition("=")
    if separator == "" or not platform or not tarball:
        raise argparse.ArgumentTypeError("image must use PLATFORM=TARBALL, for example linux/amd64=dist/image.tar")
    return ImageSpec(platform=platform, tarball=Path(tarball))


def _parse_timestamp_type(value: str) -> ts.TypedTimestampType:
    normalized = value.lower()
    if normalized in {"iso8601", "iso_8601"}:
        return ts.Iso8601()
    if normalized.startswith("epoch_"):
        return ts.Epoch(normalized.removeprefix("epoch_"))  # type: ignore[arg-type]
    raise ValueError("timestamp.type must be iso_8601 or epoch_<unit>")


def _parse_contract(config: dict[str, Any]) -> ExtractorContract:
    inputs = tuple(
        FileExtractionInput(
            name=str(item["name"]),
            environment_variable=str(item["environment_variable"]),
            file_suffixes=tuple(str(suffix).lstrip(".") for suffix in item.get("file_suffixes", [])),
            description=item.get("description"),
            required=bool(item.get("required", False)),
        )
        for item in config.get("inputs", [])
    )
    parameters = tuple(
        FileExtractionParameter(
            name=str(item["name"]),
            environment_variable=str(item["environment_variable"]),
            description=item.get("description"),
            required=bool(item.get("required", False)),
        )
        for item in config.get("parameters", [])
    )
    timestamp = config["timestamp"]
    return ExtractorContract(
        inputs=inputs,
        parameters=parameters,
        output_format=FileOutputFormat[str(config.get("output_format", "MANIFEST")).upper()],
        timestamp_column=str(timestamp["series_name"]),
        timestamp_type=_parse_timestamp_type(str(timestamp["type"])),
    )


def upsert_extractor(client: NominalClient, name: str, description: str | None) -> ContainerizedExtractor:
    """Create or fetch the extractor with this exact name, un-archiving and syncing its description."""

    def exact_matches() -> list[ContainerizedExtractor]:
        return [e for e in client.search_containerized_extractors(include_archived=True) if e.name == name]

    matches = exact_matches()
    if len(matches) > 1:
        raise ValueError(f"Multiple containerized extractors found with name '{name}'")
    if not matches:
        try:
            return client.create_containerized_extractor(name, description=description)
        except NominalAlreadyExistsError:
            # Another CI job created it between our search and create; fall through to the re-search.
            matches = exact_matches()
            if len(matches) != 1:
                raise

    extractor = matches[0]
    new_is_archived = False if extractor.is_archived else None
    new_description = description if description is not None and extractor.description != description else None
    if new_is_archived is not None or new_description is not None:
        extractor.update(description=new_description, is_archived=new_is_archived)
    return extractor


def find_registered_image(client: NominalClient, extractor: ContainerizedExtractor, tag: str) -> ContainerImage | None:
    """Find this extractor's image for an immutable tag, returning None when absent.

    The tag filter is applied server-side; the extractor match uses the public `extractor_rid`
    field since the registry search API has no extractor filter yet.
    """
    matches = [img for img in client.search_container_images(tag=tag) if img.extractor_rid == extractor.rid]
    if len(matches) > 1:
        raise ValueError(f"Multiple container images found for extractor '{extractor.rid}' with tag '{tag}'")
    return matches[0] if matches else None


def _require_matching_contract(image: ContainerImage, contract: ExtractorContract) -> None:
    """Fail when an already-registered tag carries a different contract than the config requests."""
    mismatches = []
    if tuple(image.inputs) != tuple(contract.inputs):
        mismatches.append("inputs")
    if tuple(image.parameters) != tuple(contract.parameters):
        mismatches.append("parameters")
    if image.file_output_format is not contract.output_format:
        mismatches.append("output_format")
    expected_timestamp = TimestampMetadata(
        series_name=contract.timestamp_column, timestamp_type=contract.timestamp_type
    )
    if image.default_timestamp_metadata != expected_timestamp:
        mismatches.append("default_timestamp_metadata")
    if mismatches:
        raise ValueError(
            f"Image '{image.rid}' (tag '{image.tag}') is already registered with a different contract "
            f"({', '.join(mismatches)}); tags are immutable, so register the new contract under a new tag."
        )


def register_or_reuse_image(
    client: NominalClient,
    extractor: ContainerizedExtractor,
    tarball: Path,
    tag: str,
    contract: ExtractorContract,
    *,
    squash: bool,
) -> ContainerImage:
    """Register the tarball under `tag`, reusing an already-registered image on CI re-runs."""
    if (existing := find_registered_image(client, extractor, tag)) is not None:
        _require_matching_contract(existing, contract)
        logger.info("reusing already-registered image rid=%s tag=%s", existing.rid, existing.tag)
        return existing

    with ExitStack() as stack:
        if squash:
            tarball = _squash_image_tarball(tarball, stack)
        try:
            return extractor.register_image(
                tarball,
                tag=tag,
                inputs=contract.inputs,
                parameters=contract.parameters,
                default_timestamp_column=contract.timestamp_column,
                default_timestamp_type=contract.timestamp_type,
                output_format=contract.output_format,
            )
        except NominalAlreadyExistsError:
            # Another CI job registered this tag while we were uploading; reuse its image.
            existing = find_registered_image(client, extractor, tag)
            if existing is None:
                raise
            _require_matching_contract(existing, contract)
            logger.info("reusing concurrently-registered image rid=%s tag=%s", existing.rid, existing.tag)
            return existing


def _run_docker(args: Sequence[str]) -> str:
    try:
        completed = subprocess.run(["docker", *args], check=True, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise RuntimeError("Docker CLI is required to squash an image before registering it") from exc
    except subprocess.CalledProcessError as exc:
        output = "\n".join(part for part in [exc.stdout, exc.stderr] if part)
        raise RuntimeError(f"Docker command failed: docker {' '.join(args)}\n{output}") from exc
    return "\n".join(part for part in [completed.stdout, completed.stderr] if part)


def _run_docker_best_effort(args: Sequence[str]) -> None:
    try:
        _run_docker(args)
    except RuntimeError:
        logger.warning("best-effort docker cleanup failed: docker %s", " ".join(args))


def _parse_docker_load_image_ref(output: str) -> str:
    refs = set()
    for line in output.splitlines():
        if line.startswith("Loaded image: "):
            refs.add(line.removeprefix("Loaded image: ").strip())
        elif line.startswith("Loaded image ID: "):
            refs.add(line.removeprefix("Loaded image ID: ").strip())
    refs.discard("")
    if len(refs) != 1:
        raise RuntimeError(f"Expected docker load to produce exactly one image reference; got {sorted(refs)!r}")
    return refs.pop()


def _image_platform(inspected: dict[str, Any]) -> str | None:
    os_name = inspected.get("Os")
    architecture = inspected.get("Architecture")
    if not isinstance(os_name, str) or not isinstance(architecture, str):
        return None
    platform = f"{os_name}/{architecture}"
    variant = inspected.get("Variant")
    if isinstance(variant, str) and variant:
        platform = f"{platform}/{variant}"
    return platform


def _image_config_to_import_changes(config: dict[str, Any]) -> Sequence[str]:
    """Translate the image config into `docker import --change` directives so the squash keeps runtime behavior."""
    changes = []
    if entrypoint := config.get("Entrypoint"):
        changes.append(f"ENTRYPOINT {json.dumps(entrypoint, separators=(',', ':'))}")
    if cmd := config.get("Cmd"):
        changes.append(f"CMD {json.dumps(cmd, separators=(',', ':'))}")
    if (env := config.get("Env")) and isinstance(env, list):
        changes.extend(f"ENV {item}" for item in env if isinstance(item, str))
    if (workdir := config.get("WorkingDir")) and isinstance(workdir, str):
        changes.append(f"WORKDIR {workdir}")
    if (user := config.get("User")) and isinstance(user, str):
        changes.append(f"USER {user}")
    if (stop_signal := config.get("StopSignal")) and isinstance(stop_signal, str):
        changes.append(f"STOPSIGNAL {stop_signal}")
    if (exposed_ports := config.get("ExposedPorts")) and isinstance(exposed_ports, dict):
        changes.append(f"EXPOSE {' '.join(sorted(exposed_ports))}")
    if (volumes := config.get("Volumes")) and isinstance(volumes, dict):
        changes.append(f"VOLUME {json.dumps(sorted(volumes), separators=(',', ':'))}")
    if (labels := config.get("Labels")) and isinstance(labels, dict):
        for key, value in sorted(labels.items()):
            changes.append(f"LABEL {key}={json.dumps(str(value), separators=(',', ':'))}")
    if (on_build := config.get("OnBuild")) and isinstance(on_build, list):
        changes.extend(f"ONBUILD {item}" for item in on_build if isinstance(item, str))
    return tuple(changes)


def _squash_image_tarball(tarball: Path, stack: ExitStack) -> Path:
    """Flatten a Docker archive to a single layer using local docker load/export/import/save."""
    temp_dir = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="nominal-image-squash-")))
    loaded_ref = _parse_docker_load_image_ref(_run_docker(["image", "load", "--input", str(tarball)]))
    inspected = json.loads(_run_docker(["image", "inspect", loaded_ref, "--format", "{{json .}}"]))
    config = inspected.get("Config")
    changes = _image_config_to_import_changes(config if isinstance(config, dict) else {})
    platform = _image_platform(inspected)

    container_id = _run_docker(["container", "create", loaded_ref]).strip()
    stack.callback(_run_docker_best_effort, ["container", "rm", "-f", container_id])
    rootfs = temp_dir / "rootfs.tar"
    _run_docker(["container", "export", "--output", str(rootfs), container_id])

    squashed_ref = f"nominal-sdk-squashed:{uuid.uuid4().hex}"
    import_args = ["image", "import"]
    if platform is not None:
        import_args.append(f"--platform={platform}")
    for change in changes:
        import_args.extend(["--change", change])
    import_args.extend([str(rootfs), squashed_ref])
    _run_docker(import_args)
    stack.callback(_run_docker_best_effort, ["image", "rm", "-f", squashed_ref])

    squashed_tarball = temp_dir / "squashed-image.tar"
    _run_docker(["image", "save", "--output", str(squashed_tarball), squashed_ref])
    return squashed_tarball


def _validate_tarballs(images: Sequence[ImageSpec]) -> None:
    missing = [str(image.tarball) for image in images if not image.tarball.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing image tarballs: {', '.join(missing)}")


def _read_api_key(api_key: str | None, api_key_env: str) -> str:
    if api_key is not None:
        return api_key
    value = environ.get(api_key_env)
    if not value:
        raise ValueError(f"API key not provided; set {api_key_env} or pass --api-key")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy Nominal containerized extractor images from Docker tarballs.")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key")
    parser.add_argument("--api-key-env", default="NOMINAL_API_KEY")
    parser.add_argument("--workspace-rid", required=True)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--image", required=True, action="append", type=_parse_image_spec)
    parser.add_argument("--image-tag-prefix", required=True)
    parser.add_argument("--active-platform")
    parser.add_argument("--squash-before-registering", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = json.loads(args.config.read_text(encoding="utf-8"))
    images: Sequence[ImageSpec] = tuple(args.image)
    _validate_tarballs(images)
    image_tag_prefix = _safe_tag_component(args.image_tag_prefix)

    active_platform = args.active_platform
    image_platforms = {image.platform for image in images}
    if active_platform is None:
        if len(image_platforms) != 1:
            raise ValueError("--active-platform is required when registering multiple platforms")
        active_platform = next(iter(image_platforms))
    if active_platform not in image_platforms:
        raise ValueError(f"active platform '{active_platform}' has no matching --image entry")
    squash = args.squash_before_registering or bool(config.get("squash_before_registering", False))

    contract = _parse_contract(config)

    logger.info("deploying extractor name=%s workspace=%s", config["name"], args.workspace_rid)
    logger.info("registering platforms=%s active_platform=%s", sorted(image_platforms), active_platform)
    logger.info("squash_before_registering=%s", squash)

    if args.dry_run:
        logger.info("dry run complete")
        return

    client = NominalClient.from_token(
        _read_api_key(args.api_key, args.api_key_env), args.api_url, workspace_rid=args.workspace_rid
    )
    extractor = upsert_extractor(client, str(config["name"]), config.get("description"))
    logger.info("using extractor rid=%s name=%s", extractor.rid, extractor.name)

    for image in images:
        image_tag = f"{image_tag_prefix}-{_safe_platform(image.platform)}"
        registered = register_or_reuse_image(client, extractor, image.tarball, image_tag, contract, squash=squash)
        logger.info("registered image rid=%s tag=%s status=%s", registered.rid, registered.tag, registered.status.name)
        if image.platform == active_platform:
            extractor.set_active_image(registered)
            logger.info("activated image rid=%s tag=%s", registered.rid, registered.tag)


if __name__ == "__main__":
    main()
