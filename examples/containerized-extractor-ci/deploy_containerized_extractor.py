from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass
from os import environ
from pathlib import Path
from typing import Any, Sequence

from nominal import ts
from nominal.core import (
    FileExtractionInput,
    FileExtractionParameter,
    FileOutputFormat,
    NominalClient,
    TimestampMetadata,
)

logger = logging.getLogger(__name__)
_TAG_COMPONENT_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class ImageSpec:
    platform: str
    tarball: Path


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


def _read_config(path: Path) -> dict[str, Any]:
    config: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return config


def _parse_timestamp_type(value: str) -> ts.TypedTimestampType:
    normalized = value.lower()
    if normalized in {"iso8601", "iso_8601"}:
        return ts.Iso8601()
    if normalized.startswith("epoch_"):
        return ts.Epoch(normalized.removeprefix("epoch_"))  # type: ignore[arg-type]
    raise ValueError("timestamp.type must be iso_8601 or epoch_<unit>")


def _parse_inputs(config: dict[str, Any]) -> Sequence[FileExtractionInput]:
    inputs = []
    for item in config.get("inputs", []):
        inputs.append(
            FileExtractionInput(
                name=str(item["name"]),
                environment_variable=str(item["environment_variable"]),
                file_suffixes=tuple(str(suffix).lstrip(".") for suffix in item.get("file_suffixes", [])),
                description=item.get("description"),
                required=bool(item.get("required", False)),
            )
        )
    return tuple(inputs)


def _parse_parameters(config: dict[str, Any]) -> Sequence[FileExtractionParameter]:
    parameters = []
    for item in config.get("parameters", []):
        parameters.append(
            FileExtractionParameter(
                name=str(item["name"]),
                environment_variable=str(item["environment_variable"]),
                description=item.get("description"),
                required=bool(item.get("required", False)),
            )
        )
    return tuple(parameters)


def _parse_timestamp(config: dict[str, Any]) -> TimestampMetadata:
    timestamp = config["timestamp"]
    return TimestampMetadata(
        series_name=str(timestamp["series_name"]),
        timestamp_type=_parse_timestamp_type(str(timestamp["type"])),
    )


def _parse_output_format(config: dict[str, Any]) -> FileOutputFormat:
    return FileOutputFormat[str(config.get("output_format", "MANIFEST")).upper()]


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

    config = _read_config(args.config)
    images: Sequence[ImageSpec] = tuple(args.image)
    _validate_tarballs(images)
    image_tag_prefix = _safe_tag_component(args.image_tag_prefix)

    active_platform = args.active_platform
    image_platforms = {image.platform for image in images}
    if active_platform is None:
        if len(image_platforms) == 1:
            active_platform = next(iter(image_platforms))
        else:
            raise ValueError("--active-platform is required when registering multiple platforms")
    if active_platform is not None and active_platform not in image_platforms:
        raise ValueError(f"active platform '{active_platform}' has no matching --image entry")
    squash_before_registering = args.squash_before_registering or bool(config.get("squash_before_registering", False))

    inputs = _parse_inputs(config)
    parameters = _parse_parameters(config)
    timestamp = _parse_timestamp(config)
    output_format = _parse_output_format(config)

    logger.info("deploying extractor name=%s workspace=%s", config["name"], args.workspace_rid)
    logger.info("registering platforms=%s active_platform=%s", sorted(image_platforms), active_platform)
    logger.info("squash_before_registering=%s", squash_before_registering)

    if args.dry_run:
        logger.info("dry run complete")
        return

    client = NominalClient.from_token(
        _read_api_key(args.api_key, args.api_key_env), args.api_url, workspace_rid=args.workspace_rid
    )
    extractor = client.upsert_containerized_extractor(
        str(config["name"]),
        description=config.get("description"),
        workspace=args.workspace_rid,
    )
    logger.info("using extractor rid=%s name=%s", extractor.rid, extractor.name)

    for image in images:
        image_tag = f"{image_tag_prefix}-{_safe_platform(image.platform)}"
        activate = image.platform == active_platform
        registered = extractor.register_image(
            image.tarball,
            tag=image_tag,
            inputs=inputs,
            parameters=parameters,
            default_timestamp_column=timestamp.series_name,
            default_timestamp_type=timestamp.timestamp_type,
            output_format=output_format,
            reuse_existing=True,
            wait_until_ready=True,
            activate=activate,
            squash_before_registering=squash_before_registering,
        )
        logger.info(
            "registered image rid=%s tag=%s status=%s activate=%s",
            registered.rid,
            registered.tag,
            registered.status.name,
            activate,
        )


if __name__ == "__main__":
    main()
