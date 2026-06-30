"""v2 (Nominal-hosted) containerized extractors (nominal.ingest.v2).

A containerized extractor runs a container image that Nominal hosts in its own registry. The extractor
carries identity (name/description/archived); its execution contract — inputs, parameters, output format,
timestamp metadata — lives on the container images registered against it (see `nominal.core.container_image`),
exactly one of which is active.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import uuid
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Protocol, Sequence

import grpc
from nominal_api import upload_api
from typing_extensions import Self

from nominal import ts
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin, rid_from_instance_or_string
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core._utils.pagination_tools import search_containerized_extractors_paginated
from nominal.core.container_image import (
    REGISTERABLE_OUTPUT_FORMATS,
    ContainerImage,
    ContainerImageStatus,
    FileExtractionInput,
    FileExtractionParameter,
    FileOutputFormat,
    TimestampMetadata,
    _search_container_images,
)
from nominal.core.exceptions import NominalContainerImageError
from nominal.protos.ingest.v2 import containerized_extractor_pb2, containerized_extractor_pb2_grpc
from nominal.protos.registry.v2 import registry_pb2
from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class ContainerizedExtractor(HasRid, RefreshableGrpcMixin[containerized_extractor_pb2.ContainerizedExtractor]):
    """A v2 (Nominal-hosted) containerized extractor (nominal.ingest.v2)."""

    rid: str
    name: str
    description: str | None
    is_archived: bool
    active_image: ContainerImage | None
    created_at: IntegralNanosecondsUTC
    _workspace_rid: str = field(repr=False)
    _clients: _Clients = field(repr=False)

    class _Clients(ContainerImage._Clients, Protocol):
        @property
        def containerized_extractor(self) -> containerized_extractor_pb2_grpc.ContainerizedExtractorServiceStub: ...
        @property
        def upload(self) -> upload_api.UploadService: ...

    def _get_latest_api(self) -> containerized_extractor_pb2.ContainerizedExtractor:
        with translate_grpc_errors():
            return self._clients.containerized_extractor.GetContainerizedExtractor(
                containerized_extractor_pb2.GetContainerizedExtractorRequest(
                    rid=self.rid, workspace_rid=self._workspace_rid
                )
            ).extractor

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        is_archived: bool | None = None,
        active_container_image: ContainerImage | str | None = None,
    ) -> Self:
        """Update the extractor in-place.

        Args:
            name: New name of the extractor.
            description: New description of the extractor.
            is_archived: New archived state of the extractor.
            active_container_image: Registered container image the extractor should run.

        Returns:
            This instance, refreshed with the updated values.

        Note:
            Fields left as None are unchanged (there is no way to clear a previously set field).
        """
        image_rid = None if active_container_image is None else rid_from_instance_or_string(active_container_image)
        request = containerized_extractor_pb2.UpdateContainerizedExtractorRequest(
            rid=self.rid,
            workspace_rid=self._workspace_rid,
            name=name,
            description=description,
            is_archived=is_archived,
            active_container_image_rid=image_rid,
        )
        with translate_grpc_errors():
            response = self._clients.containerized_extractor.UpdateContainerizedExtractor(request)
        return self._refresh_from_api(response.extractor)

    def archive(self) -> None:
        """Archive this extractor.

        Archived extractors are hidden from search by default and reject new ingests.
        """
        self.update(is_archived=True)

    def unarchive(self) -> None:
        """Unarchive this extractor, making it available for searching and ingesting once more."""
        self.update(is_archived=False)

    def set_active_image(self, image: ContainerImage | str, *, poll_until_ready: bool = True) -> Self:
        """Select the container image this extractor runs when ingesting.

        Args:
            image: Image (or RID of one) to activate. Must be registered against this extractor.
            poll_until_ready: If true, block until the image has finished processing before activating
                it. If false, raise immediately if the image is not READY.

        Returns:
            This instance, refreshed with the newly activated image.

        Raises:
            NominalContainerImageError: If the image is not READY (or, when polling, reaches a state
                from which it cannot become READY).
        """
        if isinstance(image, str):
            with translate_grpc_errors():
                response = self._clients.registry.GetImage(
                    registry_pb2.GetImageRequest(rid=image, workspace_rid=self._workspace_rid)
                )
            image = ContainerImage._from_proto(self._clients, self._workspace_rid, response.image)
        else:
            image.refresh()

        if poll_until_ready:
            image.poll_until_ready()
        elif image.status is not ContainerImageStatus.READY:
            raise NominalContainerImageError(
                f"Cannot activate container image {image.rid!r} (tag {image.tag!r}): status is "
                f"{image.status.name}, not READY. Pass poll_until_ready=True to wait for it."
            )

        return self.update(active_container_image=image)

    def search_images(
        self, *, tag: str | None = None, status: ContainerImageStatus | None = None
    ) -> Sequence[ContainerImage]:
        """Search images registered against this extractor."""
        return _search_container_images(
            self._clients,
            tag=tag,
            status=status,
            extractor_rid=self.rid,
            workspace_rid=self._workspace_rid,
        )

    def get_image_by_tag(self, tag: str, *, status: ContainerImageStatus | None = None) -> ContainerImage | None:
        """Fetch this extractor's image for an immutable tag, returning None when absent."""
        matches = self.search_images(tag=tag, status=status)
        if len(matches) > 1:
            raise ValueError(f"Multiple container images found for extractor '{self.rid}' with tag '{tag}'")
        return matches[0] if matches else None

    def register_image(
        self,
        tarball: Path | str,
        *,
        tag: str,
        inputs: Sequence[FileExtractionInput],
        default_timestamp_column: str,
        default_timestamp_type: ts._AnyTimestampType,
        output_format: FileOutputFormat = FileOutputFormat.PARQUET,
        parameters: Sequence[FileExtractionParameter] = (),
        reuse_existing: bool = True,
        activate: bool = False,
        wait_until_ready: bool = False,
        wait_timeout_seconds: float = 300,
        poll_interval_seconds: float = 2,
        squash_before_registering: bool = False,
    ) -> ContainerImage:
        """Upload a `docker save` tarball and register it as a container image for this extractor.

        Tags are immutable. With `reuse_existing=True`, an existing image with the same tag and contract
        is returned instead of re-uploading. Set `squash_before_registering=True` to flatten the archive
        before upload (requires the Docker CLI). Registering does not change which image the extractor
        runs unless `activate=True`.

        Args:
            tarball: Path to a `docker save` tarball of the extractor image.
            tag: Tag to register the image under.
            inputs: Input files the extractor consumes.
            default_timestamp_column: Name of the column containing timestamp data in the extractor's
                output files, when no more specific timestamp metadata applies — see
                `default_timestamp_type`.
            default_timestamp_type: How timestamps in that column are interpreted. Together with
                `default_timestamp_column`, stored as the image's `default_timestamp_metadata` — the
                fallback timestamp encoding for data ingested through this extractor; required at
                registration even when every ingest overrides it. See
                `ContainerImage.default_timestamp_metadata` for the full resolution order.
            output_format: File format the extractor writes. Must be one of
                `REGISTERABLE_OUTPUT_FORMATS` — the backend cannot currently ingest the others, so
                registering an image with one would produce an extractor whose ingests always fail.
            parameters: Scalar parameters passed to the extractor.
            reuse_existing: If true, return the already-registered image for this tag (validating it
                carries the same contract) instead of raising or re-uploading.
            activate: If true, activate the image on this extractor once it is READY.
            wait_until_ready: If true, block until the image reaches READY before returning.
            wait_timeout_seconds: Deadline for `wait_until_ready`.
            poll_interval_seconds: Poll cadence for `wait_until_ready`.
            squash_before_registering: If true, flatten the Docker archive to a single layer with the
                local Docker CLI before uploading.

        Raises:
            ValueError: If `output_format` is not currently ingestible via containerized extraction,
                or an existing image with this tag carries a different contract.
        """
        if output_format not in REGISTERABLE_OUTPUT_FORMATS:
            supported = ", ".join(sorted(fmt.name for fmt in REGISTERABLE_OUTPUT_FORMATS))
            raise ValueError(
                f"Output format {output_format.name} is not currently supported for containerized "
                f"extraction ingest; an image registered with it could never ingest data successfully. "
                f"Supported formats: {supported}."
            )
        timestamp_metadata = TimestampMetadata(
            series_name=default_timestamp_column, timestamp_type=default_timestamp_type
        )
        if reuse_existing and (existing := self.get_image_by_tag(tag)) is not None:
            _validate_image_contract(existing, inputs, parameters, output_format, timestamp_metadata)
            if wait_until_ready:
                existing.wait_until_ready(
                    timeout_seconds=wait_timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
            if activate:
                self._activate_ready_image(existing)
            return existing

        with ExitStack() as stack:
            tarball_path = Path(tarball)
            if squash_before_registering:
                tarball_path = _squash_image_tarball(tarball_path, stack)
            s3_path = upload_multipart_file(
                self._clients.auth_header,
                self._workspace_rid,
                tarball_path,
                self._clients.upload,
                header_provider=self._clients.header_provider,
            )
        request = registry_pb2.CreateImageRequest(
            workspace_rid=self._workspace_rid,
            tag=tag,
            object_path=s3_path,
            extractor_rid=self.rid,
            inputs=[i._to_proto() for i in inputs],
            parameters=[p._to_proto() for p in parameters],
            file_output_format=output_format._to_proto(),
            default_timestamp_metadata=timestamp_metadata._to_proto(),
        )
        try:
            with translate_grpc_errors():
                response = self._clients.registry.CreateImage(request)
        except Exception as exc:
            existing = self.get_image_by_tag(tag) if reuse_existing and _is_reusable_create_image_error(exc) else None
            if existing is None:
                raise
            _validate_image_contract(existing, inputs, parameters, output_format, timestamp_metadata)
            if wait_until_ready:
                existing.wait_until_ready(
                    timeout_seconds=wait_timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
            if activate:
                self._activate_ready_image(existing)
            return existing
        image = ContainerImage._from_proto(self._clients, self._workspace_rid, response.image)
        if wait_until_ready:
            image.wait_until_ready(timeout_seconds=wait_timeout_seconds, poll_interval_seconds=poll_interval_seconds)
        if activate:
            self._activate_ready_image(image)
        return image

    def _activate_ready_image(self, image: ContainerImage) -> None:
        if image.status is not ContainerImageStatus.READY:
            raise ValueError(f"Cannot activate container image '{image.rid}' while status is {image.status.name}")
        self.update(active_container_image=image)

    @classmethod
    def _from_proto(cls, clients: _Clients, msg: containerized_extractor_pb2.ContainerizedExtractor) -> Self:
        return cls(
            rid=msg.rid,
            name=msg.name,
            description=msg.description if msg.HasField("description") else None,
            is_archived=msg.is_archived,
            active_image=(
                ContainerImage._from_proto(clients, msg.workspace_rid, msg.active_container_image)
                if msg.HasField("active_container_image")
                else None
            ),
            created_at=msg.created_at.ToNanoseconds(),
            _workspace_rid=msg.workspace_rid,
            _clients=clients,
        )


def _create_containerized_extractor(
    clients: ContainerizedExtractor._Clients, name: str, *, description: str | None
) -> ContainerizedExtractor:
    request = containerized_extractor_pb2.CreateContainerizedExtractorRequest(
        workspace_rid=clients.resolve_default_workspace_rid(), name=name, description=description
    )
    with translate_grpc_errors():
        response = clients.containerized_extractor.CreateContainerizedExtractor(request)
    return ContainerizedExtractor._from_proto(clients, response.extractor)


def _get_containerized_extractor(clients: ContainerizedExtractor._Clients, rid: str) -> ContainerizedExtractor:
    with translate_grpc_errors():
        response = clients.containerized_extractor.GetContainerizedExtractor(
            containerized_extractor_pb2.GetContainerizedExtractorRequest(
                rid=rid, workspace_rid=clients.resolve_default_workspace_rid()
            )
        )
    return ContainerizedExtractor._from_proto(clients, response.extractor)


def _iter_search_containerized_extractors(
    clients: ContainerizedExtractor._Clients,
    *,
    include_archived: bool,
    file_extension: str | None,
    workspace_rid: str | None,
) -> Iterable[ContainerizedExtractor]:
    ws = clients.resolve_workspace(workspace_rid).rid
    extractors = search_containerized_extractors_paginated(
        clients.containerized_extractor,
        ws,
        include_archived=include_archived,
        file_extension=file_extension,
    )
    for extractor in extractors:
        yield ContainerizedExtractor._from_proto(clients, extractor)


def _search_containerized_extractors(
    clients: ContainerizedExtractor._Clients,
    *,
    include_archived: bool,
    file_extension: str | None,
    workspace_rid: str | None,
) -> Sequence[ContainerizedExtractor]:
    return list(
        _iter_search_containerized_extractors(
            clients, include_archived=include_archived, file_extension=file_extension, workspace_rid=workspace_rid
        )
    )

def _is_reusable_create_image_error(exc: Exception) -> bool:
    if not isinstance(exc.__cause__, grpc.RpcError):
        return False
    rpc_error = exc.__cause__
    if rpc_error.code() is grpc.StatusCode.ALREADY_EXISTS:
        return True
    details = (rpc_error.details() or "").lower()
    return (
        rpc_error.code() is grpc.StatusCode.INTERNAL
        and "failed to push image to registry" in details
        and ("blobalreadyexists" in details or "already exists" in details)
    )


def _validate_image_contract(
    image: ContainerImage,
    inputs: Sequence[FileExtractionInput],
    parameters: Sequence[FileExtractionParameter],
    output_format: FileOutputFormat,
    timestamp: TimestampMetadata,
) -> None:
    mismatches = []
    if tuple(image.inputs) != tuple(inputs):
        mismatches.append("inputs")
    if tuple(image.parameters) != tuple(parameters):
        mismatches.append("parameters")
    if image.file_output_format is not output_format:
        mismatches.append("output_format")
    if (
        image.default_timestamp_metadata is None
        or image.default_timestamp_metadata._to_proto() != timestamp._to_proto()
    ):
        mismatches.append("timestamp")
    if mismatches:
        joined = ", ".join(mismatches)
        raise ValueError(f"Existing container image '{image.rid}' has a different registration contract: {joined}")


def _run_docker(args: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            ["docker", *args],
            check=True,
            capture_output=True,
            text=True,
        )
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
        pass


def _parse_docker_load_image_ref(output: str) -> str:
    refs = []
    for line in output.splitlines():
        if line.startswith("Loaded image: "):
            refs.append(line.removeprefix("Loaded image: ").strip())
        elif line.startswith("Loaded image ID: "):
            refs.append(line.removeprefix("Loaded image ID: ").strip())
    refs = sorted(set(ref for ref in refs if ref))
    if len(refs) != 1:
        raise RuntimeError(f"Expected docker load to produce exactly one image reference; got {refs!r}")
    return refs[0]


def _image_platform(inspected: dict[str, object]) -> str | None:
    os_name = inspected.get("Os")
    architecture = inspected.get("Architecture")
    if not isinstance(os_name, str) or not isinstance(architecture, str):
        return None
    platform = f"{os_name}/{architecture}"
    variant = inspected.get("Variant")
    if isinstance(variant, str) and variant:
        platform = f"{platform}/{variant}"
    return platform


def _image_config_to_import_changes(config: dict[str, object]) -> Sequence[str]:
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
    """Flatten a Docker archive using load/export/import/save before uploading."""
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
