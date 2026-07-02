"""v2 (Nominal-hosted) containerized extractors (nominal.ingest.v2).

A containerized extractor runs a container image that Nominal hosts in its own registry. The extractor
carries identity (name/description/archived); its execution contract — inputs, parameters, output format,
timestamp metadata — lives on the container images registered against it (see `nominal.core.container_image`),
exactly one of which is active.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Protocol, Sequence

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
    ) -> ContainerImage:
        """Upload a `docker save` tarball and register it as a container image for this extractor.

        Registering attaches the image to this extractor in Nominal's registry, but does not change
        which image the extractor runs: the new image must be activated with `set_active_image`.
        This extractor instance is unmodified. Tags are immutable — registering an already-registered
        tag raises `NominalAlreadyExistsError`.

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

        Returns:
            The newly registered image. Current backends push the image to the registry within this
            call and return it READY; if a backend processes asynchronously (a non-READY image),
            `set_active_image` polls it to readiness before activating (or use
            `ContainerImage.poll_until_ready` directly).

        Raises:
            ValueError: If `output_format` is not currently ingestible via containerized extraction.
            NominalAlreadyExistsError: If an image with this tag is already registered for this
                extractor.
        """
        if output_format not in REGISTERABLE_OUTPUT_FORMATS:
            supported = ", ".join(sorted(fmt.name for fmt in REGISTERABLE_OUTPUT_FORMATS))
            raise ValueError(
                f"Output format {output_format.name} is not currently supported for containerized "
                f"extraction ingest; an image registered with it could never ingest data successfully. "
                f"Supported formats: {supported}."
            )
        s3_path = upload_multipart_file(
            self._clients.auth_header,
            self._workspace_rid,
            Path(tarball),
            self._clients.upload,
            header_provider=self._clients.header_provider,
        )
        timestamp_metadata = TimestampMetadata(
            series_name=default_timestamp_column, timestamp_type=default_timestamp_type
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
        with translate_grpc_errors():
            response = self._clients.registry.CreateImage(request)
        return ContainerImage._from_proto(self._clients, self._workspace_rid, response.image)

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
