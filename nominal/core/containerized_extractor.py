"""v2 (Nominal-hosted) containerized extractors and their container images.

A containerized extractor runs a container image that Nominal hosts in its own registry. The extractor
carries identity (name/description/archived); its execution contract — inputs, parameters, output format,
timestamp metadata — lives on the container images registered against it, exactly one of which is active.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Protocol, Sequence

from nominal_api import upload_api
from typing_extensions import Self

from nominal import ts
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin, RefreshableMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core._utils.pagination_tools import (
    search_container_images_paginated,
    search_containerized_extractors_paginated,
)
from nominal.protos.ingest.v2 import containerized_extractor_pb2 as _extractor_pb2
from nominal.protos.ingest.v2 import containerized_extractor_pb2_grpc as _extractor_grpc
from nominal.protos.registry.v2 import registry_pb2 as _registry_pb2
from nominal.protos.registry.v2 import registry_pb2_grpc as _registry_grpc
from nominal.ts import IntegralNanosecondsUTC


class FileOutputFormat(Enum):
    UNSPECIFIED = _registry_pb2.FILE_OUTPUT_FORMAT_UNSPECIFIED
    PARQUET = _registry_pb2.FILE_OUTPUT_FORMAT_PARQUET
    CSV = _registry_pb2.FILE_OUTPUT_FORMAT_CSV
    PARQUET_TAR = _registry_pb2.FILE_OUTPUT_FORMAT_PARQUET_TAR
    AVRO_STREAM = _registry_pb2.FILE_OUTPUT_FORMAT_AVRO_STREAM
    JSON_L = _registry_pb2.FILE_OUTPUT_FORMAT_JSON_L
    MANIFEST = _registry_pb2.FILE_OUTPUT_FORMAT_MANIFEST

    def _to_proto(self) -> _registry_pb2.FileOutputFormat.ValueType:
        return self.value

    @classmethod
    def _from_proto(cls, value: _registry_pb2.FileOutputFormat.ValueType) -> FileOutputFormat:
        return cls(value)


class ContainerImageStatus(Enum):
    UNSPECIFIED = _registry_pb2.CONTAINER_IMAGE_STATUS_UNSPECIFIED
    PENDING = _registry_pb2.CONTAINER_IMAGE_STATUS_PENDING
    READY = _registry_pb2.CONTAINER_IMAGE_STATUS_READY
    FAILED = _registry_pb2.CONTAINER_IMAGE_STATUS_FAILED

    def _to_proto(self) -> _registry_pb2.ContainerImageStatus.ValueType:
        return self.value

    @classmethod
    def _from_proto(cls, value: _registry_pb2.ContainerImageStatus.ValueType) -> ContainerImageStatus:
        return cls(value)


@dataclass(frozen=True)
class FileExtractionInput:
    """An input file the extractor consumes, exposed to the container via an environment variable."""

    name: str
    environment_variable: str
    file_suffixes: Sequence[str] = ()
    description: str | None = None
    required: bool = False

    def _to_proto(self) -> _registry_pb2.FileExtractionInput:
        msg = _registry_pb2.FileExtractionInput(
            environment_variable=self.environment_variable,
            name=self.name,
            required=self.required,
            file_filters=[
                _registry_pb2.FileFilter(suffix=_registry_pb2.FileSuffix(suffix=s)) for s in self.file_suffixes
            ],
        )
        if self.description is not None:
            msg.description = self.description
        return msg

    @classmethod
    def _from_proto(cls, msg: _registry_pb2.FileExtractionInput) -> Self:
        return cls(
            name=msg.name,
            environment_variable=msg.environment_variable,
            file_suffixes=tuple(ff.suffix.suffix for ff in msg.file_filters if ff.WhichOneof("filter") == "suffix"),
            description=msg.description if msg.HasField("description") else None,
            required=msg.required,
        )


@dataclass(frozen=True)
class FileExtractionParameter:
    """A scalar parameter passed to the extractor via an environment variable."""

    name: str
    environment_variable: str
    description: str | None = None
    required: bool = False

    def _to_proto(self) -> _registry_pb2.FileExtractionParameter:
        msg = _registry_pb2.FileExtractionParameter(
            environment_variable=self.environment_variable, name=self.name, required=self.required
        )
        if self.description is not None:
            msg.description = self.description
        return msg

    @classmethod
    def _from_proto(cls, msg: _registry_pb2.FileExtractionParameter) -> Self:
        return cls(
            name=msg.name,
            environment_variable=msg.environment_variable,
            description=msg.description if msg.HasField("description") else None,
            required=msg.required,
        )


@dataclass(frozen=True)
class TimestampMetadata:
    """How the extractor's output timestamps are encoded (the timestamp column + its type)."""

    series_name: str
    timestamp_type: ts._AnyTimestampType

    def _to_proto(self) -> _registry_pb2.TimestampMetadata:
        return _registry_pb2.TimestampMetadata(
            series_name=self.series_name,
            timestamp_type=ts._typed_timestamp_type_to_proto(ts._to_typed_timestamp_type(self.timestamp_type)),
        )

    @classmethod
    def _from_proto(cls, msg: _registry_pb2.TimestampMetadata) -> Self:
        return cls(
            series_name=msg.series_name,
            timestamp_type=ts._proto_timestamp_type_to_typed(msg.timestamp_type),
        )


@dataclass(frozen=True)
class ContainerImage(HasRid, RefreshableMixin[_registry_pb2.ContainerImage]):
    """A container image registered against a containerized extractor (nominal.registry.v2).

    Subclasses the base ``RefreshableMixin`` directly (rather than ``RefreshableGrpcMixin``): the image
    proto carries no workspace, so its constructor needs the extra ``workspace_rid`` this image was
    fetched with, which ``_refresh_to_self`` supplies.
    """

    rid: str
    tag: str
    status: ContainerImageStatus
    size_bytes: int
    created_at: IntegralNanosecondsUTC
    extractor_rid: str
    inputs: Sequence[FileExtractionInput]
    parameters: Sequence[FileExtractionParameter]
    file_output_format: FileOutputFormat
    default_timestamp_metadata: TimestampMetadata | None
    _workspace_rid: str = field(repr=False)
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def registry(self) -> _registry_grpc.RegistryServiceStub: ...

    def _get_latest_api(self) -> _registry_pb2.ContainerImage:
        with translate_grpc_errors():
            return self._clients.registry.GetImage(
                _registry_pb2.GetImageRequest(rid=self.rid, workspace_rid=self._workspace_rid)
            ).image

    def _refresh_to_self(self, msg: _registry_pb2.ContainerImage) -> Self:
        return type(self)._from_proto(self._clients, self._workspace_rid, msg)

    def delete(self) -> None:
        """Delete this image. Fails server-side if an extractor still references it."""
        with translate_grpc_errors():
            self._clients.registry.DeleteImage(
                _registry_pb2.DeleteImageRequest(rid=self.rid, workspace_rid=self._workspace_rid)
            )

    @classmethod
    def _from_proto(cls, clients: _Clients, workspace_rid: str, msg: _registry_pb2.ContainerImage) -> Self:
        return cls(
            rid=msg.rid,
            tag=msg.tag,
            status=ContainerImageStatus._from_proto(msg.status),
            size_bytes=msg.size_bytes,
            created_at=msg.created_at.ToNanoseconds(),
            extractor_rid=msg.extractor_rid,
            inputs=tuple(FileExtractionInput._from_proto(i) for i in msg.inputs),
            parameters=tuple(FileExtractionParameter._from_proto(p) for p in msg.parameters),
            file_output_format=FileOutputFormat._from_proto(msg.file_output_format),
            default_timestamp_metadata=(
                TimestampMetadata._from_proto(msg.default_timestamp_metadata)
                if msg.HasField("default_timestamp_metadata")
                else None
            ),
            _workspace_rid=workspace_rid,
            _clients=clients,
        )


@dataclass(frozen=True)
class ContainerizedExtractor(HasRid, RefreshableGrpcMixin[_extractor_pb2.ContainerizedExtractor]):
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
        def containerized_extractor(self) -> _extractor_grpc.ContainerizedExtractorServiceStub: ...
        @property
        def upload(self) -> upload_api.UploadService: ...

    def _get_latest_api(self) -> _extractor_pb2.ContainerizedExtractor:
        with translate_grpc_errors():
            return self._clients.containerized_extractor.GetContainerizedExtractor(
                _extractor_pb2.GetContainerizedExtractorRequest(rid=self.rid, workspace_rid=self._workspace_rid)
            ).extractor

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        is_archived: bool | None = None,
        active_container_image_rid: str | None = None,
    ) -> Self:
        """Partial update — only the provided fields change. Refreshes this instance in place."""
        request = _extractor_pb2.UpdateContainerizedExtractorRequest(rid=self.rid, workspace_rid=self._workspace_rid)
        if name is not None:
            request.name = name
        if description is not None:
            request.description = description
        if is_archived is not None:
            request.is_archived = is_archived
        if active_container_image_rid is not None:
            request.active_container_image_rid = active_container_image_rid
        with translate_grpc_errors():
            response = self._clients.containerized_extractor.UpdateContainerizedExtractor(request)
        return self._refresh_from_api(response.extractor)

    def archive(self) -> Self:
        """Archive this extractor (hidden from default search; rejects new ingests)."""
        return self.update(is_archived=True)

    def unarchive(self) -> Self:
        """Restore a previously archived extractor."""
        return self.update(is_archived=False)

    def set_active_image(self, image: ContainerImage | str) -> Self:
        """Select the image this extractor runs. The image must be READY and built for it."""
        return self.update(active_container_image_rid=image.rid if isinstance(image, ContainerImage) else image)

    def register_image(
        self,
        tarball: Path | str,
        *,
        tag: str,
        inputs: Sequence[FileExtractionInput],
        timestamp: TimestampMetadata,
        output_format: FileOutputFormat = FileOutputFormat.PARQUET,
        parameters: Sequence[FileExtractionParameter] = (),
    ) -> ContainerImage:
        """Upload a `docker save` tarball and register it as an image. Returns a PENDING image to poll."""
        s3_path = upload_multipart_file(
            self._clients.auth_header,
            self._workspace_rid,
            Path(tarball),
            self._clients.upload,
            header_provider=self._clients.header_provider,
        )
        request = _registry_pb2.CreateImageRequest(
            workspace_rid=self._workspace_rid,
            tag=tag,
            object_path=s3_path,
            extractor_rid=self.rid,
            inputs=[i._to_proto() for i in inputs],
            parameters=[p._to_proto() for p in parameters],
            file_output_format=output_format._to_proto(),
            default_timestamp_metadata=timestamp._to_proto(),
        )
        with translate_grpc_errors():
            response = self._clients.registry.CreateImage(request)
        return ContainerImage._from_proto(self._clients, self._workspace_rid, response.image)

    @classmethod
    def _from_proto(cls, clients: _Clients, msg: _extractor_pb2.ContainerizedExtractor) -> Self:
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


def _resolve_workspace_rid(clients: HasScoutParams, workspace_rid: str | None) -> str:
    return workspace_rid if workspace_rid is not None else clients.resolve_default_workspace_rid()


def _create_containerized_extractor(
    clients: ContainerizedExtractor._Clients, name: str, *, description: str | None, workspace_rid: str | None
) -> ContainerizedExtractor:
    ws = _resolve_workspace_rid(clients, workspace_rid)
    request = _extractor_pb2.CreateContainerizedExtractorRequest(workspace_rid=ws, name=name)
    if description is not None:
        request.description = description
    with translate_grpc_errors():
        response = clients.containerized_extractor.CreateContainerizedExtractor(request)
    return ContainerizedExtractor._from_proto(clients, response.extractor)


def _get_containerized_extractor(
    clients: ContainerizedExtractor._Clients, rid: str, *, workspace_rid: str | None = None
) -> ContainerizedExtractor:
    ws = _resolve_workspace_rid(clients, workspace_rid)
    with translate_grpc_errors():
        response = clients.containerized_extractor.GetContainerizedExtractor(
            _extractor_pb2.GetContainerizedExtractorRequest(rid=rid, workspace_rid=ws)
        )
    return ContainerizedExtractor._from_proto(clients, response.extractor)


def _iter_search_containerized_extractors(
    clients: ContainerizedExtractor._Clients,
    *,
    include_archived: bool,
    file_extension: str | None,
    workspace_rid: str | None,
) -> Iterable[ContainerizedExtractor]:
    ws = _resolve_workspace_rid(clients, workspace_rid)
    extractors = search_containerized_extractors_paginated(
        clients.containerized_extractor,
        workspace_rid=ws,
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


def _get_image(clients: ContainerImage._Clients, rid: str, *, workspace_rid: str | None = None) -> ContainerImage:
    ws = _resolve_workspace_rid(clients, workspace_rid)
    with translate_grpc_errors():
        response = clients.registry.GetImage(_registry_pb2.GetImageRequest(rid=rid, workspace_rid=ws))
    return ContainerImage._from_proto(clients, ws, response.image)


def _build_search_filter(tag: str | None, status: ContainerImageStatus | None) -> _registry_pb2.SearchFilter | None:
    """Build a proto SearchFilter from SDK-native tag/status parameters."""
    filters = []
    if tag is not None:
        filters.append(_registry_pb2.SearchFilter(tag=_registry_pb2.TagFilter(tag=tag)))
    if status is not None:
        filters.append(_registry_pb2.SearchFilter(status=_registry_pb2.StatusFilter(status=status._to_proto())))
    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    # `and` is a Python keyword, so mypy-protobuf can't expose it as a typed kwarg or attribute; pass it
    # through a **mapping (which also avoids getattr/setattr on the generated message).
    and_clause: dict[str, Any] = {"and": _registry_pb2.AndFilter(clauses=filters)}
    return _registry_pb2.SearchFilter(**and_clause)


def _iter_search_images(
    clients: ContainerImage._Clients,
    *,
    tag: str | None = None,
    status: ContainerImageStatus | None = None,
    workspace_rid: str | None = None,
) -> Iterable[ContainerImage]:
    ws = _resolve_workspace_rid(clients, workspace_rid)
    images = search_container_images_paginated(
        clients.registry, workspace_rid=ws, search_filter=_build_search_filter(tag, status)
    )
    for img in images:
        yield ContainerImage._from_proto(clients, ws, img)


def _search_images(
    clients: ContainerImage._Clients,
    *,
    tag: str | None = None,
    status: ContainerImageStatus | None = None,
    workspace_rid: str | None = None,
) -> Sequence[ContainerImage]:
    return list(_iter_search_images(clients, tag=tag, status=status, workspace_rid=workspace_rid))
