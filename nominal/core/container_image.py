"""Container images backing v2 (Nominal-hosted) containerized extractors (nominal.registry.v2).

A container image is a `docker save` tarball pushed into Nominal's registry and registered against a
containerized extractor. The image carries the extractor's execution contract — inputs, parameters,
output format, timestamp metadata; the extractor itself (see `nominal.core.containerized_extractor`)
carries identity and selects exactly one registered image as active.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Iterable, Protocol, Sequence

from typing_extensions import Self, assert_never

from nominal import ts
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_container_images_paginated
from nominal.core._utils.query_tools import create_search_container_images_query
from nominal.core.exceptions import NominalContainerImageError
from nominal.protos.registry.v2 import registry_pb2, registry_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC


class FileOutputFormat(Enum):
    """File format a containerized extractor writes its output data in."""

    MANIFEST = "MANIFEST"
    PARQUET = "PARQUET"
    CSV = "CSV"
    AVRO_STREAM = "AVRO_STREAM"
    PARQUET_TAR = "PARQUET_TAR"
    """Not currently ingestible via containerized extraction; `register_image` rejects it."""
    JSON_L = "JSON_L"
    """Not currently ingestible via containerized extraction; `register_image` rejects it."""
    UNSPECIFIED = "UNSPECIFIED"
    """Unset, or a format a newer server sent that this SDK doesn't know. Not registerable."""

    def _to_proto(self) -> registry_pb2.FileOutputFormat.ValueType:
        match self:
            case FileOutputFormat.UNSPECIFIED:
                result = registry_pb2.FILE_OUTPUT_FORMAT_UNSPECIFIED
            case FileOutputFormat.PARQUET:
                result = registry_pb2.FILE_OUTPUT_FORMAT_PARQUET
            case FileOutputFormat.CSV:
                result = registry_pb2.FILE_OUTPUT_FORMAT_CSV
            case FileOutputFormat.PARQUET_TAR:
                result = registry_pb2.FILE_OUTPUT_FORMAT_PARQUET_TAR
            case FileOutputFormat.AVRO_STREAM:
                result = registry_pb2.FILE_OUTPUT_FORMAT_AVRO_STREAM
            case FileOutputFormat.JSON_L:
                result = registry_pb2.FILE_OUTPUT_FORMAT_JSON_L
            case FileOutputFormat.MANIFEST:
                result = registry_pb2.FILE_OUTPUT_FORMAT_MANIFEST
            case _:
                assert_never(self)
        return result

    @classmethod
    def _from_proto(cls, value: registry_pb2.FileOutputFormat.ValueType) -> FileOutputFormat:
        match value:
            case registry_pb2.FILE_OUTPUT_FORMAT_PARQUET:
                result = cls.PARQUET
            case registry_pb2.FILE_OUTPUT_FORMAT_CSV:
                result = cls.CSV
            case registry_pb2.FILE_OUTPUT_FORMAT_PARQUET_TAR:
                result = cls.PARQUET_TAR
            case registry_pb2.FILE_OUTPUT_FORMAT_AVRO_STREAM:
                result = cls.AVRO_STREAM
            case registry_pb2.FILE_OUTPUT_FORMAT_JSON_L:
                result = cls.JSON_L
            case registry_pb2.FILE_OUTPUT_FORMAT_MANIFEST:
                result = cls.MANIFEST
            case _:
                # Unset, or a value a newer server sent that this SDK doesn't know.
                result = cls.UNSPECIFIED
        return result


REGISTERABLE_OUTPUT_FORMATS = frozenset(
    {
        FileOutputFormat.PARQUET,
        FileOutputFormat.CSV,
        FileOutputFormat.AVRO_STREAM,
        FileOutputFormat.MANIFEST,
    }
)
"""Output formats the backend can currently ingest via containerized extraction.

Registration accepts other formats server-side, but their ingest jobs would always fail
(the containerized transform path has no reader for them), so `register_image` rejects them
up-front. Update when the backend gains readers for the remaining formats.
"""


class ContainerImageStatus(Enum):
    """Lifecycle status of a container image: PENDING while being processed, then READY or FAILED."""

    UNSPECIFIED = "UNSPECIFIED"
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"

    def _to_proto(self) -> registry_pb2.ContainerImageStatus.ValueType:
        match self:
            case ContainerImageStatus.UNSPECIFIED:
                result = registry_pb2.CONTAINER_IMAGE_STATUS_UNSPECIFIED
            case ContainerImageStatus.PENDING:
                result = registry_pb2.CONTAINER_IMAGE_STATUS_PENDING
            case ContainerImageStatus.READY:
                result = registry_pb2.CONTAINER_IMAGE_STATUS_READY
            case ContainerImageStatus.FAILED:
                result = registry_pb2.CONTAINER_IMAGE_STATUS_FAILED
            case _:
                assert_never(self)
        return result

    @classmethod
    def _from_proto(cls, value: registry_pb2.ContainerImageStatus.ValueType) -> ContainerImageStatus:
        match value:
            case registry_pb2.CONTAINER_IMAGE_STATUS_PENDING:
                result = cls.PENDING
            case registry_pb2.CONTAINER_IMAGE_STATUS_READY:
                result = cls.READY
            case registry_pb2.CONTAINER_IMAGE_STATUS_FAILED:
                result = cls.FAILED
            case _:
                # Unset, or a value a newer server sent that this SDK doesn't know.
                result = cls.UNSPECIFIED
        return result


@dataclass(frozen=True)
class FileExtractionInput:
    """An input file the extractor consumes, exposed to the container via an environment variable."""

    name: str
    """Human-readable name of the input."""
    environment_variable: str
    """Environment variable through which the container receives the input file's path."""
    description: str | None = None
    """Human-readable description of the input."""
    file_suffixes: Sequence[str] = ()
    """File suffixes this input accepts (e.g. "csv", "mcap"); empty accepts any file."""
    required: bool = False
    """Whether an ingest must provide this input to run the extractor."""

    def _to_proto(self) -> registry_pb2.FileExtractionInput:
        return registry_pb2.FileExtractionInput(
            name=self.name,
            description=self.description,
            environment_variable=self.environment_variable,
            required=self.required,
            file_filters=[
                registry_pb2.FileFilter(suffix=registry_pb2.FileSuffix(suffix=s)) for s in self.file_suffixes
            ],
        )

    @classmethod
    def _from_proto(cls, msg: registry_pb2.FileExtractionInput) -> Self:
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
    """Human-readable name of the parameter."""
    environment_variable: str
    """Environment variable through which the container receives the parameter's value."""
    description: str | None = None
    """Human-readable description of the parameter."""
    required: bool = False
    """Whether an ingest must provide this parameter to run the extractor."""

    def _to_proto(self) -> registry_pb2.FileExtractionParameter:
        return registry_pb2.FileExtractionParameter(
            name=self.name,
            description=self.description,
            environment_variable=self.environment_variable,
            required=self.required,
        )

    @classmethod
    def _from_proto(cls, msg: registry_pb2.FileExtractionParameter) -> Self:
        return cls(
            name=msg.name,
            environment_variable=msg.environment_variable,
            description=msg.description if msg.HasField("description") else None,
            required=msg.required,
        )


@dataclass(frozen=True)
class TimestampMetadata:
    """How timestamps in the extractor's output data are encoded (the timestamp column + its type)."""

    series_name: str
    """Name of the column containing timestamp data in the extractor's output files."""
    timestamp_type: ts._AnyTimestampType
    """How timestamps in that column are interpreted."""

    def _to_proto(self) -> registry_pb2.TimestampMetadata:
        return registry_pb2.TimestampMetadata(
            series_name=self.series_name,
            timestamp_type=ts._to_typed_timestamp_type(self.timestamp_type)._to_proto(),
        )

    @classmethod
    def _from_proto(cls, msg: registry_pb2.TimestampMetadata) -> Self:
        return cls(
            series_name=msg.series_name,
            timestamp_type=ts._proto_timestamp_type_to_typed_timestamp_type(msg.timestamp_type),
        )


# The registry's requests are workspace-scoped but its ContainerImage message does not echo the
# workspace, so instances store the workspace_rid they were fetched with and subclass the base
# RefreshableMixin directly, re-supplying it in _refresh_to_self. This shape is unique to
# nominal.registry.v2 in the current proto surface (newer v2 resources echo their workspace), so
# it stays a documented one-off rather than a mixin variant.
@dataclass(frozen=True)
class ContainerImage(HasRid, RefreshableMixin[registry_pb2.ContainerImage]):
    """A container image registered against a containerized extractor (nominal.registry.v2)."""

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
    """How timestamps in the extractor's output are encoded, when nothing more specific applies.

    Per output file, the backend resolves timestamp metadata in this order:

    1. For MANIFEST-format extractors, the metadata a manifest output declares for itself;
    2. the ingest request's override (`Dataset.add_containerized`'s `timestamp_column`/`timestamp_type`);
    3. this default.

    The request-override-or-default portion is resolved before the container runs, and ingestion fails
    if both are absent — a fully-descriptive manifest alone is not sufficient. Always set on images
    registered through this SDK (registration requires it); may be None on images from older
    registration paths, in which case every ingest must supply an override.
    """
    _workspace_rid: str = field(repr=False)
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def registry(self) -> registry_pb2_grpc.RegistryServiceStub: ...

    def _get_latest_api(self) -> registry_pb2.ContainerImage:
        with translate_grpc_errors():
            return self._clients.registry.GetImage(
                registry_pb2.GetImageRequest(rid=self.rid, workspace_rid=self._workspace_rid)
            ).image

    def _refresh_to_self(self, msg: registry_pb2.ContainerImage) -> Self:
        return type(self)._from_proto(self._clients, self._workspace_rid, msg)

    def delete(self) -> None:
        """Delete this image.

        Fails server-side if an extractor still references it as its active image.
        """
        with translate_grpc_errors():
            self._clients.registry.DeleteImage(
                registry_pb2.DeleteImageRequest(rid=self.rid, workspace_rid=self._workspace_rid)
            )

    def poll_until_ready(self, interval: timedelta = timedelta(seconds=1)) -> Self:
        """Block until this image has finished processing server-side, refreshing in place.

        This method polls Nominal for the image's status on an interval.

        Args:
            interval: How often to poll for the image's status.

        Returns:
            This instance, once its status is `ContainerImageStatus.READY`.

        Raises:
            NominalContainerImageError: If the image reaches a state from which it cannot become
                READY (FAILED, or a status this SDK doesn't recognize).
        """
        while True:
            self.refresh()
            match self.status:
                case ContainerImageStatus.READY:
                    return self
                case ContainerImageStatus.PENDING:
                    pass
                case ContainerImageStatus.FAILED | ContainerImageStatus.UNSPECIFIED:
                    raise NominalContainerImageError(
                        f"Container image {self.rid!r} (tag {self.tag!r}) cannot become READY: "
                        f"status is {self.status.name}."
                    )
                case _:
                    assert_never(self.status)
            time.sleep(interval.total_seconds())

    @classmethod
    def _from_proto(cls, clients: _Clients, workspace_rid: str, msg: registry_pb2.ContainerImage) -> Self:
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


def _get_container_image(clients: ContainerImage._Clients, rid: str) -> ContainerImage:
    ws = clients.resolve_default_workspace_rid()
    with translate_grpc_errors():
        response = clients.registry.GetImage(registry_pb2.GetImageRequest(rid=rid, workspace_rid=ws))
    return ContainerImage._from_proto(clients, ws, response.image)


def _iter_search_container_images(
    clients: ContainerImage._Clients,
    *,
    tag: str | None = None,
    status: ContainerImageStatus | None = None,
    workspace_rid: str | None = None,
) -> Iterable[ContainerImage]:
    ws = clients.resolve_workspace(workspace_rid).rid
    search_filter = create_search_container_images_query(tag=tag, status=status)
    for img in search_container_images_paginated(clients.registry, ws, search_filter):
        yield ContainerImage._from_proto(clients, ws, img)


def _search_container_images(
    clients: ContainerImage._Clients,
    *,
    tag: str | None = None,
    status: ContainerImageStatus | None = None,
    workspace_rid: str | None = None,
) -> Sequence[ContainerImage]:
    return list(_iter_search_container_images(clients, tag=tag, status=status, workspace_rid=workspace_rid))
