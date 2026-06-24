"""v2 (Nominal-hosted) containerized extractors and their container images.

A containerized extractor runs a container image that Nominal hosts in its own registry. The extractor
carries identity (name/description/archived); its execution contract — inputs, parameters, output format,
timestamp metadata — lives on the container images registered against it, exactly one of which is active.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Sequence

from typing_extensions import Self

from nominal import ts
from nominal.protos.registry.v2 import registry_pb2 as _registry_pb2

_DEFAULT_PAGE_SIZE = 100


class FileOutputFormat(Enum):
    PARQUET = "PARQUET"
    CSV = "CSV"
    PARQUET_TAR = "PARQUET_TAR"
    AVRO_STREAM = "AVRO_STREAM"
    JSON_L = "JSON_L"
    MANIFEST = "MANIFEST"

    def _to_proto(self) -> int:
        return _registry_pb2.FileOutputFormat.Value(f"FILE_OUTPUT_FORMAT_{self.value}")

    @classmethod
    def _from_proto(cls, value: int) -> FileOutputFormat:
        return cls(_registry_pb2.FileOutputFormat.Name(value).removeprefix("FILE_OUTPUT_FORMAT_"))


class ContainerImageStatus(Enum):
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"

    @classmethod
    def _from_proto(cls, value: int) -> ContainerImageStatus:
        return cls(_registry_pb2.ContainerImageStatus.Name(value).removeprefix("CONTAINER_IMAGE_STATUS_"))


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
            file_suffixes=tuple(ff.suffix.suffix for ff in msg.file_filters),
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
