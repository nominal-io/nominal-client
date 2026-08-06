"""Conversion between this runtime's arguments and the generated manifest types."""

from __future__ import annotations

import enum

from nominal_api import ingest_manifest
from typing_extensions import assert_never

from nominal import ts
from nominal.core.exceptions import ExtractorError


class IngestType(enum.Enum):
    """How a manifest output file should be ingested."""

    TABULAR = "TABULAR"
    AVRO_STREAM = "AVRO_STREAM"
    JSON_L = "JSON_L"

    def _to_conjure(self) -> ingest_manifest.ManifestIngestType:
        match self:
            case IngestType.TABULAR:
                result = ingest_manifest.ManifestIngestType.TABULAR
            case IngestType.AVRO_STREAM:
                result = ingest_manifest.ManifestIngestType.AVRO_STREAM
            case IngestType.JSON_L:
                result = ingest_manifest.ManifestIngestType.JSON_L
            case _:
                assert_never(self)
        return result


_MANIFEST_EPOCH_UNITS: dict[str, ingest_manifest.ManifestEpochTimeUnit] = {
    "seconds": ingest_manifest.ManifestEpochTimeUnit.SECONDS,
    "milliseconds": ingest_manifest.ManifestEpochTimeUnit.MILLISECONDS,
    "microseconds": ingest_manifest.ManifestEpochTimeUnit.MICROSECONDS,
    "nanoseconds": ingest_manifest.ManifestEpochTimeUnit.NANOSECONDS,
}

# The timestamp field in the canonical avro-stream schema. An avro output declares only how to read
# its timestamps: the schema fixes which field holds them, and the pipeline reads that field by name.
# This fills the series name the manifest type requires, and reaches only the file's own metadata.
_AVRO_TIMESTAMPS_FIELD = "timestamps"


def _manifest_timestamp_metadata(
    series_name: str, timestamp_type: ts._AnyNumericTimestampType
) -> ingest_manifest.ManifestTimestampMetadata:
    """Validate a per-output timestamp type against the manifest contract and convert it.

    Manifest outputs express numeric timestamps in seconds through nanoseconds, either absolute
    (:class:`ts.Epoch`) or offsets from a starting time (:class:`ts.Relative`); outputs needing
    richer types (ISO 8601, custom formats) must omit per-output metadata and rely on the
    job-level timestamp metadata, which supports the full range.
    """
    typed = ts._to_typed_timestamp_type(timestamp_type)
    if not isinstance(typed, (ts.Epoch, ts.Relative)):
        raise ExtractorError(
            f"per-output timestamp metadata only supports numeric epoch timestamps (ts.Epoch) and "
            f"relative timestamps (ts.Relative), not {typed!r}; "
            "omit it and rely on the job-level timestamp metadata for richer timestamp types"
        )
    unit = _MANIFEST_EPOCH_UNITS.get(typed.unit)
    if unit is None:
        raise ExtractorError(
            f"per-output timestamp metadata does not support time unit {typed.unit!r}; "
            f"supported units are {', '.join(_MANIFEST_EPOCH_UNITS)}"
        )
    return ingest_manifest.ManifestTimestampMetadata(
        series_name=series_name,
        epoch_time_unit=unit,
        relative_offset=ts._SecondsNanos.from_flexible(typed.start).to_iso8601()
        if isinstance(typed, ts.Relative)
        else None,
    )


def _optional_manifest_timestamp_metadata(
    series_name: str | None, timestamp_type: ts._AnyNumericTimestampType | None
) -> ingest_manifest.ManifestTimestampMetadata | None:
    """Convert a timestamp column/type pair, which must be given together or not at all.

    Formats that let the author name the timestamp field -- a tabular column, a journal JSON field --
    take the pair. One without the other describes nothing, so it is rejected here.

    A half-specified pair raises `ValueError`, like every other malformed argument in this client;
    `ExtractorError` is for violations of the extractor's own contract.
    """
    ts._validate_timestamp_pair(series_name, timestamp_type)
    if series_name is None or timestamp_type is None:
        return None
    return _manifest_timestamp_metadata(series_name, timestamp_type)
