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


def _manifest_timestamp_metadata(
    series_name: str, timestamp_type: ts._AnyTimestampType
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
