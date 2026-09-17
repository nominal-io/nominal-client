"""File-ingest options and validation for the experimental ingest builder."""

from __future__ import annotations

from typing import Mapping

from nominal.core.filetype import FileType
from nominal.protos.ingest.v2 import common_pb2, file_ingest_pb2
from nominal.ts import Epoch, Relative, _AnyNumericTimestampType, _AnyTimestampType, _to_typed_timestamp_type


def tabular_file_options(
    file_type: FileType,
    timestamp_column: str,
    timestamp_type: _AnyTimestampType,
    *,
    tag_columns: Mapping[str, str] | None = None,
    units: Mapping[str, str] | None = None,
    channel_prefix: str | None = None,
    channel_name_overrides: Mapping[str, str] | None = None,
    header_row: int | None = None,
    data_row: int | None = None,
    units_row: int | None = None,
) -> file_ingest_pb2.FileIngestOptions:
    """Build tabular options, preserving omitted CSV row fields and copying maps."""
    if not file_type.is_csv() and not file_type.is_parquet():
        raise ValueError(f"unsupported tabular file type: {file_type}")
    rows = {"header_row": header_row, "data_row": data_row, "units_row": units_row}
    if not file_type.is_csv() and any(row is not None for row in rows.values()):
        raise ValueError("header_row, data_row, and units_row are only supported for CSV files")
    for name, row in rows.items():
        if row is not None and (isinstance(row, bool) or not isinstance(row, int) or not 1 <= row <= 2**31 - 1):
            raise ValueError(f"{name} must be a positive one-based int32 record number")
    effective_header = 1 if header_row is None else header_row
    effective_data = effective_header + 1 if data_row is None else data_row
    if effective_data <= effective_header:
        raise ValueError("data_row must be greater than header_row")
    if units_row == effective_header:
        raise ValueError("units_row must not be header_row")
    if units_row is not None and units_row >= effective_data:
        raise ValueError("units_row must be less than data_row, which defaults to header_row + 1")

    options = file_ingest_pb2.FileIngestOptions(
        timestamp_metadata=common_pb2.TimestampMetadata(
            column=timestamp_column, type=_to_typed_timestamp_type(timestamp_type)._to_proto()
        ),
        units=units,
        channel_prefix=channel_prefix,
        channel_name_overrides=channel_name_overrides,
    )
    wide_format = file_ingest_pb2.WideFormat(tag_columns=tag_columns)
    if file_type.is_csv():
        options.csv.CopyFrom(
            file_ingest_pb2.CsvIngestOptions(
                format=file_ingest_pb2.CsvFormat(wide=wide_format),
                header_row=header_row,
                data_row=data_row,
                units_row=units_row,
            )
        )
    else:
        options.parquet.CopyFrom(
            file_ingest_pb2.ParquetIngestOptions(
                format=file_ingest_pb2.ParquetFormat(wide=wide_format),
                is_archive=file_type.is_parquet_archive(),
            )
        )
    return options


def avro_file_options(
    *,
    timestamp_type: _AnyNumericTimestampType | None = None,
    units: Mapping[str, str] | None = None,
    channel_prefix: str | None = None,
) -> file_ingest_pb2.FileIngestOptions:
    """Build canonical Avro stream options with numeric timestamps only."""
    declared_type = Epoch(unit="nanoseconds") if timestamp_type is None else _to_typed_timestamp_type(timestamp_type)
    if not isinstance(declared_type, (Epoch, Relative)):
        raise ValueError("avro stream timestamps must be numeric (ts.Epoch or ts.Relative)")
    return file_ingest_pb2.FileIngestOptions(
        timestamp_metadata=common_pb2.TimestampMetadata(column="timestamps", type=declared_type._to_proto()),
        units=units,
        channel_prefix=channel_prefix,
        avro=file_ingest_pb2.AvroIngestOptions(),
    )
