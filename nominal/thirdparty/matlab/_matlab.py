from __future__ import annotations

import gzip
import logging
import pathlib
import shutil
from typing import Mapping, Sequence, overload

from google.protobuf.timestamp_pb2 import Timestamp

from nominal.core._utils.dataexport import export_timestamp_format, stream_export
from nominal.core.channel import Channel
from nominal.core.client import NominalClient
from nominal.protos.compute.v1 import expressions_pb2
from nominal.protos.dataexports.v1 import data_exports_pb2
from nominal.protos.types import common_pb2
from nominal.ts import (
    _MAX_TIMESTAMP,
    _MIN_TIMESTAMP,
    IntegralNanosecondsDuration,
    _AnyExportableTimestampType,
    _InferrableTimestampType,
    _SecondsNanos,
    _to_proto_duration,
)

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1024 * 1024


@overload
def export_channels_to_matlab(
    client: NominalClient,
    output_path: pathlib.Path,
    channels: Sequence[Channel],
    *,
    start_time: _InferrableTimestampType | None = None,
    end_time: _InferrableTimestampType | None = None,
    resolution: IntegralNanosecondsDuration,
    export_timestamp_type: _AnyExportableTimestampType = "iso_8601",
    forward_fill_lookback: IntegralNanosecondsDuration | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None: ...


@overload
def export_channels_to_matlab(
    client: NominalClient,
    output_path: pathlib.Path,
    channels: Sequence[Channel],
    *,
    start_time: _InferrableTimestampType | None = None,
    end_time: _InferrableTimestampType | None = None,
    num_buckets: int,
    export_timestamp_type: _AnyExportableTimestampType = "iso_8601",
    forward_fill_lookback: IntegralNanosecondsDuration | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None: ...


@overload
def export_channels_to_matlab(
    client: NominalClient,
    output_path: pathlib.Path,
    channels: Sequence[Channel],
    *,
    start_time: _InferrableTimestampType | None = None,
    end_time: _InferrableTimestampType | None = None,
    export_timestamp_type: _AnyExportableTimestampType = "iso_8601",
    forward_fill_lookback: IntegralNanosecondsDuration | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None: ...


def export_channels_to_matlab(
    client: NominalClient,
    output_path: pathlib.Path,
    channels: Sequence[Channel],
    *,
    tags: Mapping[str, str] | None = None,
    start_time: _InferrableTimestampType | None = None,
    end_time: _InferrableTimestampType | None = None,
    resolution: IntegralNanosecondsDuration | None = None,
    num_buckets: int | None = None,
    export_timestamp_type: _AnyExportableTimestampType = "iso_8601",
    forward_fill_lookback: IntegralNanosecondsDuration | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    r"""Export one or more channels to a MATLAB `.mat` file on disk.

    This function requests a server-side export of the given channels over a specified
    time range, and streams the result directly to the provided output path. The export
    is returned from the API as a gzip-compressed byte stream, which is decompressed
    on the fly and written to disk without loading the full dataset into memory.

    Args:
        client: The Nominal client used to issue the data export request
        output_path: Location on disk to write the resulting `.mat` file.

            **Note:** The parent directory will be created if it does not already exist.

            Must have a `.mat` suffix.
        channels: List of channels to export.

            **Note:** Must be non-empty.
        tags: Optional dictionary of tags to apply when exporting each channel.
        start_time: The minimum timestamp to include in the export.

            **Note:** If not provided, uses the earliest available timestamp.
        end_time: The maximum timestamp to include in the export.

            **Note:** If not provided, uses the latest available timestamp.
        resolution: Fixed resolution (in nanoseconds) to downsample the export data.

            **Note:** Mutually exclusive with `num_buckets`.
        num_buckets: Number of buckets to aggregate the selected time window into.

            **Note:** Mutually exclusive with `resolution`.
        export_timestamp_type: Format of exported timestamps. Defaults to string-based iso8601 timestamps.
        forward_fill_lookback: If provided, enables forward-filling of values at timestamps
            where data is missing, up to the given lookback duration. If not provided,
            missing values are left empty.
        chunk_size: Size in bytes of the buffer used while streaming the decompressed
            export to disk. Defaults to 1 MiB.

    Exceptions raised by the underlying API client or file I/O operations
    (e.g. network errors, filesystem errors) propagate to the caller.

    Raises:
        ValueError: If no channels are provided, if both `resolution` and `num_buckets`
            are specified, or if the output path does not have a `.mat` suffix.

    Example:
        ```python
        # Export undecimated data over a given time range
        export_channels_to_matlab(
            client=client,
            output_path=pathlib.Path("out/my_export.mat"),
            channels=[channel_a, channel_b],
            start_time=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC),
        )

        # Export with fixed resolution (100ms) and relative timestamps in microseconds
        export_channels_to_matlab(
            client=client,
            output_path=pathlib.Path("out/resampled.mat"),
            channels=[channel_a, channel_b],
            resolution=100_000_000,
            export_timestamp_type=Relative(
                "microseconds", datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
            ),
        )

        # Export with original resolution but timestamps as seconds since unix epoch
        export_channels_to_matlab(
            client=client,
            output_path=pathlib.Path("out/epoch_seconds.mat"),
            channels=[channel_a, channel_b],
            export_timestamp_type="epoch_seconds",
        )

        # Export bucketed data with forward fill up to 5 seconds
        export_channels_to_matlab(
            client=client,
            output_path=pathlib.Path("out/bucketed.mat"),
            channels=[channel_a, channel_b, channel_c],
            num_buckets=3600,
            forward_fill_lookback=5_000_000_000,
        )
        ```

    Usage in MATLAB:
        Once the `.mat` file is generated, you can load it directly into MATLAB using
        the built-in `load` function or by double-clicking the file in the MATLAB UI:

        ```matlab
        >> result = load("out/my_export.mat");
        ```

        The result contains a struct named `data`, where each field corresponds to a
        channel exported from Nominal, represented as a numeric array. You can convert
        this struct to a MATLAB table using `struct2table` for easier manipulation:

        ```matlab
        >> T = struct2table(result.data);
        ```

        If any channel name contains characters that are not valid MATLAB identifiers
        (for example, a period `"."`), you can still access it safely using the dynamic
        field reference syntax in either the struct or table form:

        ```matlab
        >> data.("channel.name")
        >> T.("channel.name")
        ```

        If your export includes timestamps as ISO-8601 strings, they will often appear
        as a cell array of character vectors in the table. You can convert them into
        native MATLAB `datetime` objects like so (replace `timestamps` with the actual
        field name if different):

        ```matlab
        % Normalize timestamps to always include a fractional second then convert to utc datetime
        >> T.timestamps = datetime( ...
            regexprep(T.timestamps, ':(\d{2})Z$', ':$1.000000Z'), ...
            'InputFormat', ...
            'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z''', ...
            'TimeZone', ...
            'UTC');
        ```

        After this conversion, you can use MATLAB's native time-series and plotting
        functions directly on the `timestamps` column.
    """
    if not channels:
        raise ValueError("No channels requested for export")
    elif resolution is not None and num_buckets is not None:
        raise ValueError("May only specify one of `resolution` or `num_buckets`")
    elif output_path.suffix != ".mat":
        raise ValueError(f"Output path {output_path} must have a suffix of '.mat', detected {output_path.suffix}")

    start = _MIN_TIMESTAMP if start_time is None else _SecondsNanos.from_flexible(start_time)
    end = _MAX_TIMESTAMP if end_time is None else _SecondsNanos.from_flexible(end_time)
    if resolution is not None:
        seconds, nanos = divmod(resolution, 1_000_000_000)
        export_resolution = data_exports_pb2.ExportResolution(step=common_pb2.Duration(seconds=seconds, nanos=nanos))
    elif num_buckets is not None:
        export_resolution = data_exports_pb2.ExportResolution(bucket_count=num_buckets)
    else:
        export_resolution = data_exports_pb2.ExportResolution(full=data_exports_pb2.FullResolution())

    gap_fill = (
        data_exports_pb2.GapFill(leave_empty=data_exports_pb2.LeaveEmpty())
        if forward_fill_lookback is None
        else data_exports_pb2.GapFill(
            forward_fill=data_exports_pb2.ForwardFill(
                max_fill_duration=_to_proto_duration(forward_fill_lookback, common_pb2.Duration)
            )
        )
    )
    request = data_exports_pb2.ExportDefinition(
        format=data_exports_pb2.ExportFileFormat(matfile=data_exports_pb2.MatfileFormat()),
        compression=data_exports_pb2.EXPORT_COMPRESSION_GZIP,
        start_time=Timestamp(seconds=start.seconds, nanos=start.nanos),
        end_time=Timestamp(seconds=end.seconds, nanos=end.nanos),
        resolution=export_resolution,
        time_domain=data_exports_pb2.TimeDomainColumns(
            columns=[
                data_exports_pb2.ExportColumn(name=channel.name, series=channel._to_proto_compute_series(tags))
                for channel in channels
            ],
            gap_fill=gap_fill,
            timestamp_format=export_timestamp_format(export_timestamp_type),
        ),
        context=expressions_pb2.ComputeContext(),
    )
    output_path.parent.mkdir(exist_ok=True, parents=True)
    with (
        stream_export(client._clients.grpc_dataexport, request) as response,
        gzip.GzipFile(fileobj=response, mode="rb") as gz,
        output_path.open(mode="wb") as wf,
    ):
        shutil.copyfileobj(gz, wf, length=chunk_size)
