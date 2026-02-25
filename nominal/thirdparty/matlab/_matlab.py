from __future__ import annotations

import gzip
import logging
import pathlib
import shutil
from typing import BinaryIO, Mapping, Sequence, cast, overload

from nominal_api import scout_compute_api, scout_dataexport_api

from nominal.core.channel import Channel
from nominal.core.client import NominalClient
from nominal.ts import (
    _MAX_TIMESTAMP,
    _MIN_TIMESTAMP,
    IntegralNanosecondsDuration,
    _AnyExportableTimestampType,
    _InferrableTimestampType,
    _SecondsNanos,
    _to_api_duration,
    _to_export_timestamp_format,
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
            NOTE: The parent directory will be created if it does not already exist.
            NOTE: Must have a `.mat` suffix.
        channels: List of channels to export.
            NOTE: Must be non-empty.
        tags: Optional dictionary of tags to apply when exporting each channel.
        start_time: The minimum timestamp to include in the export.
            NOTE: If not provided, uses the earliest available timestamp.
        end_time: The maximum timestamp to include in the export.
            NOTE: If not provided, uses the latest available timestamp.
        resolution: Fixed resolution (in nanoseconds) to downsample the export data.
            NOTE: Mutually exclusive with `num_buckets`.
        num_buckets: Number of buckets to aggregate the selected time window into.
            NOTE: Mutually exclusive with `resolution`.
        export_timestamp_type: Format of exported timestamps. Defaults to string-based iso8601 timestamps.
        forward_fill_lookback: If provided, enables forward-filling of values at timestamps
            where data is missing, up to the given lookback duration. If not provided,
            missing values are left empty.
        chunk_size: Size in bytes of the buffer used while streaming the decompressed
            export to disk. Defaults to 1 MiB.

    Raises:
        ValueError: If no channels are provided, if both `resolution` and `num_buckets`
            are specified, or if the output path does not have a `.mat` suffix.
        Any exceptions raised by the underlying API client or file I/O operations
            (e.g. network errors, filesystem errors).

    Example:
        ```python
        # Export undecimated data over a given time range
        export_channels_to_matlab(
            client,
            pathlib.Path("out/my_export.mat"),
            [channel_a, channel_b],
            start_time=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc),
        )

        # Export with fixed resolution (100ms) and relative timestamps in microseconds
        export_channels_to_matlab(
            client,
            pathlib.Path("out/resampled.mat"),
            [channel_a, channel_b],
            resolution=100_000_000,
            relative_to=datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc),
            relative_resolution="microseconds",
        )

        # Export bucketed data with forward fill up to 5 seconds
        export_channels_to_matlab(
            client,
            pathlib.Path("out/bucketed.mat"),
            [channel_a, channel_b, channel_c],
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

    api_start = (_MIN_TIMESTAMP if start_time is None else _SecondsNanos.from_flexible(start_time)).to_api()
    api_end = (_MAX_TIMESTAMP if end_time is None else _SecondsNanos.from_flexible(end_time)).to_api()

    # Since we already restrict users to providing at most one of resolution or num buckets,
    # we just have to use None for undecimated if one is provided or a empty struct if not
    api_undecimated = (
        None if (resolution is not None or num_buckets is not None) else scout_dataexport_api.UndecimatedResolution()
    )
    api_resolution = scout_dataexport_api.ResolutionOption(
        undecimated=api_undecimated, nanoseconds=resolution, buckets=num_buckets
    )

    # If the user specified forward fill settings, set the timestamp merging strategy
    merge_strategy = (
        scout_dataexport_api.MergeTimestampStrategy(none=scout_dataexport_api.NoneStrategy())
        if forward_fill_lookback is None
        else scout_dataexport_api.MergeTimestampStrategy(
            all_timestamps_forward_fill=scout_dataexport_api.AllTimestampsForwardFillStrategy(
                _to_api_duration(forward_fill_lookback)
            )
        )
    )

    # channels to export
    export_channels = scout_dataexport_api.ExportChannels(
        time_domain=scout_dataexport_api.ExportTimeDomainChannels(
            channels=[c._to_time_domain_channel(tags) for c in channels],
            merge_timestamp_strategy=merge_strategy,
            output_timestamp_format=_to_export_timestamp_format(export_timestamp_type),
        )
    )

    resp = client._clients.dataexport.export_channel_data(
        client._clients.auth_header,
        scout_dataexport_api.ExportDataRequest(
            format=scout_dataexport_api.ExportFormat(matfile=scout_dataexport_api.Matfile()),
            compression=scout_dataexport_api.CompressionFormat.GZIP,
            start_time=api_start,
            end_time=api_end,
            resolution=api_resolution,
            channels=export_channels,
            context=scout_compute_api.Context({}),
        ),
    )
    bin_resp = cast(BinaryIO, resp)

    output_path.parent.mkdir(exist_ok=True, parents=True)
    with (
        gzip.GzipFile(fileobj=bin_resp, mode="rb") as gz,
        output_path.open(mode="wb") as wf,
    ):
        shutil.copyfileobj(gz, wf, length=chunk_size)
