"""Apply a locally-authored ``nominal_compute`` expression to a run/dataset's channels and return the result.

The expression's references (``nominal_compute.NumericSeries.Reference("a")``) are bound to concrete channels at
execution time via the compute ``Context`` (i.e.) nothing is persisted back to Nominal.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any, BinaryIO, Mapping, cast

import pandas as pd
from conjure_python_client._serde.decoder import ConjureDecoder
from nominal_api import scout_compute_api, scout_dataexport_api

from nominal import ts
from nominal.core import NominalClient
from nominal.core.channel import Channel

if TYPE_CHECKING:
    import nominal_compute

_FlexibleTimestamp = str | datetime | ts.IntegralNanosecondsUTC


def _to_conjure_series(
    series: nominal_compute.NumericSeries | nominal_compute.CategoricalSeries,
) -> scout_compute_api.Series:
    """Convert a ``nominal_compute`` series expression into the ``scout_compute_api.Series`` the compute API expects."""
    wire_json = series.to_json()  # type: ignore[union-attr]
    if type(series).__name__ == "NumericSeries":
        numeric: scout_compute_api.NumericSeries = ConjureDecoder.do_decode(
            json.loads(wire_json), scout_compute_api.NumericSeries
        )
        return scout_compute_api.Series(numeric=numeric)
    enum: scout_compute_api.EnumSeries = ConjureDecoder.do_decode(json.loads(wire_json), scout_compute_api.EnumSeries)
    return scout_compute_api.Series(enum=enum)


def compute_series(
    client: NominalClient,
    expr: nominal_compute.NumericSeries | nominal_compute.CategoricalSeries,
    inputs: Mapping[str, Channel],
    start: _FlexibleTimestamp | None = None,
    end: _FlexibleTimestamp | None = None,
    *,
    tags: Mapping[str, Mapping[str, str]] | None = None,
    name: str = "value",
    enable_gzip: bool = True,
) -> pd.Series[Any]:
    """Compute a derived series by applying a ``nominal_compute`` expression to a set of channels.

    Args:
        client: The NominalClient to compute with.
        expr: The compute-as-code expression to evaluate (numeric or categorical), typically authored in
            ``nominal_compute`` and imported from your own repository.
        inputs: Maps each reference name used in ``expr`` to the :class:`~nominal.core.channel.Channel` it should
            resolve to. Look these up from the run/dataset the caller wants to compute against.
        start: Start of the time range to compute over. Defaults to the earliest supported timestamp.
        end: End of the time range to compute over. Defaults to the latest supported timestamp.
        tags: Optional tag filters keyed by the same reference names as ``inputs``, used to narrow a channel to a
            single tagged series when its name and data source alone match more than one.
        name: Name for the returned series (also the CSV column name requested from the export service).
        enable_gzip: If true, gzip the export from Nominal.

    Returns:
        A ``pandas.Series`` of the computed values, indexed by timestamp. The index name is "timestamp" and the series
        name is ``name``.
    """
    request = _build_export_request(
        compute_node=_to_conjure_series(expr),
        column_name=name,
        inputs=inputs,
        start=start,
        end=end,
        enable_gzip=enable_gzip,
        tags=tags,
    )
    response = client._clients.dataexport.export_channel_data(client._clients.auth_header, request)
    df = pd.read_csv(
        cast(BinaryIO, response),
        parse_dates=["timestamp"],
        index_col="timestamp",
        compression="gzip" if enable_gzip else "infer",
    )
    return df[name]


def _build_export_request(
    compute_node: scout_compute_api.Series,
    column_name: str,
    inputs: Mapping[str, Channel],
    start: _FlexibleTimestamp | None,
    end: _FlexibleTimestamp | None,
    enable_gzip: bool,
    tags: Mapping[str, Mapping[str, str]] | None,
) -> scout_dataexport_api.ExportDataRequest:
    start_ts = ts._MIN_TIMESTAMP.to_api() if start is None else ts._SecondsNanos.from_flexible(start).to_api()
    end_ts = ts._MAX_TIMESTAMP.to_api() if end is None else ts._SecondsNanos.from_flexible(end).to_api()
    return scout_dataexport_api.ExportDataRequest(
        channels=scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=[scout_dataexport_api.TimeDomainChannel(column_name=column_name, compute_node=compute_node)],
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    # only one series is exported, so there is nothing to merge
                    none=scout_dataexport_api.NoneStrategy(),
                ),
                output_timestamp_format=ts._to_export_timestamp_format("iso_8601"),
            )
        ),
        start_time=start_ts,
        end_time=end_ts,
        context=_build_context(inputs, tags),
        format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
        resolution=scout_dataexport_api.ResolutionOption(
            undecimated=scout_dataexport_api.UndecimatedResolution(),
        ),
        compression=scout_dataexport_api.CompressionFormat.GZIP if enable_gzip else None,
    )


def _build_context(
    inputs: Mapping[str, Channel],
    tags: Mapping[str, Mapping[str, str]] | None = None,
) -> scout_compute_api.Context:
    """Bind each reference name to its channel (and any tags), so ``expr``'s ``Reference(name)`` nodes resolve."""
    tags = tags or {}
    return scout_compute_api.Context(
        dataset_references={},
        variables={
            ref_name: scout_compute_api.VariableValue(channel=channel._to_channel_series(tags=tags.get(ref_name)))
            for ref_name, channel in inputs.items()
        },
        function_variables={},
    )
