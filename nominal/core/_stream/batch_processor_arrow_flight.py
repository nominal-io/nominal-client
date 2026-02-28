"""Arrow Flight batch processor for streaming time series data.

Replaces the HTTP JSON/protobuf transport with Arrow Flight DoPut.
Groups enqueued items by (channel, tags, type), computes series UUIDs client-side,
and sends all series interleaved in a single Arrow RecordBatch over one DoPut stream.

This is a drop-in replacement: users call the same `stream.enqueue()` API.
"""

from __future__ import annotations

import itertools
import json
import logging
from typing import Sequence

import pyarrow as pa
import pyarrow.flight as flight

from nominal.core._stream.series_hasher import series_uuid
from nominal.core._stream.write_stream import BatchItem, DataItem, PointType

logger = logging.getLogger(__name__)

# Arrow type for the value column, keyed by PointType
_ARROW_VALUE_TYPES: dict[PointType, pa.DataType] = {
    PointType.DOUBLE: pa.float64(),
    PointType.INT: pa.int64(),
    PointType.STRING: pa.utf8(),
}


def process_batch_arrow_flight(
    batch: Sequence[DataItem],
    nominal_data_source_rid: str,
    flight_uri: str,
    org_rid: str = "",
    auth_header: str = "",
) -> None:
    """Process a batch of data items by sending them via Arrow Flight DoPut.

    Groups items by (channel, tags, type), computes a series UUID for each group,
    and sends all data in a single Arrow RecordBatch with schema (series, timestamp, value).

    The FlightDescriptor includes series_metadata and org_rid so that stream-consumer
    can write series rows to ClickHouse and Postgres without additional DB lookups.
    """
    if not batch:
        return

    data_source_uuid = _extract_locator(nominal_data_source_rid)

    # Group by (channel, tags, type) — same grouping as other batch processors
    sorted_batch = sorted(batch, key=BatchItem.sort_key)
    grouped = itertools.groupby(sorted_batch, key=BatchItem.sort_key)

    # Build interleaved arrays: all groups go into one batch
    series_values: list[str] = []
    timestamp_values: list[int] = []
    double_values: list[float | None] = []
    value_type: PointType | None = None
    series_metadata: dict[str, dict] = {}

    for _key, group_iter in grouped:
        group = list(group_iter)
        point_type = group[0].get_point_type()

        # For hackweek, only support scalar DOUBLE
        if point_type not in _ARROW_VALUE_TYPES:
            logger.warning("Arrow Flight: unsupported point type %s, falling back to DOUBLE", point_type)
            continue

        if value_type is None:
            value_type = point_type
        elif value_type != point_type:
            # Mixed types in one batch — send separately
            # For hackweek, just warn and skip non-matching types
            logger.warning(
                "Arrow Flight: mixed types in batch (%s vs %s), skipping non-matching group",
                value_type,
                point_type,
            )
            continue

        channel = group[0].channel_name
        tags = dict(group[0].tags) if group[0].tags else {}
        sid = str(series_uuid(channel, data_source_uuid, tags, point_type.name))

        series_metadata[sid] = {
            "channel": channel,
            "tags": tags,
            "data_type": point_type.name,
        }

        for item in group:
            series_values.append(sid)
            timestamp_values.append(item.timestamp)
            double_values.append(float(item.value))  # type: ignore[arg-type]

    if not series_values:
        return

    if value_type is None:
        value_type = PointType.DOUBLE

    # Build Arrow RecordBatch
    schema = pa.schema(
        [
            ("series", pa.utf8()),
            ("timestamp", pa.timestamp("ns", tz="UTC")),
            ("value", _ARROW_VALUE_TYPES[value_type]),
        ]
    )

    arrays = [
        pa.array(series_values, type=pa.utf8()),
        pa.array(timestamp_values, type=pa.timestamp("ns", tz="UTC")),
        pa.array(double_values, type=_ARROW_VALUE_TYPES[value_type]),
    ]
    record_batch = pa.record_batch(arrays, schema=schema)

    descriptor_payload: dict = {
        "dataset_rid": nominal_data_source_rid,
        "org_rid": org_rid,
        "series_metadata": series_metadata,
    }
    if auth_header:
        descriptor_payload["auth_header"] = auth_header
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(descriptor_payload).encode("utf-8")
    )

    # Connect and send
    client = flight.connect(flight_uri)
    try:
        writer, _reader = client.do_put(descriptor, schema)
        writer.write_batch(record_batch)
        writer.close()
    finally:
        client.close()

    logger.info(
        "Arrow Flight: sent %d points (%d series) to %s",
        len(series_values),
        len(set(series_values)),
        flight_uri,
    )


def _extract_locator(rid: str) -> str:
    """Extract the locator (last segment) from a resource identifier."""
    last_dot = rid.rfind(".")
    if last_dot >= 0:
        return rid[last_dot + 1 :]
    return rid
