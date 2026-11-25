from __future__ import annotations

import json
import logging
import shutil
import tempfile
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import BinaryIO

import requests
from google.protobuf.timestamp_pb2 import Timestamp
from urllib3.exceptions import HTTPError

from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.channel import Channel
from nominal.core.client import NominalClient
from nominal.protos.compute.v1 import expressions_pb2
from nominal.protos.dataexports.v1 import data_exports_pb2 as data
from nominal.protos.exportjobs.v1 import export_jobs_pb2 as jobs
from nominal.protos.sql.v1 import sql_pb2
from nominal.protos.types import common_pb2
from nominal.protos.types.time import time_pb2
from nominal.ts import (
    _SecondsNanos,
    _to_proto_duration,
)

logger = logging.getLogger(__name__)

MAX_SQL_QUERY_LENGTH = 100_000


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _channel_filter(channels: Sequence[Channel], tags: Mapping[str, str]) -> str:
    names = ", ".join(_sql_string(ch.name) for ch in channels)
    filters = [
        f"dataset_rid = {_sql_string(channels[0].data_source)}",
        f"channel IN ({names})",
        *(f"tags[{_sql_string(key)}] = {_sql_string(value)}" for key, value in tags.items()),
    ]
    return " WHERE " + " AND ".join(filters)


def raw_storage_types(
    client: NominalClient, channels: Sequence[Channel], tags: Mapping[str, str]
) -> Iterator[tuple[str, str]]:
    """Read the physical types for one dataset. The logical channel type can mask signed and unsigned storage."""
    query = "SELECT DISTINCT channel, data_type FROM channels" + _channel_filter(channels, tags)
    if len(query) > MAX_SQL_QUERY_LENGTH:
        if len(channels) > 1:
            middle = len(channels) // 2
            yield from raw_storage_types(client, channels[:middle], tags)
            yield from raw_storage_types(client, channels[middle:], tags)
        return  # A single oversized predicate already requires compute.
    request = sql_pb2.SqlServiceQueryRequest(
        workspace_rid=client._clients.resolve_default_workspace_rid(),
        query=query,
        result_format=sql_pb2.SQL_SERVICE_QUERY_RESULT_FORMAT_JSON_LINES,
    )
    with translate_grpc_errors():
        payload = b"".join(part.payload for part in client._clients.sql.Query(request))
    for line in payload.splitlines():
        row = json.loads(line)
        yield row["channel"], row["data_type"]


def raw_export_query(kind: str, channels: Sequence[Channel], start: int, end: int, tags: Mapping[str, str]) -> str:
    # All caller-controlled strings are SQL literals.
    start_literal, end_literal = (
        _SecondsNanos.from_nanoseconds(ns).to_iso8601().removesuffix("Z").replace("T", " ") for ns in (start, end)
    )
    where = (
        _channel_filter(channels, tags) + f" AND ts >= TIMESTAMP '{start_literal}' AND ts <= TIMESTAMP '{end_literal}'"
    )
    if kind in {"points_double", "points_int", "points_string"}:
        source = f'SELECT ts, channel, "value" FROM {kind}' + where
    else:
        # The channel API can report DOUBLE for physically stored Int64. A scalar UNION would round large integers.
        floats = 'SELECT ts, channel, "value", CAST(NULL AS BIGINT) AS integer_value FROM points_double' + where
        integers = (
            'SELECT ts, channel, CAST(NULL AS DOUBLE) AS "value", "value" AS integer_value FROM points_int' + where
        )
        source = f"SELECT * FROM ({floats} UNION ALL {integers}) AS samples"
    return source + " ORDER BY ts, channel"


def compute_export_request(
    workspace: str,
    channels: Mapping[str, Channel],
    start: int,
    end: int,
    tags: Mapping[str, str],
    buckets: int | None,
    resolution: int | None,
) -> jobs.CreateExportJobRequest:
    if buckets is not None:
        spacing = data.ExportResolution(bucket_count=buckets)
    elif resolution is not None:
        spacing = data.ExportResolution(step=_to_proto_duration(resolution, common_pb2.Duration))
    else:
        spacing = data.ExportResolution(full=data.FullResolution())
    start_time = Timestamp()
    start_time.FromNanoseconds(start)
    end_time = Timestamp()
    end_time.FromNanoseconds(end)
    definition = data.ExportDefinition(
        context=expressions_pb2.ComputeContext(),
        start_time=start_time,
        end_time=end_time,
        resolution=spacing,
        format=data.ExportFileFormat(csv=data.CsvFormat()),
        compression=data.EXPORT_COMPRESSION_GZIP,
        time_domain=data.TimeDomainColumns(
            columns=[
                data.ExportColumn(
                    name=name,
                    series=ch._to_proto_compute_series(tags),
                )
                for name, ch in channels.items()
            ],
            gap_fill=data.GapFill(leave_empty=data.LeaveEmpty()),
            timestamp_format=data.ExportTimestampFormat(
                relative=data.RelativeTimestamps(
                    relative_to=Timestamp(),
                    unit=time_pb2.NANOSECONDS,
                )
            ),
        ),
    )
    return jobs.CreateExportJobRequest(workspace_rid=workspace, compute=jobs.ComputeExportSource(definition=definition))


@contextmanager
def download_export(client: NominalClient, request: jobs.CreateExportJobRequest) -> Iterator[BinaryIO]:
    started = time.monotonic()
    service = client._clients.export_jobs
    with translate_grpc_errors():
        export_id = service.CreateExportJob(request).export_job.export_id
        lookup = jobs.GetExportJobRequest(export_id=export_id)
        interval = 0.25
        while True:
            job = service.GetExportJob(lookup).export_job
            if job.status == jobs.EXPORT_JOB_STATUS_COMPLETED:
                break
            if job.status not in (jobs.EXPORT_JOB_STATUS_SUBMITTED, jobs.EXPORT_JOB_STATUS_IN_PROGRESS):
                raise RuntimeError(f"Export job {export_id} ended with status {jobs.ExportJobStatus.Name(job.status)}")
            time.sleep(interval)
            interval = min(interval * 2, 2.0)

    # Both compute and SQL write a single object. Only GetExportJob supplies the URL.
    if len(job.files) != 1 or not job.files[0].download_url:
        raise RuntimeError(f"Export job {export_id} did not return a single downloadable file")
    ready = time.monotonic()
    with tempfile.TemporaryFile() as file:
        try:
            with requests.get(
                job.files[0].download_url, stream=True, timeout=(10, 120), verify=client._clients.trust_store_path
            ) as response:
                if response.status_code != 200:
                    raise RuntimeError(f"Export job {export_id} download returned HTTP {response.status_code}")
                shutil.copyfileobj(response.raw, file, length=1024 * 1024)
        except (requests.RequestException, HTTPError):
            # HTTP exceptions can contain the signed URL, which is a credential.
            raise RuntimeError(f"Could not download the result of export job {export_id}") from None
        if job.files[0].size_bytes and file.tell() != job.files[0].size_bytes:
            raise RuntimeError(f"Incomplete file for export job {export_id}")
        logger.debug(
            "Export job %s (%s): ready in %.3fs, downloaded %d bytes in %.3fs",
            export_id,
            request.WhichOneof("source"),
            ready - started,
            file.tell(),
            time.monotonic() - ready,
        )
        file.seek(0)
        yield file
