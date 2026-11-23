from __future__ import annotations

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
from nominal.protos.types import common_pb2
from nominal.protos.types.time import time_pb2
from nominal.ts import (
    _SecondsNanos,
    _to_proto_duration,
)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def raw_export_query(table: str, channels: Sequence[Channel], start: int, end: int, tags: Mapping[str, str]) -> str:
    # Only the table name comes from the SDK. All caller-controlled strings are SQL literals.
    names = ", ".join(_sql_string(ch.name) for ch in channels)
    start_literal, end_literal = (
        _SecondsNanos.from_nanoseconds(ns).to_iso8601().removesuffix("Z").replace("T", " ") for ns in (start, end)
    )
    filters = [
        f"dataset_rid = {_sql_string(channels[0].data_source)}",
        f"channel IN ({names})",
        f"ts >= TIMESTAMP '{start_literal}'",
        f"ts <= TIMESTAMP '{end_literal}'",
        *(f"tags[{_sql_string(key)}] = {_sql_string(value)}" for key, value in tags.items()),
    ]
    return f'SELECT ts, channel, "value" FROM {table} WHERE ' + " AND ".join(filters) + " ORDER BY ts"


def compute_export_request(
    workspace: str,
    channels: Sequence[Channel],
    start: int,
    end: int,
    tags: Mapping[str, str],
    buckets: int | None,
    resolution: int | None,
    timestamp_alias: str,
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
                    name=timestamp_alias if ch.name == "timestamp" else ch.name,
                    series=ch._to_proto_compute_series(tags),
                )
                for ch in channels
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
    with tempfile.TemporaryFile() as file:
        try:
            with requests.get(job.files[0].download_url, stream=True, timeout=(10, 120)) as response:
                if response.status_code != 200:
                    raise RuntimeError(f"Export job {export_id} download returned HTTP {response.status_code}")
                shutil.copyfileobj(response.raw, file, length=1024 * 1024)
        except (requests.RequestException, HTTPError):
            # HTTP exceptions can contain the signed URL, which is a credential.
            raise RuntimeError(f"Could not download the result of export job {export_id}") from None
        if job.files[0].size_bytes and file.tell() != job.files[0].size_bytes:
            raise RuntimeError(f"Incomplete file for export job {export_id}")
        file.seek(0)
        yield file
