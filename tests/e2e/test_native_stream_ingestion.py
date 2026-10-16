"""Staging coverage for the core journal stream ingestion capability."""

from __future__ import annotations

import gzip
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pytest

from nominal import ts
from nominal.core import FileTypes, NominalClient
from tests.e2e import POLL_INTERVAL


@pytest.mark.parametrize("compressed", [False, True], ids=["jsonl", "jsonl-gz"])
def test_journal_stream_matches_path_ingestion_storage_bytes_and_timestamps(
    client: NominalClient, archive, tmp_path: Path, compressed: bool
) -> None:
    """Core path and stream ingestion use the same upload bucket and retain bytes and custom epoch timestamps."""
    payload = b'{"source_ts":1767225600,"MESSAGE":"stream-first"}\n{"source_ts":1767225601,"MESSAGE":"stream-second"}\n'
    if compressed:
        payload = gzip.compress(payload)
    file_type = FileTypes.JOURNAL_JSONL_GZ if compressed else FileTypes.JOURNAL_JSONL
    path = tmp_path / f"journal{file_type.extension}"
    path.write_bytes(payload)

    path_dataset = client.create_dataset(f"journal-path-{uuid4().hex[:8]}")
    archive(path_dataset)
    path_file = path_dataset.add_journal_json(path, timestamp_column="source_ts", timestamp_type=ts.EPOCH_SECONDS)
    path_file.poll_until_ingestion_completed(interval=POLL_INTERVAL)

    stream_dataset = client.create_dataset(f"journal-stream-{uuid4().hex[:8]}")
    archive(stream_dataset)
    stream = BytesIO(payload)
    stream_file = stream_dataset.add_journal_json_from_io(
        stream,
        file_type=file_type,
        file_name=path.name,
        channel="events",
        timestamp_column="source_ts",
        timestamp_type=ts.EPOCH_SECONDS,
    )
    assert not stream.closed
    stream.close()
    stream_file.poll_until_ingestion_completed(interval=POLL_INTERVAL)

    path_handle = path_file._get_latest_api().handle.s3
    stream_handle = stream_file._get_latest_api().handle.s3
    assert path_handle is not None and stream_handle is not None
    assert stream_handle.bucket == path_handle.bucket
    assert stream_handle.key != path_handle.key
    assert stream_file.timestamp_channel == path_file.timestamp_channel == "source_ts"
    assert stream_file.timestamp_type == path_file.timestamp_type == ts.EPOCH_SECONDS

    download_directory = tmp_path / "downloaded"
    download_directory.mkdir()
    assert stream_file.download(download_directory).read_bytes() == payload
    expected = [(1767225600_000000000, "stream-first"), (1767225601_000000000, "stream-second")]
    assert sorted((log.timestamp, log.message) for log in path_dataset.get_channel("logs").search_logs()) == expected
    assert (
        sorted((log.timestamp, log.message) for log in stream_dataset.get_channel("events").search_logs()) == expected
    )
