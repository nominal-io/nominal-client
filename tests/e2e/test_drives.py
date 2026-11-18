from __future__ import annotations

import io
import pathlib
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset_file import IngestStatus
from nominal.exceptions import NominalAlreadyExistsError, NominalNotFoundError
from nominal.experimental import drives
from nominal.experimental.drives import (
    Drive,
    DriveFileState,
    FileStoreErrorCode,
    ManagedDriveFile,
    NominalFileStoreError,
)
from nominal.experimental.ingest import IngestBuilder
from tests.e2e import POLL_INTERVAL

E2E_DRIVE_ID = "sdk-e2e"
"""Drives cannot be deleted and archiving one needs admin rights, so every run shares one drive
and removes the files it puts there."""


@pytest.fixture(scope="module")
def drive(client: NominalClient) -> Drive:
    """The shared e2e drive, created on first use in a workspace."""
    try:
        return drives.get_drive_by_id(client, E2E_DRIVE_ID)
    except NominalNotFoundError:
        pass
    try:
        return drives.create_drive(client, E2E_DRIVE_ID, description="Scratch drive for nominal-client e2e tests")
    except NominalAlreadyExistsError:
        # A concurrent run created it between the lookup and the create.
        return drives.get_drive_by_id(client, E2E_DRIVE_ID)


@pytest.fixture
def run_dir() -> str:
    """A directory no other run writes to, so concurrent runs never collide on a path."""
    return f"runs/{uuid4().hex}"


def test_uploaded_bytes_download_unchanged_from_the_file_and_its_revision(
    drive: Drive, run_dir: str, tmp_path: pathlib.Path
) -> None:
    """A put stores exactly the bytes given, and both the file and its revision serve them back."""
    part_size = 5 * 1024**2
    content = b"a" * part_size + b"last part"
    uploaded = drive.put_file_obj(io.BytesIO(content), f"{run_dir}/payload.bin", chunk_size=part_size)
    try:
        assert uploaded.size_bytes == len(content)
        assert drive.get_file(uploaded.path).rid == uploaded.rid

        (tmp_path / "file").mkdir()
        assert uploaded.download(tmp_path / "file", part_size=part_size).read_bytes() == content
        (revision,) = uploaded.revisions()
        (tmp_path / "revision").mkdir()
        assert revision.download(tmp_path / "revision").read_bytes() == content
    finally:
        uploaded.remove()


def test_a_file_can_be_moved_removed_and_restored(drive: Drive, run_dir: str) -> None:
    """Each change takes effect on the live service, and removal keeps content restorable."""
    file = drive.put_file_obj(io.BytesIO(b"a,b\n1,2\n"), f"{run_dir}/incoming.csv")

    file.move_to(f"{run_dir}/archive.csv")
    assert file.path == f"{run_dir}/archive.csv"

    file.remove()
    assert file.state is DriveFileState.REMOVED

    content_revision = next(r for r in file.revisions() if r.state is DriveFileState.ACTIVE)
    restored = content_revision.restore(f"{run_dir}/restored.csv")
    try:
        assert isinstance(restored, ManagedDriveFile)
        assert restored.path == f"{run_dir}/restored.csv"
        assert restored.state is DriveFileState.ACTIVE
    finally:
        restored.remove()


def test_putting_onto_an_occupied_path_reports_path_already_exists(drive: Drive, run_dir: str) -> None:
    """A put never overwrites; the service's in-band failure decodes to a typed error code."""
    first = drive.put_file_obj(io.BytesIO(b"first"), f"{run_dir}/taken.txt")
    try:
        with pytest.raises(NominalFileStoreError) as excinfo:
            drive.put_file_obj(io.BytesIO(b"second"), f"{run_dir}/taken.txt")
        assert excinfo.value.code is FileStoreErrorCode.PATH_ALREADY_EXISTS
    finally:
        first.remove()


def test_putting_onto_a_file_adds_a_version_and_keeps_its_identity(
    drive: Drive, run_dir: str, tmp_path: pathlib.Path
) -> None:
    """New content put onto an existing file becomes its next revision rather than a new file."""
    first = drive.put_file_obj(io.BytesIO(b"v1"), f"{run_dir}/O'Brien 50%.txt")
    second = drive.put_file_obj(io.BytesIO(b"version two"), first)
    try:
        assert second.rid == first.rid
        assert second.current_revision_rid != first.current_revision_rid
        assert len(second.revisions()) == 2
        assert second.download(tmp_path).read_bytes() == b"version two"
    finally:
        second.remove()


def test_ingest_builder_uses_pinned_revision_alongside_local_file(
    client: NominalClient, drive: Drive, run_dir: str, tmp_path: pathlib.Path, archive
) -> None:
    """Ingesting a captured revision reads its original content even after the Drive file changes."""
    dataset = client.create_dataset(f"drive-ingest-e2e-{uuid4().hex[:8]}")
    archive(dataset)
    uploaded = drive.put_file_obj(io.BytesIO(b"time,original\n1767268800,1\n"), f"{run_dir}/readings.csv")
    try:
        (revision,) = uploaded.revisions()
        builder = IngestBuilder(client, dataset).add_csv(revision, "time", "epoch_seconds")
        uploaded = drive.put_file_obj(io.BytesIO(b"time,replacement\n1767268800,2\n"), uploaded)
        local = tmp_path / "local.csv"
        local.write_text("time,local\n1767268800,3\n")
        job = builder.add_csv(local, "time", "epoch_seconds").submit()
        results = list(job.as_files_ingested(poll_interval=POLL_INTERVAL))
        assert len(results) == 2
        assert all(result.ingest_status == IngestStatus.SUCCESS for result in results)
        assert dataset.get_channel("original").name == "original"
        assert dataset.get_channel("local").name == "local"
    finally:
        uploaded.remove()
