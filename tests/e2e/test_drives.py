from __future__ import annotations

import io
import pathlib
from uuid import uuid4

import pytest

from nominal.core import (
    Drive,
    DriveFileState,
    FileStoreErrorCode,
    ManagedDriveFile,
    NominalClient,
    NominalFileStoreError,
)
from nominal.exceptions import NominalAlreadyExistsError, NominalNotFoundError

E2E_DRIVE_ID = "sdk-e2e"
"""Drives cannot be deleted and archiving one needs admin rights, so every run shares one drive
and removes the files it puts there."""


@pytest.fixture(scope="module")
def drive(client: NominalClient) -> Drive:
    """The shared e2e drive, created on first use in a workspace."""
    try:
        return client.get_drive_by_id(E2E_DRIVE_ID)
    except NominalNotFoundError:
        pass
    try:
        return client.create_drive(E2E_DRIVE_ID, description="Scratch drive for nominal-client e2e tests")
    except NominalAlreadyExistsError:
        # A concurrent run created it between the lookup and the create.
        return client.get_drive_by_id(E2E_DRIVE_ID)


@pytest.fixture
def run_dir() -> str:
    """A directory no other run writes to, so concurrent runs never collide on a path."""
    return f"runs/{uuid4().hex}"


def test_uploaded_bytes_download_unchanged_from_the_file_and_its_revision(
    drive: Drive, run_dir: str, tmp_path: pathlib.Path
) -> None:
    """A put stores exactly the bytes given, and both the file and its revision serve them back."""
    content = uuid4().bytes * 4096
    uploaded = drive.put_file_obj(io.BytesIO(content), f"{run_dir}/payload.bin")
    try:
        assert uploaded.size_bytes == len(content)
        assert drive.get_file(uploaded.path).rid == uploaded.rid

        (tmp_path / "file").mkdir()
        assert uploaded.download(tmp_path / "file").read_bytes() == content
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
