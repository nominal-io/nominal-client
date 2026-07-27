from __future__ import annotations

import pathlib
from concurrent.futures import CancelledError, Future
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.exceptions import NominalMultipartUploadFailed
from nominal.core.filetype import FileTypes
from nominal.experimental.ingest._ingest_builder import MultipartUploader, _Upload, _upload_all
from nominal.protos.ingest.v2 import file_ingest_pb2, ingest_service_pb2


def make_upload(path: pathlib.Path) -> _Upload:
    item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem())
    return _Upload(path=path, file_type=FileTypes.CSV, target=item.file.source)


class FakeUploader:
    """Stands in for MultipartUploader: every enqueued file settles immediately.

    Records how its context manager was left, because the builder relies on an exception
    escaping the `with` block to trigger the uploader's cancelling shutdown.
    """

    def __init__(self, results: dict[str, object]) -> None:
        """Take the outcome each file name settles with: a location string or an exception."""
        self.results = results
        self.exit_exc_type: type[BaseException] | None = None

    def enqueue_file(self, path, *, file_type=None, name=None, part_size=None):
        fut: Future[str] = Future()
        outcome = self.results[path.name]
        if isinstance(outcome, BaseException):
            fut.set_exception(outcome)
        else:
            fut.set_result(outcome)
        return fut

    def __enter__(self):
        """Enter the uploader context, exactly as the real uploader does."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Record how the block was left; never suppress, so failures reach the caller."""
        self.exit_exc_type = exc_type


def upload_with(fake: FakeUploader, uploads, workspace_rid="rid.workspace.test", clients=None):
    """Run `_upload_all` against `fake` instead of a real uploader."""
    with patch.object(MultipartUploader, "create", autospec=True, return_value=fake) as create:
        _upload_all(uploads, workspace_rid, clients if clients is not None else MagicMock())
    return create


class TestUploadAll:
    def test_fills_every_target_with_its_own_location(self, tmp_path) -> None:
        a, b = tmp_path / "a.csv", tmp_path / "b.csv"
        a.write_bytes(b"a")
        b.write_bytes(b"b")
        uploads = [make_upload(a), make_upload(b)]

        upload_with(FakeUploader({"a.csv": "s3://bucket/a", "b.csv": "s3://bucket/b"}), uploads)

        assert [u.target.s3.path for u in uploads] == ["s3://bucket/a", "s3://bucket/b"]

    def test_passes_the_clients_bundle_and_workspace_to_the_uploader(self, tmp_path) -> None:
        """`create` derives auth and headers from `clients`, so the bundle itself must reach it.

        `autospec=True` is what makes this bite: a call that no longer matches `create`'s real
        signature fails here instead of only in production.
        """
        a = tmp_path / "a.csv"
        a.write_bytes(b"a")
        clients = MagicMock()

        create = upload_with(
            FakeUploader({"a.csv": "s3://bucket/a"}), [make_upload(a)], workspace_rid="rid.workspace.x", clients=clients
        )

        (passed_clients,) = create.call_args.args  # autospec on a classmethod drops `cls`
        assert passed_clients is clients
        assert create.call_args.kwargs["workspace_rid"] == "rid.workspace.x"

    def test_no_files_never_builds_an_uploader(self) -> None:
        """An empty batch must not spin up thread pools and an HTTP session for nothing."""
        with patch.object(MultipartUploader, "create", autospec=True) as create:
            _upload_all([], "rid.workspace.test", MagicMock())
        create.assert_not_called()

    @pytest.mark.parametrize(
        "failure",
        [
            NominalMultipartUploadFailed("part 1 failed", [RuntimeError("boom")]),
            # A file the uploader's abnormal shutdown cut short settles with CancelledError as its
            # exception, and a driver caught mid-submit settles with RuntimeError. Nothing here may
            # assume a multipart failure is the only way an upload ends badly.
            CancelledError("uploader is closing"),
            RuntimeError("cannot schedule new futures after shutdown"),
        ],
        ids=["multipart-failure", "cancelled", "shutdown-race"],
    )
    def test_any_upload_failure_propagates(self, tmp_path, failure: BaseException) -> None:
        a = tmp_path / "a.csv"
        a.write_bytes(b"a")

        with pytest.raises(type(failure)):
            upload_with(FakeUploader({"a.csv": failure}), [make_upload(a)])

    def test_failure_leaves_the_uploader_context_by_raising(self, tmp_path) -> None:
        """The `with` block must see the exception: that is what cancels the rest of the batch.

        Swallowing it (or draining the futures outside the block) would leave the remaining
        files uploading, and — after a cancelling close — waiting on them would hang.
        """
        a = tmp_path / "a.csv"
        a.write_bytes(b"a")
        fake = FakeUploader({"a.csv": RuntimeError("upload failed")})

        with pytest.raises(RuntimeError, match="upload failed"):
            upload_with(fake, [make_upload(a)])

        assert fake.exit_exc_type is RuntimeError

    def test_targets_are_left_untouched_when_an_upload_fails(self, tmp_path) -> None:
        """Atomicity: a partially filled request must never be sendable."""
        a, b = tmp_path / "a.csv", tmp_path / "b.csv"
        a.write_bytes(b"a")
        b.write_bytes(b"b")
        uploads = [make_upload(a), make_upload(b)]

        with pytest.raises(RuntimeError, match="upload failed"):
            upload_with(FakeUploader({"a.csv": RuntimeError("upload failed"), "b.csv": "s3://bucket/b"}), uploads)

        assert uploads[0].target.s3.path == ""  # the failed file never produced a location
