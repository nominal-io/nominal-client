from __future__ import annotations

import pathlib
from concurrent.futures import CancelledError, Future
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.exceptions import NominalIngestError, NominalMultipartUploadFailed
from nominal.core.filetype import FileType, FileTypes
from nominal.experimental.ingest._ingest_builder import IngestBuilder, MultipartUploader, _Upload, _upload_all
from nominal.protos.ingest.v2 import file_ingest_pb2, ingest_service_pb2

WriteFile = Callable[[str, int], pathlib.Path]


def make_upload(path: pathlib.Path) -> _Upload:
    """An `_Upload` whose target aliases a fresh item's file source, as the builder produces."""
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

    def enqueue_file(
        self,
        path: pathlib.Path,
        *,
        file_type: FileType | None = None,
        name: str | None = None,
        part_size: int | None = None,
    ) -> Future[str]:
        fut: Future[str] = Future()
        outcome = self.results[path.name]
        if isinstance(outcome, BaseException):
            fut.set_exception(outcome)
        else:
            fut.set_result(str(outcome))
        return fut

    def __enter__(self) -> FakeUploader:
        """Enter the uploader context, exactly as the real uploader does."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: Any, traceback: Any) -> None:
        """Record how the block was left; never suppress, so failures reach the caller."""
        self.exit_exc_type = exc_type


def upload_with(fake: FakeUploader, uploads: list[_Upload], client: MagicMock | None = None) -> MagicMock:
    """Run `_upload_all` against `fake` instead of a real uploader; return the patched `create`."""
    with patch.object(MultipartUploader, "create", autospec=True, return_value=fake) as create:
        _upload_all(uploads, client if client is not None else MagicMock())
    return create


@pytest.fixture
def one_file_builder(write_file: WriteFile) -> tuple[MagicMock, IngestBuilder]:
    """A builder holding one registered CSV, on a MagicMock client."""
    client = MagicMock()
    builder = IngestBuilder(client, "ri.catalog.test.dataset").add_csv(
        write_file("a.csv", 1), timestamp_column="ts", timestamp_type="epoch_seconds"
    )
    return client, builder


class TestUploadAll:
    def test_fills_every_target_with_its_own_location(self, write_file: WriteFile) -> None:
        """Each upload's proto target receives that upload's own storage location, not another's."""
        uploads = [make_upload(write_file("a.csv", 1)), make_upload(write_file("b.csv", 1))]

        upload_with(FakeUploader({"a.csv": "s3://bucket/a", "b.csv": "s3://bucket/b"}), uploads)

        assert [u.target.s3.path for u in uploads] == ["s3://bucket/a", "s3://bucket/b"]

    def test_passes_the_client_to_the_uploader(self, write_file: WriteFile) -> None:
        """`create` derives auth, workspace, and transport from the client, so it must reach it.

        `autospec=True` is what makes this bite: a call that no longer matches `create`'s real
        signature fails here instead of only in production.
        """
        client = MagicMock()

        create = upload_with(
            FakeUploader({"a.csv": "s3://bucket/a"}), [make_upload(write_file("a.csv", 1))], client=client
        )

        (passed_client,) = create.call_args.args  # autospec on a classmethod drops `cls`
        assert passed_client is client
        assert create.call_args.kwargs == {}  # the uploader's defaults govern; nothing overridden

    def test_no_files_never_builds_an_uploader(self) -> None:
        """An empty batch must not spin up thread pools and an HTTP session for nothing."""
        with patch.object(MultipartUploader, "create", autospec=True) as create:
            _upload_all([], MagicMock())
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
    def test_any_upload_failure_propagates(self, write_file: WriteFile, failure: BaseException) -> None:
        """Whatever exception a file settles with reaches the caller unchanged."""
        with pytest.raises(type(failure)):
            upload_with(FakeUploader({"a.csv": failure}), [make_upload(write_file("a.csv", 1))])

    def test_failure_leaves_the_uploader_context_by_raising(self, write_file: WriteFile) -> None:
        """The `with` block must see the exception: that is what cancels the rest of the batch.

        Swallowing it would leave the remaining files uploading after the caller was told the
        batch failed.
        """
        fake = FakeUploader({"a.csv": RuntimeError("upload failed")})

        with pytest.raises(RuntimeError, match="upload failed"):
            upload_with(fake, [make_upload(write_file("a.csv", 1))])

        assert fake.exit_exc_type is RuntimeError

    def test_targets_are_left_untouched_when_an_upload_fails(self, write_file: WriteFile) -> None:
        """Atomicity: a partially filled request must never be sendable."""
        uploads = [make_upload(write_file("a.csv", 1)), make_upload(write_file("b.csv", 1))]

        with pytest.raises(RuntimeError, match="upload failed"):
            upload_with(FakeUploader({"a.csv": RuntimeError("upload failed"), "b.csv": "s3://bucket/b"}), uploads)

        assert uploads[0].target.s3.path == ""  # the failed file never produced a location


class TestSubmitSingleUse:
    def test_a_second_submit_raises_instead_of_reingesting(
        self, one_file_builder: tuple[MagicMock, IngestBuilder]
    ) -> None:
        """Builders are single-use: re-submitting would re-upload and double-ingest every item."""
        client, builder = one_file_builder

        with patch.object(
            MultipartUploader, "create", autospec=True, return_value=FakeUploader({"a.csv": "s3://bucket/a"})
        ):
            builder.submit()
            with pytest.raises(NominalIngestError, match="single-use"):
                builder.submit()

        client._clients.ingest_v2.Ingest.assert_called_once()

    def test_a_failed_submit_also_consumes_the_builder(self, one_file_builder: tuple[MagicMock, IngestBuilder]) -> None:
        """Even a failed trigger may have committed server-side, so no retry path exists."""
        client, builder = one_file_builder

        with patch.object(
            MultipartUploader, "create", autospec=True, return_value=FakeUploader({"a.csv": RuntimeError("boom")})
        ):
            with pytest.raises(RuntimeError, match="boom"):
                builder.submit()
            with pytest.raises(NominalIngestError, match="single-use"):
                builder.submit()

        client._clients.ingest_v2.Ingest.assert_not_called()
