from __future__ import annotations

import json
import pathlib
from concurrent.futures import CancelledError, Future
from datetime import datetime, timezone
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest

from nominal.core.exceptions import NominalIngestError, NominalMultipartUploadFailed
from nominal.core.filetype import FileType, FileTypes
from nominal.experimental.ingest._ingest_builder import IngestBuilder, MultipartUploader, _PendingFile, _upload_all

WriteFile = Callable[[str, int], pathlib.Path]


def pending_file(path: pathlib.Path) -> _PendingFile:
    """A CSV `_PendingFile`, as the builder registers them for upload."""
    return _PendingFile(path=path, file_type=FileTypes.CSV)


class FakeUploader:
    """Stands in for MultipartUploader: every enqueued file settles immediately.

    Records how its context manager was left, because the builder relies on an exception
    escaping the `with` block to trigger the uploader's cancelling shutdown.
    """

    def __init__(self, results: dict[str, object]) -> None:
        """Take the outcome each file name settles with: a location string or an exception.

        Files not named in `results` — generated manifest files have random names — settle
        with a location derived from their own name.
        """
        self.results = results
        self.enqueued: list[pathlib.Path] = []
        self.enqueued_contents: list[bytes] = []  # captured at enqueue: temp files die at submit
        self.exit_exc_type: type[BaseException] | None = None

    def enqueue_file(
        self,
        path: pathlib.Path,
        *,
        file_type: FileType | None = None,
        name: str | None = None,
        part_size: int | None = None,
    ) -> Future[str]:
        self.enqueued.append(path)
        self.enqueued_contents.append(path.read_bytes())
        fut: Future[str] = Future()
        outcome = self.results.get(path.name, f"s3://bucket/{path.name}")
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


def upload_with(fake: FakeUploader, files: list[_PendingFile], client: MagicMock | None = None) -> MagicMock:
    """Run `_upload_all` against `fake` instead of a real uploader; return the patched `create`."""
    with patch.object(MultipartUploader, "create", autospec=True, return_value=fake) as create:
        _upload_all(files, client if client is not None else MagicMock())
    return create


@pytest.fixture
def one_file_builder(write_file: WriteFile) -> tuple[MagicMock, IngestBuilder]:
    """A builder holding one registered CSV, on a MagicMock client."""
    client = MagicMock()
    builder = IngestBuilder(client, "ri.catalog.test.dataset").add_tabular_data(
        write_file("a.csv", 1), timestamp_column="ts", timestamp_type="epoch_seconds"
    )
    return client, builder


class TestUploadAll:
    def test_returns_each_files_location_keyed_by_the_file(self, write_file: WriteFile) -> None:
        """Every file's location comes back keyed by the file object itself — no order to get wrong."""
        files = [pending_file(write_file("a.csv", 1)), pending_file(write_file("b.csv", 1))]
        fake = FakeUploader({"a.csv": "s3://bucket/a", "b.csv": "s3://bucket/b"})

        with patch.object(MultipartUploader, "create", autospec=True, return_value=fake):
            assert _upload_all(files, MagicMock()) == {files[0]: "s3://bucket/a", files[1]: "s3://bucket/b"}

    def test_passes_the_client_to_the_uploader(self, write_file: WriteFile) -> None:
        """`create` derives auth, workspace, and transport from the client, so it must reach it.

        `autospec=True` is what makes this bite: a call that no longer matches `create`'s real
        signature fails here instead of only in production.
        """
        client = MagicMock()

        create = upload_with(
            FakeUploader({"a.csv": "s3://bucket/a"}), [pending_file(write_file("a.csv", 1))], client=client
        )

        (passed_client,) = create.call_args.args  # autospec on a classmethod drops `cls`
        assert passed_client is client
        assert create.call_args.kwargs == {}  # the uploader's defaults govern; nothing overridden

    def test_no_files_never_builds_an_uploader(self) -> None:
        """An empty batch must not spin up thread pools and an HTTP session for nothing."""
        with patch.object(MultipartUploader, "create", autospec=True) as create:
            assert _upload_all([], MagicMock()) == {}
        create.assert_not_called()

    @pytest.mark.parametrize(
        "failure",
        [
            NominalMultipartUploadFailed("part 1 failed", [RuntimeError("boom")]),
            # A file the uploader's abnormal shutdown cut short settles with CancelledError as
            # its exception. Nothing here may assume a multipart failure is the only way an
            # upload ends badly.
            CancelledError("uploader is closing"),
        ],
        ids=["multipart-failure", "cancelled"],
    )
    def test_any_upload_failure_propagates(self, write_file: WriteFile, failure: BaseException) -> None:
        """Whatever exception a file settles with reaches the caller unchanged."""
        with pytest.raises(type(failure)):
            upload_with(FakeUploader({"a.csv": failure}), [pending_file(write_file("a.csv", 1))])

    def test_failure_leaves_the_uploader_context_by_raising(self, write_file: WriteFile) -> None:
        """The `with` block must see the exception: that is what cancels the rest of the batch.

        Swallowing it would leave the remaining files uploading after the caller was told the
        batch failed.
        """
        fake = FakeUploader({"a.csv": RuntimeError("upload failed")})

        with pytest.raises(RuntimeError, match="upload failed"):
            upload_with(fake, [pending_file(write_file("a.csv", 1))])

        assert fake.exit_exc_type is RuntimeError


class TestSubmit:
    def test_submitted_items_carry_their_own_files_locations(self, write_file: WriteFile) -> None:
        """Each item in the sent request holds the storage location of its own file, none other's.

        This is the invariant the whole builder exists to uphold: uploads and items can never
        desync, because every item is constructed directly from its own files' locations.
        """
        client = MagicMock()
        builder = IngestBuilder(client, "ri.catalog.test.dataset")
        builder.add_tabular_data(write_file("a.csv", 1), timestamp_column="ts", timestamp_type="epoch_seconds")
        builder.add_tabular_data(write_file("b.csv", 1), timestamp_column="ts", timestamp_type="epoch_seconds")

        with patch.object(
            MultipartUploader,
            "create",
            autospec=True,
            return_value=FakeUploader({"a.csv": "s3://bucket/a", "b.csv": "s3://bucket/b"}),
        ):
            builder.submit()

        (request,) = client._clients.ingest_v2.Ingest.call_args.args
        assert [item.file.source.s3.path for item in request.items] == ["s3://bucket/a", "s3://bucket/b"]

    def test_the_same_path_registered_twice_uploads_twice(self, write_file: WriteFile) -> None:
        """Two registrations of one path are independent uploads and independent items."""
        path = write_file("a.csv", 1)
        client = MagicMock()
        builder = IngestBuilder(client, "ri.catalog.test.dataset")
        builder.add_tabular_data(path, timestamp_column="ts", timestamp_type="epoch_seconds")
        builder.add_tabular_data(path, timestamp_column="ts", timestamp_type="epoch_seconds")
        fake = FakeUploader({"a.csv": "s3://bucket/a"})

        with patch.object(MultipartUploader, "create", autospec=True, return_value=fake):
            builder.submit()

        assert len(fake.enqueued) == 2
        (request,) = client._clients.ingest_v2.Ingest.call_args.args
        assert [item.file.source.s3.path for item in request.items] == ["s3://bucket/a", "s3://bucket/a"]

    def test_video_with_start_sends_a_starting_timestamp(self, write_file: WriteFile) -> None:
        """A start-timestamped video item carries its source, channel, and first-frame instant."""
        start = datetime(2026, 7, 26, 6, 0, tzinfo=timezone.utc)
        client = MagicMock()
        builder = IngestBuilder(client, "ri.catalog.test.dataset")
        builder.add_video(write_file("cam.mp4", 1), channel="camera.front", start=start)

        with patch.object(
            MultipartUploader, "create", autospec=True, return_value=FakeUploader({"cam.mp4": "s3://bucket/cam"})
        ):
            builder.submit()

        (request,) = client._clients.ingest_v2.Ingest.call_args.args
        video = request.items[0].video
        assert video.source.s3.path == "s3://bucket/cam"
        assert video.ingest.channel == "camera.front"
        assert video.ingest.timestamp_manifest.no_manifest.starting_timestamp.seconds == int(start.timestamp())

    def test_video_frame_timestamps_upload_as_a_manifest_alongside(self, write_file: WriteFile) -> None:
        """Per-frame timestamps ride along as a generated JSON manifest linked to the video item."""
        fake = FakeUploader({"cam.mp4": "s3://bucket/cam"})
        client = MagicMock()
        builder = IngestBuilder(client, "ri.catalog.test.dataset")
        builder.add_video(write_file("cam.mp4", 1), channel="camera.front", frame_timestamps=[1, 2, 3])

        with patch.object(MultipartUploader, "create", autospec=True, return_value=fake):
            builder.submit()

        _video_path, manifest_path = fake.enqueued
        assert json.loads(fake.enqueued_contents[1]) == [1, 2, 3]  # what the manifest carried when uploaded
        (request,) = client._clients.ingest_v2.Ingest.call_args.args
        manifest = request.items[0].video.ingest.timestamp_manifest
        assert [s.s3.path for s in manifest.timestamp_manifest_files.sources] == [f"s3://bucket/{manifest_path.name}"]
        assert not manifest_path.exists()  # the generated temp manifest is deleted once uploads finish

    def test_a_failed_submit_still_deletes_the_generated_manifest(self, write_file: WriteFile) -> None:
        """A builder-generated manifest must not outlive a failed submit — the builder is single-use."""
        builder = IngestBuilder(MagicMock(), "ri.catalog.test.dataset")
        builder.add_video(write_file("cam.mp4", 1), channel="camera.front", frame_timestamps=[1, 2, 3])
        fake = FakeUploader({"cam.mp4": RuntimeError("boom")})

        with patch.object(MultipartUploader, "create", autospec=True, return_value=fake):
            with pytest.raises(RuntimeError, match="boom"):
                builder.submit()

        _video_path, manifest_path = fake.enqueued
        assert not manifest_path.exists()

    def test_tabular_dispatches_on_extension(self, write_file: WriteFile) -> None:
        """One method handles both tabular formats: the extension picks the wire options."""
        client = MagicMock()
        builder = IngestBuilder(client, "ri.catalog.test.dataset")
        builder.add_tabular_data(write_file("a.csv", 1), timestamp_column="ts", timestamp_type="epoch_seconds")
        builder.add_tabular_data(write_file("b.parquet", 1), timestamp_column="ts", timestamp_type="epoch_seconds")

        with patch.object(MultipartUploader, "create", autospec=True, return_value=FakeUploader({})):
            builder.submit()

        (request,) = client._clients.ingest_v2.Ingest.call_args.args
        assert request.items[0].file.ingest.HasField("csv")
        assert request.items[1].file.ingest.HasField("parquet")

    def test_tabular_rejects_a_non_tabular_path(self, write_file: WriteFile) -> None:
        """A non-tabular extension fails at registration, before any upload."""
        builder = IngestBuilder(MagicMock(), "ri.catalog.test.dataset")
        with pytest.raises(ValueError, match="tabular"):
            builder.add_tabular_data(write_file("cam.mp4", 1), timestamp_column="ts", timestamp_type="epoch_seconds")

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            pytest.param({}, "exactly one", id="neither"),
            pytest.param({"start": 0, "frame_timestamps": [1]}, "exactly one", id="both"),
            pytest.param({"frame_timestamps": []}, "at least one timestamp", id="empty-frame-timestamps"),
        ],
    )
    def test_video_requires_exactly_one_usable_timestamp_source(
        self, write_file: WriteFile, kwargs: dict[str, Any], message: str
    ) -> None:
        """A video needs a start instant or non-empty per-frame timestamps — exactly one, never both."""
        builder = IngestBuilder(MagicMock(), "ri.catalog.test.dataset")
        with pytest.raises(ValueError, match=message):
            builder.add_video(write_file("cam.mp4", 1), channel="camera.front", **kwargs)

    def test_video_rejects_a_non_video_path_before_uploading(self, write_file: WriteFile) -> None:
        """A non-video path fails at registration, not after uploading a possibly-huge wrong file."""
        builder = IngestBuilder(MagicMock(), "ri.catalog.test.dataset")
        with pytest.raises(ValueError, match="video path"):
            builder.add_video(write_file("readings.csv", 1), channel="camera.front", start=0)

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
