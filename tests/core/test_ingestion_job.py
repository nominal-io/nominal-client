from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from typing import Callable, Iterator, Sequence
from unittest.mock import MagicMock, patch

import pytest
from nominal_api import ingest_api

from nominal.core.dataset_file import IngestStatus
from nominal.core.exceptions import NominalIngestFailed, NominalIngestTimeout
from nominal.core.ingestion_job import IngestionJob, IngestionJobStatus


class _StopPolling(Exception):
    """Raised from a mocked sleep to end a wait loop that would otherwise never reach its deadline."""


def _job_bean(**overrides: object) -> ingest_api.IngestJob:
    """Build a conjure IngestJob bean with sensible defaults for tests."""
    kwargs: dict[str, object] = dict(
        ingest_job_rid="ri.ingest.test.ingest-job.0000",
        status=ingest_api.IngestJobStatus.IN_PROGRESS,
        ingest_type=ingest_api.IngestType.TABULAR,
        created_by="11111111-1111-1111-1111-111111111111",
        org_uuid="22222222-2222-2222-2222-222222222222",
        created_by_rid="ri.authn.test.user.abc",
        dataset_rid="ri.catalog.test.dataset.def",
        origin_files=None,
        produced_file_count=None,
        created_at=None,
        start_time=None,
        end_time=None,
    )
    kwargs.update(overrides)
    return ingest_api.IngestJob(**kwargs)


def test_cancel_calls_service_and_refreshes(mock_clients: MagicMock) -> None:
    """cancel() calls the cancel endpoint and refreshes the job in place from the response."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.cancel_ingest_job.return_value = _job_bean(status=ingest_api.IngestJobStatus.CANCELLED)

    result = job.cancel()

    mock_clients.ingest_jobs.cancel_ingest_job.assert_called_once_with(mock_clients.auth_header, job.rid)
    assert result is job
    assert job.status is IngestionJobStatus.CANCELLED


def test_dataset_files_paginates_and_maps(mock_clients: MagicMock) -> None:
    """dataset_files() pages through the per-job files endpoint, threading the page token."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean())

    raw_file_1 = object()
    raw_file_2 = object()
    raw_file_3 = object()
    mock_clients.catalog.get_dataset_files_for_job.side_effect = [
        SimpleNamespace(files=[raw_file_1, raw_file_2], next_page="t2"),
        SimpleNamespace(files=[raw_file_3], next_page=None),
    ]

    sentinels = {raw_file_1: "f1", raw_file_2: "f2", raw_file_3: "f3"}
    with patch(
        "nominal.core.ingestion_job._dataset_file_from_conjure",
        side_effect=lambda _clients, raw_file: sentinels[raw_file],
    ):
        result = job.dataset_files()

    assert result == ["f1", "f2", "f3"]
    assert mock_clients.catalog.get_dataset_files_for_job.call_count == 2
    first_call = mock_clients.catalog.get_dataset_files_for_job.call_args_list[0]
    assert first_call.args == (mock_clients.auth_header, job.rid, None)
    second_call = mock_clients.catalog.get_dataset_files_for_job.call_args_list[1]
    assert second_call.args[2] == "t2"


def _page(*files: object) -> SimpleNamespace:
    """Build a single-page get_dataset_files_for_job response."""
    return SimpleNamespace(files=list(files), next_page=None)


def _responses(*values: object) -> Callable[..., object]:
    """Successive mock responses, repeating the last one for any further calls."""
    remaining = list(values)

    def _next(*_args: object, **_kwargs: object) -> object:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return _next


def _make_file(file_id: str, statuses: Sequence[IngestStatus]) -> MagicMock:
    """Build a stand-in dataset file whose ingest_status advances one step per refresh."""
    file = MagicMock()
    file.id = file_id
    file.dataset_rid = "ri.catalog.test.dataset.def"
    file.ingest_status = statuses[0]
    observed = iter(statuses)

    def refresh(_: object) -> MagicMock:
        file.ingest_status = next(observed, statuses[-1])
        return file

    file._refresh_from_api.side_effect = refresh
    return file


def _advance_polled_files(files: Sequence[MagicMock], **_kwargs: object) -> set[str]:
    """Stand in for _batch_refresh_files: refresh every polled file, report none absent."""
    for file in files:
        file._refresh_from_api(None)
    return set()


@contextmanager
def _polling() -> Iterator[MagicMock]:
    """Patch out file refreshes, conjure file conversion, and sleeping; yield the sleep mock."""
    with (
        patch("nominal.core.dataset_file._batch_refresh_files", side_effect=_advance_polled_files),
        patch("nominal.core.ingestion_job._dataset_file_from_conjure", side_effect=lambda _clients, file: file),
        patch("nominal.core.ingestion_job.time.sleep") as mock_sleep,
    ):
        yield mock_sleep


def test_as_files_ingested_waits_for_files_registered_after_the_call(mock_clients: MagicMock) -> None:
    """Files that do not exist yet when the wait starts are still yielded once the job registers them."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS),
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS),
        _job_bean(status=ingest_api.IngestJobStatus.COMPLETED),
    )
    first = _make_file("first-file", [IngestStatus.SUCCESS])
    second = _make_file("second-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(
        _page(),
        _page(),
        _page(first, second),
    )

    with _polling():
        yielded = list(job.as_files_ingested())

    assert yielded == [first, second]


def test_as_files_ingested_yields_completed_files_while_the_job_is_still_running(mock_clients: MagicMock) -> None:
    """A file that finishes ingesting is yielded without waiting for the job to reach a terminal status."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS)
    )
    file = _make_file("only-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(file))

    with _polling():
        first_yielded = next(iter(job.as_files_ingested()))

    assert first_yielded is file
    assert job.status is IngestionJobStatus.IN_PROGRESS


def test_as_files_ingested_does_not_poll_a_job_that_is_already_terminal(mock_clients: MagicMock) -> None:
    """An already-completed job yields its files without re-fetching the job or sleeping."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED))
    file = _make_file("only-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(file))

    with _polling() as mock_sleep:
        yielded = list(job.as_files_ingested())

    assert yielded == [file]
    mock_clients.ingest_jobs.get_ingest_job.assert_not_called()
    mock_sleep.assert_not_called()


def test_as_files_ingested_yields_each_file_once_when_the_list_grows(mock_clients: MagicMock) -> None:
    """A file seen on an earlier poll is not yielded again when it reappears in a later file listing."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS),
        _job_bean(status=ingest_api.IngestJobStatus.COMPLETED),
    )
    first = _make_file("first-file", [IngestStatus.IN_PROGRESS, IngestStatus.SUCCESS])
    second = _make_file("second-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(
        _page(first),
        _page(first, second),
    )

    with _polling():
        yielded = list(job.as_files_ingested())

    assert yielded == [first, second]


def test_as_files_ingested_skips_relisting_files_while_the_produced_count_is_unchanged(
    mock_clients: MagicMock,
) -> None:
    """The file list is not re-read on a poll where the job's produced file count has not moved."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS, produced_file_count=1),
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS, produced_file_count=1),
        _job_bean(status=ingest_api.IngestJobStatus.COMPLETED, produced_file_count=1),
    )
    file = _make_file("only-file", [IngestStatus.IN_PROGRESS, IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(file))

    with _polling():
        yielded = list(job.as_files_ingested())

    assert yielded == [file]
    assert mock_clients.catalog.get_dataset_files_for_job.call_count == 2


def test_as_files_ingested_yields_files_that_failed_to_ingest(mock_clients: MagicMock) -> None:
    """A completed job's failed file is yielded rather than dropped, so the caller can see its status."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED))
    failed = _make_file("failed-file", [IngestStatus.FAILED])
    succeeded = _make_file("good-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(failed, succeeded))

    with _polling():
        yielded = list(job.as_files_ingested())

    assert yielded == [failed, succeeded]
    assert failed.ingest_status is IngestStatus.FAILED


def test_as_files_ingested_yields_a_file_absent_from_the_batch_response(mock_clients: MagicMock) -> None:
    """A file that disappears mid-wait is treated as done rather than polled forever."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED))
    file = _make_file("deleted-file", [IngestStatus.IN_PROGRESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(file))

    with (
        patch("nominal.core.dataset_file._batch_refresh_files", return_value={"deleted-file"}),
        patch("nominal.core.ingestion_job._dataset_file_from_conjure", side_effect=lambda _clients, f: f),
        patch("nominal.core.ingestion_job.time.sleep"),
    ):
        yielded = list(job.as_files_ingested())

    assert yielded == [file]


def test_as_files_ingested_raises_when_the_job_fails(mock_clients: MagicMock) -> None:
    """A job that ends FAILED raises rather than quietly yielding nothing."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.FAILED)
    )
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page())

    with _polling(), pytest.raises(NominalIngestFailed, match="failed"):
        list(job.as_files_ingested())


def test_as_files_ingested_names_ingested_files_when_the_job_fails(mock_clients: MagicMock) -> None:
    """A failing job's error names the files that did ingest, which the raise discards from list()."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.FAILED)
    )
    file = _make_file("landed-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(file))

    with _polling(), pytest.raises(NominalIngestFailed, match="landed-file"):
        list(job.as_files_ingested())


def test_as_files_ingested_yields_without_raising_when_the_job_is_cancelled(mock_clients: MagicMock) -> None:
    """A cancelled job yields what did ingest instead of raising, since cancelling was the caller's ask."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.CANCELLED)
    )
    file = _make_file("landed-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(file))

    with _polling():
        yielded = list(job.as_files_ingested())

    assert yielded == [file]


def test_as_files_ingested_stops_on_an_unrecognized_job_status(mock_clients: MagicMock) -> None:
    """An unrecognized job status ends the wait instead of looping on it forever."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.UNKNOWN)
    )
    file = _make_file("only-file", [IngestStatus.SUCCESS])
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page(file))

    with _polling():
        yielded = list(job.as_files_ingested())

    assert yielded == [file]
    assert job.status is IngestionJobStatus.UNKNOWN


def test_as_files_ingested_raises_timeout_while_the_job_is_still_running(mock_clients: MagicMock) -> None:
    """An exhausted wait budget raises rather than blocking on a job that is still running."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS)
    )
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page())

    with _polling() as mock_sleep, pytest.raises(NominalIngestTimeout, match="in_progress"):
        list(job.as_files_ingested(timeout=timedelta(0)))

    mock_sleep.assert_not_called()


def test_as_files_ingested_does_not_sleep_past_the_timeout_deadline(mock_clients: MagicMock) -> None:
    """A poll interval longer than the remaining budget is shortened to the budget, not slept in full."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(
        _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS)
    )
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(_page())

    with _polling() as mock_sleep:
        # Sleep is mocked, so no wall-clock time passes; stop at the first sleep to inspect its length.
        mock_sleep.side_effect = _StopPolling
        with pytest.raises(_StopPolling):
            list(job.as_files_ingested(poll_interval=timedelta(minutes=5), timeout=timedelta(seconds=2)))

    assert 0 < mock_sleep.call_args.args[0] <= 2
