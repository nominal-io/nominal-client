from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Callable, Mapping, Sequence
from unittest.mock import MagicMock

import pytest
from nominal_api import api, ingest_api, scout_catalog

from nominal.core.dataset_file import DatasetFile, IngestStatus
from nominal.core.exceptions import NominalIngestFailed, NominalIngestTimeout
from nominal.core.ingestion_job import IngestionJob, IngestionJobStatus

DATASET_RID = "ri.catalog.test.dataset.def"

IN_PROGRESS = api.IngestStatusV2(in_progress=api.InProgressResult())
SUCCESS = api.IngestStatusV2(success=api.SuccessResult())
FAILED = api.IngestStatusV2(error=api.ErrorResult(error_type="INTERNAL", message="boom"))

# These tests drive the job through the conjure clients and assert on what the caller receives, so the
# poll interval only sets how fast the loop spins. Nothing here patches time or module internals.
FAST = timedelta(0)


def _job_bean(**overrides: object) -> ingest_api.IngestJob:
    """Build a conjure IngestJob bean with sensible defaults for tests."""
    kwargs: dict[str, object] = dict(
        ingest_job_rid="ri.ingest.test.ingest-job.0000",
        status=ingest_api.IngestJobStatus.IN_PROGRESS,
        ingest_type=ingest_api.IngestType.TABULAR,
        created_by="11111111-1111-1111-1111-111111111111",
        org_uuid="22222222-2222-2222-2222-222222222222",
        created_by_rid="ri.authn.test.user.abc",
        dataset_rid=DATASET_RID,
        origin_files=None,
        produced_file_count=None,
        created_at=None,
        start_time=None,
        end_time=None,
    )
    kwargs.update(overrides)
    return ingest_api.IngestJob(**kwargs)


def _file_bean(file_id: str, ingest_status: api.IngestStatusV2) -> scout_catalog.DatasetFile:
    """Build a conjure DatasetFile bean as the catalog service would return it."""
    return scout_catalog.DatasetFile(
        dataset_rid=DATASET_RID,
        handle=scout_catalog.Handle(s3=scout_catalog.S3Handle(bucket="test-bucket", key=file_id)),
        id=file_id,
        ingest_status=ingest_status,
        name=f"{file_id}.csv",
        uploaded_at="2026-01-01T00:00:00Z",
    )


def _page(*files: scout_catalog.DatasetFile, next_page: str | None = None) -> scout_catalog.DatasetFilesPage:
    """Build one page of a get_dataset_files_for_job response."""
    return scout_catalog.DatasetFilesPage(files=list(files), next_page=next_page)


def _responses(*values: object) -> Callable[..., object]:
    """Successive service responses, repeating the last one for any further calls."""
    remaining = list(values)

    def _next(*_args: object, **_kwargs: object) -> object:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return _next


def _serve_job(mock_clients: MagicMock, *statuses: ingest_api.IngestJobStatus) -> None:
    """Serve the job status each successive refresh sees, repeating the last."""
    mock_clients.ingest_jobs.get_ingest_job.side_effect = _responses(*(_job_bean(status=s) for s in statuses))


def _serve_listing(mock_clients: MagicMock, *pages: scout_catalog.DatasetFilesPage) -> None:
    """Serve successive file listings for the job, repeating the last."""
    mock_clients.catalog.get_dataset_files_for_job.side_effect = _responses(*pages)


def _serve_refreshes(mock_clients: MagicMock, *states: Mapping[str, api.IngestStatusV2]) -> None:
    """Serve successive batch refreshes, each mapping a file id to the status it now reports.

    A file left out of a state is absent from that batch response, which is how the service reports one
    that has been deleted.
    """
    mock_clients.catalog.batch_get_dataset_files.side_effect = _responses(
        *({file_id: _file_bean(file_id, status) for file_id, status in state.items()} for state in states)
    )


def _ids(files: Sequence[DatasetFile]) -> list[str]:
    """The ids of the dataset files a wait produced, in order."""
    return [file.id for file in files]


def test_cancel_calls_service_and_refreshes(mock_clients: MagicMock) -> None:
    """cancel() cancels the job server-side and leaves the local job reporting the new status."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    mock_clients.ingest_jobs.cancel_ingest_job.return_value = _job_bean(status=ingest_api.IngestJobStatus.CANCELLED)

    result = job.cancel()

    mock_clients.ingest_jobs.cancel_ingest_job.assert_called_once_with(mock_clients.auth_header, job.rid)
    assert result is job
    assert job.status is IngestionJobStatus.CANCELLED


def test_dataset_files_returns_every_page(mock_clients: MagicMock) -> None:
    """dataset_files() returns the files from every page of the listing, not just the first."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean())
    _serve_listing(
        mock_clients,
        _page(_file_bean("f1", SUCCESS), _file_bean("f2", SUCCESS), next_page="t2"),
        _page(_file_bean("f3", SUCCESS)),
    )

    assert _ids(job.dataset_files()) == ["f1", "f2", "f3"]


def test_as_files_ingested_waits_for_files_registered_after_the_call(mock_clients: MagicMock) -> None:
    """Files that do not exist yet when the wait starts are still yielded once the job registers them."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(
        mock_clients,
        ingest_api.IngestJobStatus.IN_PROGRESS,
        ingest_api.IngestJobStatus.IN_PROGRESS,
        ingest_api.IngestJobStatus.COMPLETED,
    )
    _serve_listing(mock_clients, _page(), _page(), _page(_file_bean("first", SUCCESS), _file_bean("second", SUCCESS)))
    _serve_refreshes(mock_clients, {"first": SUCCESS, "second": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["first", "second"]


def test_as_files_ingested_yields_completed_files_while_the_job_is_still_running(mock_clients: MagicMock) -> None:
    """A file that finishes ingesting is yielded without waiting for the job to reach a terminal status."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.IN_PROGRESS)
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    first_yielded = next(iter(job.as_files_ingested(poll_interval=FAST)))

    assert first_yielded.id == "only"
    assert job.status is IngestionJobStatus.IN_PROGRESS


def test_as_files_ingested_does_not_refetch_a_job_that_is_already_terminal(mock_clients: MagicMock) -> None:
    """An already-completed job yields its files without asking the service for the job again."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED))
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["only"]
    mock_clients.ingest_jobs.get_ingest_job.assert_not_called()


def test_as_files_ingested_yields_each_file_once_when_a_page_repeats_it(mock_clients: MagicMock) -> None:
    """A file served on two pages of one listing pass is yielded once, as offset paging can repeat rows."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED))
    _serve_listing(
        mock_clients,
        _page(_file_bean("repeated", SUCCESS), _file_bean("other", SUCCESS), next_page="t2"),
        _page(_file_bean("repeated", SUCCESS)),
    )
    _serve_refreshes(mock_clients, {"repeated": SUCCESS, "other": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["repeated", "other"]


def test_as_files_ingested_yields_each_file_once_when_the_list_grows(mock_clients: MagicMock) -> None:
    """A file seen on an earlier poll is not yielded again when it reappears in a later file listing."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.IN_PROGRESS, ingest_api.IngestJobStatus.COMPLETED)
    _serve_listing(
        mock_clients,
        _page(_file_bean("first", IN_PROGRESS)),
        _page(_file_bean("first", SUCCESS), _file_bean("second", SUCCESS)),
    )
    _serve_refreshes(mock_clients, {"first": IN_PROGRESS}, {"first": SUCCESS, "second": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["first", "second"]


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
    _serve_listing(mock_clients, _page(_file_bean("only", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {"only": IN_PROGRESS}, {"only": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["only"]
    assert mock_clients.catalog.get_dataset_files_for_job.call_count == 2


def test_as_files_ingested_warns_when_fewer_files_listed_than_the_job_reports(
    mock_clients: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Listing fewer files than the job's produced count is reported rather than passing silently."""
    job = IngestionJob._from_conjure(
        mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED, produced_file_count=3)
    )
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    with caplog.at_level(logging.WARNING):
        yielded = list(job.as_files_ingested(poll_interval=FAST))

    assert _ids(yielded) == ["only"]
    assert "reports 3 produced file(s) but only 1 could be listed" in caplog.text


def test_as_files_ingested_yields_files_that_failed_to_ingest(mock_clients: MagicMock) -> None:
    """A completed job's failed file is yielded rather than dropped, so the caller can see its status."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED))
    _serve_listing(mock_clients, _page(_file_bean("bad", IN_PROGRESS), _file_bean("good", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {"bad": FAILED, "good": SUCCESS})

    yielded = list(job.as_files_ingested(poll_interval=FAST))

    assert _ids(yielded) == ["bad", "good"]
    assert yielded[0].ingest_status is IngestStatus.FAILED


def test_as_files_ingested_yields_a_file_absent_from_the_batch_response(mock_clients: MagicMock) -> None:
    """A file that disappears mid-wait is treated as done rather than polled forever."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.COMPLETED))
    _serve_listing(mock_clients, _page(_file_bean("deleted", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["deleted"]


def test_as_files_ingested_raises_when_the_job_fails(mock_clients: MagicMock) -> None:
    """A job that ends FAILED raises rather than quietly yielding nothing."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.FAILED)
    _serve_listing(mock_clients, _page())

    with pytest.raises(NominalIngestFailed, match="failed"):
        list(job.as_files_ingested(poll_interval=FAST))


def test_as_files_ingested_names_ingested_files_when_the_job_fails(mock_clients: MagicMock) -> None:
    """The failure names the files that did ingest, so a partial result is still reconcilable."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.FAILED)
    _serve_listing(mock_clients, _page(_file_bean("landed", SUCCESS)))
    _serve_refreshes(mock_clients, {"landed": SUCCESS})

    with pytest.raises(NominalIngestFailed, match="landed"):
        list(job.as_files_ingested(poll_interval=FAST))


def test_as_files_ingested_yields_without_raising_when_the_job_is_cancelled(mock_clients: MagicMock) -> None:
    """A cancelled job yields what did ingest instead of raising, since cancelling was the caller's ask."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.CANCELLED)
    _serve_listing(mock_clients, _page(_file_bean("landed", SUCCESS)))
    _serve_refreshes(mock_clients, {"landed": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["landed"]


def test_as_files_ingested_stops_on_an_unrecognized_job_status(mock_clients: MagicMock) -> None:
    """A job status this client does not recognize ends the wait instead of looping on it forever.

    `IngestJobStatus.UNKNOWN` is what conjure decodes any status a newer server adds into, so this is
    the forward-compatibility case exactly as it arrives.
    """
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.UNKNOWN)
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["only"]
    assert job.status is IngestionJobStatus.UNKNOWN


def test_as_files_ingested_raises_timeout_while_the_job_is_still_running(mock_clients: MagicMock) -> None:
    """An exhausted wait budget raises rather than blocking on a job that is still running."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.IN_PROGRESS)
    _serve_listing(mock_clients, _page())

    with pytest.raises(NominalIngestTimeout, match="in_progress"):
        list(job.as_files_ingested(timeout=timedelta(0)))


def test_as_files_ingested_gives_up_on_the_timeout_not_a_full_poll_interval_later(mock_clients: MagicMock) -> None:
    """A timeout shorter than the poll interval ends the wait when the budget runs out, not an interval later."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.IN_PROGRESS)
    _serve_listing(mock_clients, _page())

    started = time.monotonic()
    with pytest.raises(NominalIngestTimeout):
        list(job.as_files_ingested(poll_interval=timedelta(seconds=5), timeout=timedelta(seconds=0.05)))

    assert time.monotonic() - started < 2


def test_as_files_ingested_timeout_names_the_pending_files_when_the_job_is_terminal(
    mock_clients: MagicMock,
) -> None:
    """A timeout on a job that finished blames its still-ingesting files, not the job's own status."""
    job = IngestionJob._from_conjure(mock_clients, _job_bean(status=ingest_api.IngestJobStatus.IN_PROGRESS))
    _serve_job(mock_clients, ingest_api.IngestJobStatus.COMPLETED)
    _serve_listing(mock_clients, _page(_file_bean("slow", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {"slow": IN_PROGRESS})

    with pytest.raises(NominalIngestTimeout, match="completed, but 1 of its file"):
        list(job.as_files_ingested(timeout=timedelta(0)))
