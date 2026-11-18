from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Callable, Mapping, Sequence
from unittest.mock import MagicMock

import grpc
import pytest
import requests
from conjure_python_client import ConjureHTTPError
from google.protobuf.timestamp_pb2 import Timestamp
from nominal_api import api, ingest_api, scout_catalog

from nominal.core import NominalClient
from nominal.core.dataset_file import DatasetFile, IngestStatus
from nominal.core.ingestion_job import IngestionJob, IngestionJobStatus, IngestType
from nominal.exceptions import NominalError, NominalIngestFailed, NominalIngestTimeout, NominalNotFoundError
from nominal.protos.ingestjobs.v1 import ingest_jobs_pb2

DATASET_RID = "ri.catalog.test.dataset.def"

IN_PROGRESS = api.IngestStatusV2(in_progress=api.InProgressResult())
SUCCESS = api.IngestStatusV2(success=api.SuccessResult())
FAILED = api.IngestStatusV2(error=api.ErrorResult(error_type="INTERNAL", message="boom"))

# These tests drive the job through the service clients and assert on what the caller receives, so the
# poll interval only sets how fast the loop spins. Nothing here patches time or module internals.
FAST = timedelta(0)


def _job_proto(
    *,
    status: ingest_jobs_pb2.IngestJobStatus.ValueType = ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS,
    produced_file_count: int = 1,
) -> ingest_jobs_pb2.IngestJob:
    """Build a job response with one produced file unless the test specifies a count."""
    return ingest_jobs_pb2.IngestJob(
        rid="ri.ingest.test.ingest-job.0000",
        status=status,
        type=ingest_jobs_pb2.INGEST_JOB_TYPE_TABULAR,
        created_by="ri.authn.test.user.abc",
        dataset_rid=DATASET_RID,
        produced_file_count=produced_file_count,
    )


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


def _serve_job(mock_clients: MagicMock, *statuses: ingest_jobs_pb2.IngestJobStatus.ValueType) -> None:
    """Serve the job status each successive refresh sees, repeating the last."""
    mock_clients.grpc_ingest_jobs.GetIngestJob.side_effect = _responses(
        *(ingest_jobs_pb2.GetIngestJobResponse(ingest_job=_job_proto(status=s)) for s in statuses)
    )


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
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    mock_clients.grpc_ingest_jobs.CancelIngestJob.return_value = ingest_jobs_pb2.CancelIngestJobResponse(
        ingest_job=_job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_CANCELLED)
    )

    result = job.cancel()

    mock_clients.grpc_ingest_jobs.CancelIngestJob.assert_called_once_with(
        ingest_jobs_pb2.CancelIngestJobRequest(ingest_job_rid=job.rid)
    )
    assert result is job
    assert job.status is IngestionJobStatus.CANCELLED


def test_dataset_files_returns_every_page(mock_clients: MagicMock) -> None:
    """dataset_files() returns the files from every page of the listing, not just the first."""
    job = IngestionJob._from_proto(mock_clients, _job_proto())
    _serve_listing(
        mock_clients,
        _page(_file_bean("f1", SUCCESS), _file_bean("f2", SUCCESS), next_page="t2"),
        _page(_file_bean("f3", SUCCESS)),
    )

    assert _ids(job.dataset_files()) == ["f1", "f2", "f3"]


def test_as_files_ingested_waits_for_files_registered_after_the_call(mock_clients: MagicMock) -> None:
    """Files that do not exist yet when the wait starts are still yielded once the job registers them."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(
        mock_clients,
        ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS,
        ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS,
        ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED,
    )
    _serve_listing(mock_clients, _page(), _page(), _page(_file_bean("first", SUCCESS), _file_bean("second", SUCCESS)))
    _serve_refreshes(mock_clients, {"first": SUCCESS, "second": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["first", "second"]


def test_as_files_ingested_yields_completed_files_while_the_job_is_still_running(mock_clients: MagicMock) -> None:
    """A file that finishes ingesting is yielded without waiting for the job to reach a terminal status."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS)
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    first_yielded = next(iter(job.as_files_ingested(poll_interval=FAST)))

    assert first_yielded.id == "only"
    assert job.status is IngestionJobStatus.IN_PROGRESS


def test_as_files_ingested_does_not_refetch_a_job_that_is_already_terminal(mock_clients: MagicMock) -> None:
    """An already-completed job yields its files without asking the service for the job again."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED))
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["only"]
    mock_clients.grpc_ingest_jobs.GetIngestJob.assert_not_called()


def test_as_files_ingested_yields_each_file_once_when_a_page_repeats_it(mock_clients: MagicMock) -> None:
    """A file served on two pages of one listing pass is yielded once, as offset paging can repeat rows."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED))
    _serve_listing(
        mock_clients,
        _page(_file_bean("repeated", SUCCESS), _file_bean("other", SUCCESS), next_page="t2"),
        _page(_file_bean("repeated", SUCCESS)),
    )
    _serve_refreshes(mock_clients, {"repeated": SUCCESS, "other": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["repeated", "other"]


def test_as_files_ingested_yields_each_file_once_when_the_list_grows(mock_clients: MagicMock) -> None:
    """A file seen on an earlier poll is not yielded again when it reappears in a later file listing."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS, ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED)
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
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    mock_clients.grpc_ingest_jobs.GetIngestJob.side_effect = _responses(
        ingest_jobs_pb2.GetIngestJobResponse(
            ingest_job=_job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS, produced_file_count=1)
        ),
        ingest_jobs_pb2.GetIngestJobResponse(
            ingest_job=_job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS, produced_file_count=1)
        ),
        ingest_jobs_pb2.GetIngestJobResponse(
            ingest_job=_job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED, produced_file_count=1)
        ),
    )
    _serve_listing(mock_clients, _page(_file_bean("only", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {"only": IN_PROGRESS}, {"only": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["only"]
    assert mock_clients.catalog.get_dataset_files_for_job.call_count == 2


def test_as_files_ingested_warns_when_fewer_files_listed_than_the_job_reports(
    mock_clients: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Listing fewer files than the job's produced count is reported rather than passing silently."""
    job = IngestionJob._from_proto(
        mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED, produced_file_count=3)
    )
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    with caplog.at_level(logging.WARNING):
        yielded = list(job.as_files_ingested(poll_interval=FAST))

    assert _ids(yielded) == ["only"]
    assert "reports 3 produced file(s) but only 1 could be listed" in caplog.text


def test_as_files_ingested_yields_files_that_failed_to_ingest(mock_clients: MagicMock) -> None:
    """A completed job's failed file is yielded rather than dropped, so the caller can see its status."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED))
    _serve_listing(mock_clients, _page(_file_bean("bad", IN_PROGRESS), _file_bean("good", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {"bad": FAILED, "good": SUCCESS})

    yielded = list(job.as_files_ingested(poll_interval=FAST))

    assert _ids(yielded) == ["bad", "good"]
    assert yielded[0].ingest_status is IngestStatus.FAILED


def test_as_files_ingested_yields_a_file_absent_from_the_batch_response(mock_clients: MagicMock) -> None:
    """A file that disappears mid-wait is treated as done rather than polled forever."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED))
    _serve_listing(mock_clients, _page(_file_bean("deleted", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["deleted"]


def test_as_files_ingested_raises_when_the_job_fails(mock_clients: MagicMock) -> None:
    """A job that ends FAILED raises rather than quietly yielding nothing."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_FAILED)
    _serve_listing(mock_clients, _page())

    with pytest.raises(NominalIngestFailed, match="failed"):
        list(job.as_files_ingested(poll_interval=FAST))


def test_as_files_ingested_names_ingested_files_when_the_job_fails(mock_clients: MagicMock) -> None:
    """The failure names the files that did ingest, so a partial result is still reconcilable."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_FAILED)
    _serve_listing(mock_clients, _page(_file_bean("landed", SUCCESS)))
    _serve_refreshes(mock_clients, {"landed": SUCCESS})

    with pytest.raises(NominalIngestFailed, match="landed"):
        list(job.as_files_ingested(poll_interval=FAST))


def test_as_files_ingested_yields_without_raising_when_the_job_is_cancelled(mock_clients: MagicMock) -> None:
    """A cancelled job yields what did ingest instead of raising, since cancelling was the caller's ask."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_CANCELLED)
    _serve_listing(mock_clients, _page(_file_bean("landed", SUCCESS)))
    _serve_refreshes(mock_clients, {"landed": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["landed"]


@pytest.mark.parametrize("status", ["DELETION_IN_PROGRESS", "DELETED"])
def test_as_files_ingested_stops_on_job_deletion(mock_clients: MagicMock, status: str) -> None:
    """Deletion preserves the job status and stops job polling after yielding listed files."""
    job = IngestionJob._from_proto(mock_clients, _job_proto())
    api_status = ingest_jobs_pb2.IngestJobStatus.Value(f"INGEST_JOB_STATUS_{status}")
    _serve_job(mock_clients, api_status)
    _serve_listing(mock_clients, _page(_file_bean("landed", SUCCESS)))
    _serve_refreshes(mock_clients, {"landed": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST, timeout=timedelta(seconds=1)))) == ["landed"]
    assert job.status.value == status
    assert job.status._to_conjure().name == status
    mock_clients.grpc_ingest_jobs.GetIngestJob.assert_called_once()


def test_as_files_ingested_stops_on_an_unrecognized_job_status(mock_clients: MagicMock) -> None:
    """A job status this client does not recognize ends the wait instead of looping on it forever.

    A newer server may return an enum number absent from these bindings.
    """
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, 999)
    _serve_listing(mock_clients, _page(_file_bean("only", SUCCESS)))
    _serve_refreshes(mock_clients, {"only": SUCCESS})

    assert _ids(list(job.as_files_ingested(poll_interval=FAST))) == ["only"]
    assert job.status is IngestionJobStatus.UNKNOWN


def test_as_files_ingested_raises_timeout_while_the_job_is_still_running(mock_clients: MagicMock) -> None:
    """An exhausted wait budget raises rather than blocking on a job that is still running."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS)
    _serve_listing(mock_clients, _page())

    with pytest.raises(NominalIngestTimeout, match="in_progress"):
        list(job.as_files_ingested(timeout=timedelta(0)))


def test_as_files_ingested_gives_up_on_the_timeout_not_a_full_poll_interval_later(mock_clients: MagicMock) -> None:
    """A timeout shorter than the poll interval ends the wait when the budget runs out, not an interval later."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS)
    _serve_listing(mock_clients, _page())

    started = time.monotonic()
    with pytest.raises(NominalIngestTimeout):
        list(job.as_files_ingested(poll_interval=timedelta(seconds=5), timeout=timedelta(seconds=0.05)))

    assert time.monotonic() - started < 2


def test_as_files_ingested_timeout_names_the_pending_files_when_the_job_is_terminal(
    mock_clients: MagicMock,
) -> None:
    """A timeout on a job that finished blames its still-ingesting files, not the job's own status."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS))
    _serve_job(mock_clients, ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED)
    _serve_listing(mock_clients, _page(_file_bean("slow", IN_PROGRESS)))
    _serve_refreshes(mock_clients, {"slow": IN_PROGRESS})

    with pytest.raises(NominalIngestTimeout, match="completed, but 1 of its file"):
        list(job.as_files_ingested(timeout=timedelta(0)))


@pytest.mark.parametrize("present", [False, True])
def test_proto_conversion_preserves_optional_fields_and_nanoseconds(mock_clients, present) -> None:
    """Absent metadata stays absent while epoch and submicrosecond timestamps stay exact."""
    raw = ingest_jobs_pb2.IngestJob(rid="job", produced_file_count=0)
    if present:
        raw.dataset_rid = ""
        raw.create_time.CopyFrom(Timestamp(seconds=0))
        raw.start_time.CopyFrom(Timestamp(seconds=-1, nanos=999999999))
        raw.end_time.CopyFrom(Timestamp(seconds=1, nanos=123))
        raw.origins.paths.extend(["first", "second"])
    job = IngestionJob._from_proto(mock_clients, raw)
    assert job.dataset_rid == ("" if present else None)
    assert job.created_at == (0 if present else None)
    assert job.start_time == (-1 if present else None)
    assert job.end_time == (1000000123 if present else None)
    assert job.origin_files == (("first", "second") if present else ())
    assert job.created_by_rid is None
    assert job.produced_file_count == 0
    assert job.status is IngestionJobStatus.UNKNOWN
    assert job.ingest_type is IngestType.UNKNOWN


def test_zero_produced_count_skips_running_job_listing(mock_clients) -> None:
    """An authoritative zero count avoids listing while the job is still running."""
    job = IngestionJob._from_proto(mock_clients, _job_proto(produced_file_count=0))
    mock_clients.grpc_ingest_jobs.GetIngestJob.return_value = ingest_jobs_pb2.GetIngestJobResponse(
        ingest_job=_job_proto(produced_file_count=0)
    )
    assert job._poll_for_new_files(set()) == (True, [])
    mock_clients.catalog.get_dataset_files_for_job.assert_not_called()


def test_get_ingestion_job_translates_not_found(mock_clients, fake_rpc_error) -> None:
    """Missing jobs raise the SDK not-found error rather than a raw RPC error."""
    client = NominalClient(_clients=mock_clients)
    mock_clients.grpc_ingest_jobs.GetIngestJob.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)
    with pytest.raises(NominalNotFoundError):
        client.get_ingestion_job("missing")


def test_cancel_failure_preserves_snapshot(mock_clients, fake_rpc_error) -> None:
    """Rejected cancellation raises an SDK error without changing the local job."""
    job = IngestionJob._from_proto(mock_clients, _job_proto())
    error = fake_rpc_error(grpc.StatusCode.FAILED_PRECONDITION)
    mock_clients.grpc_ingest_jobs.CancelIngestJob.side_effect = error
    with pytest.raises(NominalError) as caught:
        job.cancel()
    assert caught.value.__cause__ is error
    assert job.status is IngestionJobStatus.IN_PROGRESS


def test_conjure_search_result_refreshes_in_place_from_grpc(mock_clients) -> None:
    """Search snapshots keep their identity when refreshed from the gRPC job service."""
    raw = ingest_api.IngestJob(
        ingest_job_rid="job",
        status=ingest_api.IngestJobStatus.IN_PROGRESS,
        ingest_type=ingest_api.IngestType.TABULAR,
        created_by="user",
        org_uuid="org",
    )
    job = IngestionJob._from_conjure(mock_clients, raw)
    mock_clients.grpc_ingest_jobs.GetIngestJob.return_value = ingest_jobs_pb2.GetIngestJobResponse(
        ingest_job=ingest_jobs_pb2.IngestJob(rid="job", status=ingest_jobs_pb2.INGEST_JOB_STATUS_COMPLETED)
    )
    assert job.refresh() is job
    assert job.status is IngestionJobStatus.COMPLETED
    assert job.produced_file_count == 0


@pytest.mark.parametrize(
    "name",
    ["SUBMITTED", "QUEUED", "IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED", "DELETION_IN_PROGRESS", "DELETED"],
)
def test_proto_status_matches_public_status(name) -> None:
    """Every known wire status retains its existing public enum value."""
    value = ingest_jobs_pb2.IngestJobStatus.Value(f"INGEST_JOB_STATUS_{name}")
    assert IngestionJobStatus._from_proto(value) is IngestionJobStatus[name]


@pytest.mark.parametrize(
    "name",
    ["TABULAR", "MCAP", "DATAFLASH", "JOURNAL_JSON", "CONTAINERIZED", "VIDEO", "AVRO_STREAM", "POINT_CLOUD", "MULTI"],
)
def test_proto_type_matches_public_type(name) -> None:
    """Every known wire ingest type retains its existing public enum value."""
    value = ingest_jobs_pb2.IngestJobType.Value(f"INGEST_JOB_TYPE_{name}")
    assert IngestType._from_proto(value) is IngestType[name]


def test_unknown_proto_type_is_readable(mock_clients) -> None:
    """A future ingest type does not prevent reading a job."""
    job = IngestionJob._from_proto(mock_clients, ingest_jobs_pb2.IngestJob(type=999))
    assert job.ingest_type is IngestType.UNKNOWN


@pytest.mark.parametrize("truncated", [False, True])
def test_get_ingestion_job_preserves_complete_origins(mock_clients, truncated) -> None:
    """Only truncated gRPC origins trigger a compatibility read of the complete legacy list."""
    raw = _job_proto()
    raw.origins.CopyFrom(ingest_jobs_pb2.IngestJobOrigins(paths=["first"], truncated=truncated))
    mock_clients.grpc_ingest_jobs.GetIngestJob.return_value = ingest_jobs_pb2.GetIngestJobResponse(ingest_job=raw)
    mock_clients.ingest_jobs.get_ingest_job.return_value = ingest_api.IngestJob(
        ingest_job_rid=raw.rid,
        status=ingest_api.IngestJobStatus.COMPLETED,
        ingest_type=ingest_api.IngestType.TABULAR,
        created_by="user",
        org_uuid="org",
        origin_files=["first", "second"],
    )
    job = NominalClient(_clients=mock_clients).get_ingestion_job(raw.rid)
    assert job.status is IngestionJobStatus.IN_PROGRESS
    assert job.origin_files == (("first", "second") if truncated else ("first",))
    if truncated:
        mock_clients.ingest_jobs.get_ingest_job.assert_called_once_with(mock_clients.auth_header, raw.rid)
    else:
        mock_clients.ingest_jobs.get_ingest_job.assert_not_called()


def test_cancel_preserves_complete_origins(mock_clients) -> None:
    """Cancellation hydrates truncated origins while keeping the authoritative cancellation status."""
    job = IngestionJob._from_proto(mock_clients, _job_proto())
    raw = _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_CANCELLED)
    raw.origins.CopyFrom(ingest_jobs_pb2.IngestJobOrigins(paths=["first"], truncated=True))
    mock_clients.grpc_ingest_jobs.CancelIngestJob.return_value = ingest_jobs_pb2.CancelIngestJobResponse(ingest_job=raw)
    mock_clients.ingest_jobs.get_ingest_job.return_value = ingest_api.IngestJob(
        ingest_job_rid=raw.rid,
        status=ingest_api.IngestJobStatus.COMPLETED,
        ingest_type=ingest_api.IngestType.TABULAR,
        created_by="user",
        org_uuid="org",
        origin_files=["first", "second"],
    )
    assert job.cancel() is job
    assert job.status is IngestionJobStatus.CANCELLED
    assert job.origin_files == ("first", "second")


def test_cancel_updates_snapshot_when_origin_lookup_fails(mock_clients) -> None:
    """A failed compatibility read cannot hide a cancellation that already succeeded."""
    job = IngestionJob._from_proto(mock_clients, _job_proto())
    raw = _job_proto(status=ingest_jobs_pb2.INGEST_JOB_STATUS_CANCELLED)
    raw.origins.CopyFrom(ingest_jobs_pb2.IngestJobOrigins(paths=["first"], truncated=True))
    mock_clients.grpc_ingest_jobs.CancelIngestJob.return_value = ingest_jobs_pb2.CancelIngestJobResponse(ingest_job=raw)
    response = requests.Response()
    response.status_code = 503
    error = ConjureHTTPError(requests.HTTPError("Origin lookup unavailable", response=response))
    mock_clients.ingest_jobs.get_ingest_job.side_effect = error

    with pytest.raises(ConjureHTTPError) as raised:
        job.cancel()

    assert raised.value is error
    assert job.status is IngestionJobStatus.CANCELLED
    assert job.origin_files == ("first",)
    mock_clients.grpc_ingest_jobs.CancelIngestJob.assert_called_once()
