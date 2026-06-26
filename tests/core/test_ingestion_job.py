from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from nominal_api import ingest_api

from nominal.core.ingestion_job import IngestionJob, IngestionJobStatus


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


def test_from_conjure_unknown_status_falls_back_to_unknown() -> None:
    """An unrecognized wire status name maps to UNKNOWN for forward-compatibility."""
    future = SimpleNamespace(name="SOME_FUTURE_STATUS")
    assert IngestionJobStatus._from_conjure(future) is IngestionJobStatus.UNKNOWN


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
        "nominal.core.ingestion_job.DatasetFile._from_conjure",
        side_effect=lambda _clients, raw_file: sentinels[raw_file],
    ):
        result = job.dataset_files()

    assert result == ["f1", "f2", "f3"]
    assert mock_clients.catalog.get_dataset_files_for_job.call_count == 2
    first_call = mock_clients.catalog.get_dataset_files_for_job.call_args_list[0]
    assert first_call.args == (mock_clients.auth_header, job.rid, None)
    second_call = mock_clients.catalog.get_dataset_files_for_job.call_args_list[1]
    assert second_call.args[2] == "t2"
