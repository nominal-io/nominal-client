from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Protocol, Sequence

from nominal_api import ingest_api
from typing_extensions import Self

from nominal.core._utils.api_tools import HasRid, RefreshableConjureMixin
from nominal.core._utils.frontend_urls import ingestion_job_url
from nominal.core.dataset_file import DatasetFile, _dataset_file_from_conjure, _poll_files_once
from nominal.core.exceptions import NominalIngestError
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


class IngestionJobStatus(Enum):
    """Lifecycle status of an ingest job."""

    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"
    """Unknown or unrecognized status returned by a newer server."""

    @classmethod
    def _from_conjure(cls, status: ingest_api.IngestJobStatus) -> IngestionJobStatus:
        match status.name:
            case "SUBMITTED":
                result = cls.SUBMITTED
            case "QUEUED":
                result = cls.QUEUED
            case "IN_PROGRESS":
                result = cls.IN_PROGRESS
            case "COMPLETED":
                result = cls.COMPLETED
            case "FAILED":
                result = cls.FAILED
            case "CANCELLED":
                result = cls.CANCELLED
            case _:
                result = cls.UNKNOWN
        return result

    def _to_conjure(self) -> ingest_api.IngestJobStatus:
        match self.name:
            case "SUBMITTED":
                result = ingest_api.IngestJobStatus.SUBMITTED
            case "QUEUED":
                result = ingest_api.IngestJobStatus.QUEUED
            case "IN_PROGRESS":
                result = ingest_api.IngestJobStatus.IN_PROGRESS
            case "COMPLETED":
                result = ingest_api.IngestJobStatus.COMPLETED
            case "FAILED":
                result = ingest_api.IngestJobStatus.FAILED
            case "CANCELLED":
                result = ingest_api.IngestJobStatus.CANCELLED
            case _:
                result = ingest_api.IngestJobStatus.UNKNOWN
        return result


_RUNNING_JOB_STATUSES = frozenset(
    {IngestionJobStatus.SUBMITTED, IngestionJobStatus.QUEUED, IngestionJobStatus.IN_PROGRESS}
)
"""Statuses from which a job can still produce more files.

Deliberately the running set rather than the terminal set: a status this client does not recognize
stops the wait instead of looping forever on it.
"""


class IngestType(Enum):
    """The kind of data an ingest job ingests."""

    TABULAR = "TABULAR"
    MCAP = "MCAP"
    DATAFLASH = "DATAFLASH"
    JOURNAL_JSON = "JOURNAL_JSON"
    CONTAINERIZED = "CONTAINERIZED"
    VIDEO = "VIDEO"
    AVRO_STREAM = "AVRO_STREAM"
    POINT_CLOUD = "POINT_CLOUD"
    MULTI = "MULTI"
    UNKNOWN = "UNKNOWN"
    """Unknown or unrecognized ingest type returned by a newer server."""

    @classmethod
    def _from_conjure(cls, ingest_type: ingest_api.IngestType) -> IngestType:
        match ingest_type.name:
            case "TABULAR":
                result = cls.TABULAR
            case "MCAP":
                result = cls.MCAP
            case "DATAFLASH":
                result = cls.DATAFLASH
            case "JOURNAL_JSON":
                result = cls.JOURNAL_JSON
            case "CONTAINERIZED":
                result = cls.CONTAINERIZED
            case "VIDEO":
                result = cls.VIDEO
            case "AVRO_STREAM":
                result = cls.AVRO_STREAM
            case "POINT_CLOUD":
                result = cls.POINT_CLOUD
            case "MULTI":
                result = cls.MULTI
            case _:
                result = cls.UNKNOWN
        return result


def _optional_iso_to_nanos(value: str | None) -> IntegralNanosecondsUTC | None:
    if value is None:
        return None
    return _SecondsNanos.from_flexible(value).to_nanoseconds()


@dataclass(frozen=True)
class IngestionJob(HasRid, RefreshableConjureMixin[ingest_api.IngestJob]):
    """A trackable record of one ingestion request moving through the async pipeline."""

    rid: str
    status: IngestionJobStatus
    ingest_type: IngestType
    dataset_rid: str | None
    origin_files: Sequence[str]
    produced_file_count: int | None
    created_by_rid: str | None
    created_at: IntegralNanosecondsUTC | None
    start_time: IntegralNanosecondsUTC | None
    end_time: IntegralNanosecondsUTC | None
    _clients: _Clients = field(repr=False)

    class _Clients(DatasetFile._Clients, Protocol):
        @property
        def ingest_jobs(self) -> ingest_api.IngestJobService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this ingest job in the Nominal app."""
        return ingestion_job_url(self._clients, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, job: ingest_api.IngestJob) -> Self:
        return cls(
            rid=job.ingest_job_rid,
            status=IngestionJobStatus._from_conjure(job.status),
            ingest_type=IngestType._from_conjure(job.ingest_type),
            dataset_rid=job.dataset_rid,
            origin_files=tuple(job.origin_files or ()),
            produced_file_count=job.produced_file_count,
            created_by_rid=job.created_by_rid,
            created_at=_optional_iso_to_nanos(job.created_at),
            start_time=_optional_iso_to_nanos(job.start_time),
            end_time=_optional_iso_to_nanos(job.end_time),
            _clients=clients,
        )

    def _get_latest_api(self) -> ingest_api.IngestJob:
        return self._clients.ingest_jobs.get_ingest_job(self._clients.auth_header, self.rid)

    def cancel(self) -> Self:
        """Cancel this ingest job.

        SUBMITTED/QUEUED jobs transition directly to CANCELLED; IN_PROGRESS jobs have their
        underlying workflow cancelled. Cancelling a job already in a terminal state raises a
        conjure IngestJobNotCancellable (CONFLICT) error from the server.
        """
        job = self._clients.ingest_jobs.cancel_ingest_job(self._clients.auth_header, self.rid)
        return self._refresh_from_api(job)

    def _iter_dataset_files(self) -> Iterable[DatasetFile]:
        next_page_token = None
        while True:
            page = self._clients.catalog.get_dataset_files_for_job(self._clients.auth_header, self.rid, next_page_token)
            for dataset_file in page.files:
                yield _dataset_file_from_conjure(self._clients, dataset_file)
            if page.next_page is None:
                break
            next_page_token = page.next_page

    def dataset_files(self) -> Sequence[DatasetFile]:
        """Return the dataset files this ingest job has produced so far.

        This is a point-in-time snapshot: a job that is not yet terminal can still produce more files,
        and one that produces its files in bulk has none to report until its work is done. To wait for
        the complete set, use `as_files_ingested()`.
        """
        return list(self._iter_dataset_files())

    def as_files_ingested(
        self,
        *,
        poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
        timeout: datetime.timedelta | None = None,
    ) -> Iterable[DatasetFile]:
        """Yield this job's dataset files as each completes ingestion.

        Waits out both stages of an ingest: the job producing its files, then those files finishing
        ingestion. The file list is re-read on each poll for as long as the job is still running, so
        files that do not exist yet when this is called are still picked up — a job that emits many
        files registers none of them until its work is done, and a wait that read the list only once
        would report success over zero files. `list(job.as_files_ingested())` therefore blocks until
        the job is terminal and every file it produced has finished ingesting.

        Yielded files may have failed: a job can complete with some of its files FAILED, so check
        `ingest_status` on each yielded file if that matters. For timeout / return-when control over
        a fixed set of files, use `nominal.core.wait_for_files_to_ingest(job.dataset_files(), ...)`.

        Args:
            poll_interval: Interval to sleep between polls of this job and of its pending files.
            timeout: If given, the maximum time to wait before raising `TimeoutError`.

        Yields:
            Dataset files as they finish ingesting. Due to the polling mechanics, the files are not
            yielded in strictly sorted order based on their ingestion completion time.

        Raises:
            NominalIngestError: The job itself finished FAILED or CANCELLED.
            TimeoutError: `timeout` elapsed while the job or one of its files was still in progress.
        """
        start_time = datetime.datetime.now()
        seen_file_ids: set[str] = set()
        pending: list[DatasetFile] = []
        job_running = True

        while True:
            if job_running:
                if self.status in _RUNNING_JOB_STATUSES:
                    self.refresh()
                # A terminal job's file list is complete, so read it once more and then stop looking.
                job_running = self.status in _RUNNING_JOB_STATUSES
                # produced_file_count is a live count that comes free with the refresh above. It says
                # nothing about how many files to expect, but an unchanged one does mean a re-listing
                # cannot turn up anything new — worth skipping for a job holding thousands of files.
                # The pass that observes a terminal status always lists, whatever the count says.
                if not job_running or self.produced_file_count != len(seen_file_ids):
                    for file in self._iter_dataset_files():
                        if file.id not in seen_file_ids:
                            seen_file_ids.add(file.id)
                            pending.append(file)

            if pending:
                newly_done, pending, _ = _poll_files_once(pending)
                yield from newly_done

            if not job_running and not pending:
                break

            if timeout is not None and datetime.datetime.now() - start_time >= timeout:
                raise TimeoutError(
                    f"timed out after {timeout} waiting for ingest job {self.rid}: "
                    f"job status {self.status.name}, {len(pending)} file(s) still ingesting"
                )

            logger.info(
                "Sleeping for %f seconds while awaiting ingest job %s (status %s, %d file(s) ingesting)...",
                poll_interval.total_seconds(),
                self.rid,
                self.status.name,
                len(pending),
            )
            time.sleep(poll_interval.total_seconds())

        if self.status in (IngestionJobStatus.FAILED, IngestionJobStatus.CANCELLED):
            raise NominalIngestError(
                f"ingest job {self.rid} finished {self.status.name} after producing {len(seen_file_ids)} file(s)"
            )
        elif self.status is not IngestionJobStatus.COMPLETED:
            logger.warning(
                "Ingest job %s had unrecognized status %s; treating as terminal after %d file(s).",
                self.rid,
                self.status.name,
                len(seen_file_ids),
            )
