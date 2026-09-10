from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import AbstractSet, Collection, Iterable, Protocol, Sequence

from nominal_api import ingest_api
from typing_extensions import Self

from nominal.core._utils.api_tools import HasRid, RefreshableConjureMixin
from nominal.core._utils.frontend_urls import ingestion_job_url
from nominal.core.dataset_file import (
    DatasetFile,
    _dataset_file_from_conjure,
    _deadline_from,
    _poll_files_once,
    _sleep_until_next_poll,
)
from nominal.core.exceptions import NominalIngestFailed, NominalIngestTimeout
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
stops a wait instead of looping forever on it.
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

    def _poll_for_new_files(self, seen_file_ids: AbstractSet[str]) -> tuple[bool, list[DatasetFile]]:
        """Refresh this job once, returning whether it can still produce files and any not yet seen.

        `produced_file_count` is a live count that arrives with the refresh. It says nothing about how
        many files to expect, but an unchanged one does mean a re-listing cannot turn up anything new,
        which is worth skipping for a job holding thousands of files. A job that has stopped running is
        always listed once more, so the final file set never depends on that count.

        Keyed by file id rather than filtered into a list: the listing pages by offset over a column
        that is not unique, so a row written while a pass is in flight shifts later pages and can serve
        the same file twice within that one pass.
        """
        if self.status in _RUNNING_JOB_STATUSES:
            self.refresh()
        job_running = self.status in _RUNNING_JOB_STATUSES

        if job_running and self.produced_file_count == len(seen_file_ids):
            return True, []
        new_files = {file.id: file for file in self._iter_dataset_files() if file.id not in seen_file_ids}
        return job_running, list(new_files.values())

    def _timeout_error(self, pending: Sequence[DatasetFile]) -> NominalIngestTimeout:
        """Describe which of the two stages this wait was still blocked on when its budget expired."""
        blocked_on = (
            f"was still {self.status.name.lower()}"
            if self.status in _RUNNING_JOB_STATUSES
            else f"{self.status.name.lower()}, but {len(pending)} of its file(s) were still ingesting"
        )
        return NominalIngestTimeout(f"ingest job {self.rid} {blocked_on} when its wait budget expired")

    def _report_terminal_state(self, seen_file_ids: Collection[str]) -> None:
        """Raise or warn about how this job finished, once every file it produced has been waited out.

        `produced_file_count` is the only cross-check available on whether the paged listing returned
        everything: it counts the same rows the listing selects, before that listing drops unlanded
        files and ones in datasets this caller cannot read. Neither is a race a longer wait would win.
        A file is unlanded only while its row is still a reservation, and a reservation is non-terminal
        work that holds the job itself off COMPLETED — so one still unlanded here is one that failed
        before it landed. A shortfall is legitimate for some jobs, so it is reported rather than raised.
        """
        if self.produced_file_count is not None and len(seen_file_ids) < self.produced_file_count:
            logger.warning(
                "Ingest job %s reports %d produced file(s) but only %d could be listed. Unlanded files "
                "and files in datasets you cannot read are counted but not listed; otherwise the paged "
                "listing dropped rows.",
                self.rid,
                self.produced_file_count,
                len(seen_file_ids),
            )

        if self.status is IngestionJobStatus.FAILED:
            raise NominalIngestFailed(
                f"ingest job {self.rid} failed after producing {len(seen_file_ids)} file(s): {sorted(seen_file_ids)}"
            )
        elif self.status is not IngestionJobStatus.COMPLETED:
            logger.warning(
                "Ingest job %s finished %s rather than COMPLETED, after producing %d file(s).",
                self.rid,
                self.status.name,
                len(seen_file_ids),
            )

    def as_files_ingested(
        self,
        *,
        poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
        timeout: datetime.timedelta | None = None,
    ) -> Iterable[DatasetFile]:
        """Yield this job's dataset files as each completes ingestion.

        Waits out both stages of an ingest: the job producing its files, then those files finishing
        ingestion. The file list is re-read on each poll while the job is still running, so files that
        do not exist yet when this is called are still picked up — a containerized extractor registers
        its outputs only once its container has exited. `list(job.as_files_ingested())` blocks until
        the job is terminal and every file it produced has finished ingesting.

        Yielded files may have failed: a job can complete with some of its files FAILED, so check
        `ingest_status` on each yielded file if that matters. A job that was cancelled yields whatever
        did ingest and logs a warning, since a cancelled job is the caller getting what they asked for.

        Unlike the free function `nominal.core.as_files_ingested`, which only ever reports per-file
        status, this raises when the job itself fails — a failed job can have no files to report the
        failure through. Iterating this rather than wrapping it in `list()` keeps the files yielded
        before that point. For timeout / return-when control over a fixed set of files, use
        `nominal.core.wait_for_files_to_ingest(job.dataset_files(), ...)`.

        Args:
            poll_interval: Interval to sleep between polls of this job and of its pending files.
            timeout: Give up after this long and raise `NominalIngestTimeout`; None waits
                indefinitely. Never sleeps past the deadline.

        Yields:
            Dataset files as they finish ingesting. Due to the polling mechanics, the files are not
            yielded in strictly sorted order based on their ingestion completion time.

        Raises:
            NominalIngestFailed: The job itself finished FAILED. Files that did ingest are named in
                the error, and were yielded before it was raised.
            NominalIngestTimeout: `timeout` elapsed while the job or one of its files was still in
                progress.
        """
        deadline = _deadline_from(timeout)
        seen_file_ids: set[str] = set()
        pending: list[DatasetFile] = []
        # Not `self.status in _RUNNING_JOB_STATUSES`: a job that is already terminal here still owes
        # one listing pass, which is the whole file set for a job that finished before this was called.
        may_produce_more = True

        while True:
            if may_produce_more:
                may_produce_more, new_files = self._poll_for_new_files(seen_file_ids)
                seen_file_ids.update(file.id for file in new_files)
                pending.extend(new_files)

            if pending:
                newly_done, pending, _ = _poll_files_once(pending)
                yield from newly_done

            if not may_produce_more and not pending:
                break

            logger.info(
                "Awaiting ingest job %s (status %s, %d file(s) ingesting)...",
                self.rid,
                self.status.name,
                len(pending),
            )
            if not _sleep_until_next_poll(poll_interval, deadline):
                raise self._timeout_error(pending)

        self._report_terminal_state(seen_file_ids)
