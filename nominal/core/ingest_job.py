from __future__ import annotations

import datetime
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Iterable, Protocol, Sequence

from nominal_api import ingest_api, ingest_workflow_api, scout_catalog
from typing_extensions import Self

from nominal._utils import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.pagination_tools import paginate_rpc
from nominal.core.dataset_file import DatasetFile
from nominal.core.exceptions import NominalIngestError

logger = logging.getLogger(__name__)


class IngestJobStatus(enum.Enum):
    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @classmethod
    def _from_conjure(cls, status: ingest_api.IngestJobStatus) -> IngestJobStatus:
        rev_map = {value.value: value for value in cls.__members__.values()}
        mapped_status = rev_map.get(status.value)
        if mapped_status is None:
            raise ValueError(f"Unknown ingest job status: {status}")
        else:
            return mapped_status


class IngestType(enum.Enum):
    TABULAR = "TABULAR"
    MCAP = "MCAP"
    DATAFLASH = "DATAFLASH"
    JOURNAL_JSON = "JOURNAL_JSON"
    CONTAINERIZED = "CONTAINERIZED"
    VIDEO = "VIDEO"
    AVRO_STREAM = "AVRO_STREAM"

    @classmethod
    def _from_conjure(cls, ingest_type: ingest_api.IngestType) -> IngestType:
        rev_map = {value.value: value for value in cls.__members__.values()}
        mapped_ingest_type = rev_map.get(ingest_type.value)
        if mapped_ingest_type is None:
            raise ValueError(f"Unknown ingest type: {ingest_type}")
        else:
            return mapped_ingest_type


@dataclass(frozen=True)
class IngestJob:
    rid: str
    ingest_status: IngestJobStatus
    ingest_type: IngestType

    _origin_files: Sequence[str] = field(repr=False)
    _ingest_request: ingest_api.IngestJobRequest = field(repr=False)
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def catalog(self) -> scout_catalog.CatalogService: ...
        @property
        def ingest(self) -> ingest_api.IngestService: ...
        @property
        def internal_ingest(self) -> ingest_workflow_api.IngestInternalService: ...

    def _get_latest_api(self) -> ingest_api.IngestJob:
        return self._clients.internal_ingest.get_ingest_job(
            auth_header=self._clients.auth_header,
            ingest_job_rid=self.rid,
        )

    def _refresh_from_api(self, ingest_job: ingest_api.IngestJob) -> Self:
        updated_job = self.__class__._from_conjure(self._clients, ingest_job)
        update_dataclass(self, updated_job, self.__dataclass_fields__)
        return self

    def refresh(self) -> Self:
        return self._refresh_from_api(self._get_latest_api())

    def _iter_dataset_files(self) -> Iterable[DatasetFile]:
        def rpc_wrapper(auth_header: str, page_token: str | None) -> scout_catalog.DatasetFilesPage:
            return self._clients.catalog.get_dataset_files_for_job(auth_header, self.rid, page_token)

        def request_factory(page_token: str | None) -> str | None:
            return page_token

        def token_factory(file_page: scout_catalog.DatasetFilesPage) -> str | None:
            return file_page.next_page

        for dataset_file_page in paginate_rpc(
            rpc=rpc_wrapper,
            auth_header=self._clients.auth_header,
            request_factory=request_factory,
            token_factory=token_factory,
        ):
            for raw_dataset_file in dataset_file_page.files:
                yield DatasetFile._from_conjure(self._clients, raw_dataset_file)

    def list_dataset_files(self) -> Sequence[DatasetFile]:
        return list(self._iter_dataset_files())

    def rerun(self) -> Self:
        raw_resp = self._clients.ingest.rerun_ingest(self._clients.auth_header, ingest_api.RerunIngestRequest(self.rid))
        if raw_resp.ingest_job_rid is None:
            raise ValueError("Expected ingest response to have an ingest job associated!")

        raw_ingest_job = self._clients.internal_ingest.get_ingest_job(
            self._clients.auth_header, raw_resp.ingest_job_rid
        )
        return self.__class__._from_conjure(self._clients, raw_ingest_job)

    def poll_until_ingestion_completed(self, interval: datetime.timedelta = datetime.timedelta(seconds=1)) -> Self:
        while True:
            self.refresh()
            match self.ingest_status:
                case IngestJobStatus.COMPLETED:
                    break
                case IngestJobStatus.QUEUED | IngestJobStatus.IN_PROGRESS | IngestJobStatus.SUBMITTED:
                    continue
                case IngestJobStatus.CANCELLED:
                    raise NominalIngestError(f"Ingest job {self.rid} was cancelled!")
                case IngestJobStatus.FAILED:
                    raise NominalIngestError(f"Ingest job {self.rid} failed!")
                case _:
                    raise NominalIngestError(f"Unknown ingest status {self.ingest_status} for ingest job {self.rid}")

            logger.debug("Sleeping for %f seconds before polling for ingest status", interval.total_seconds())
            time.sleep(interval.total_seconds())

        return self

    @classmethod
    def _from_conjure(cls, clients: _Clients, ingest_job: ingest_api.IngestJob) -> Self:
        return cls(
            rid=ingest_job.ingest_job_rid,
            ingest_status=IngestJobStatus._from_conjure(ingest_job.status),
            ingest_type=IngestType._from_conjure(ingest_job.ingest_type),
            _origin_files=ingest_job.origin_files or [],
            _ingest_request=ingest_job.ingest_job_request,
            _clients=clients,
        )
