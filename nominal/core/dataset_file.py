from __future__ import annotations

import concurrent.futures
import logging
import pathlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol, Sequence

from nominal_api import api, scout_catalog
from typing_extensions import Self

from nominal._utils.download_tools import download_presigned_uri
from nominal.core._clientsbunch import HasScoutParams
from nominal.core.bounds import Bounds
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetFile:
    id: str
    dataset_rid: str
    name: str
    bounds: Bounds | None
    uploaded_at: IntegralNanosecondsUTC
    ingested_at: IntegralNanosecondsUTC | None
    ingest_status: IngestStatus

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def catalog(self) -> scout_catalog.CatalogService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, dataset_file: scout_catalog.DatasetFile) -> Self:
        upload_time = _SecondsNanos.from_flexible(dataset_file.uploaded_at).to_nanoseconds()
        ingest_time = (
            None
            if dataset_file.ingested_at is None
            else _SecondsNanos.from_flexible(dataset_file.ingested_at).to_nanoseconds()
        )
        return cls(
            id=dataset_file.id,
            dataset_rid=dataset_file.dataset_rid,
            name=dataset_file.name,
            bounds=None if dataset_file.bounds is None else Bounds._from_conjure(dataset_file.bounds),
            uploaded_at=upload_time,
            ingested_at=ingest_time,
            ingest_status=IngestStatus._from_conjure(dataset_file.ingest_status),
            _clients=clients,
        )

    def download(self, destination: pathlib.Path) -> pathlib.Path:
        """Download the dataset file to a destination on local disk.

        Args:
            destination: If an existing directory, downloads the file to the given directory.
                Otherwise, downloads the file to the given path.

        Returns:
            Path that the file was downloaded to

        Raises:
            FileExistsError: File already exists at destination
            RuntimeError: Error downloading file
        """
        uri = self._clients.catalog.get_dataset_file_uri(self._clients.auth_header, self.dataset_rid, self.id).uri
        logger.info("Downloading %s (%s) => %s", self.name, uri, destination)
        return download_presigned_uri(uri, destination)

    def download_origin_files(self, destination: pathlib.Path) -> Sequence[pathlib.Path]:
        """Download the origin files for a given dataset file to a destination on local disk.

        Args:
            destination: Destination to download file(s) to.
                NOTE: If multiple files are requested for export, must be a directory.
                NOTE: If a single file is requested for export, mest be an existing directory or the path
                    to write the file as

        Returns:
            List of downloaded file locations.

        Raises:
            FileExistsError: File already exists at the destination
        """
        origin_uris = self._clients.catalog.get_origin_file_uris(self._clients.auth_header, self.dataset_rid, self.id)
        if not origin_uris:
            return []

        if destination.exists():
            if len(origin_uris) == 1 and destination.is_file():
                raise FileExistsError(f"Cannot download origin file to {destination}: already exists!")
            elif destination.is_file():
                raise FileExistsError(f"Cannot download origin files to {destination}: already exists as a file!")
        elif len(origin_uris) == 1:
            destination.parent.mkdir(parents=True, exist_ok=True)
        else:
            destination.mkdir(parents=True, exist_ok=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(origin_uris)) as pool:
            futures = {pool.submit(download_presigned_uri, uri.uri, destination): uri for uri in origin_uris}
            results = []
            for future in concurrent.futures.as_completed(futures):
                uri = futures[future]
                ex = future.exception()
                if ex is not None:
                    logger.error("Failed to download %s => %s", uri.path, destination, exc_info=ex)
                    continue

                results.append(future.result())
            return results


class IngestStatus(Enum):
    SUCCESS = "SUCCESS"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"

    @classmethod
    def _from_conjure(cls, status: api.IngestStatusV2) -> IngestStatus:
        if status.type == "success":
            return cls.SUCCESS
        elif status.type == "in_progress":
            return cls.IN_PROGRESS
        elif status.type == "failed":
            return cls.FAILED
        raise ValueError(f"Unknown ingest status: {status.type}")
