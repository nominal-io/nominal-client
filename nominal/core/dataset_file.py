from __future__ import annotations

import concurrent.futures
import logging
import pathlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Protocol, Sequence

from cachetools.func import ttl_cache
from nominal_api import api, scout_catalog
from typing_extensions import Self

from nominal._utils.download_tools import download_presigned_uri, filename_from_uri
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

    def download(
        self,
        output_directory: pathlib.Path,
        force: bool = False,
    ) -> pathlib.Path:
        """Download the dataset file to a destination on local disk.

        Args:
            output_directory: Download file to the given directory
            force: If true, delete any files that exist / create parent directories if nonexistent

        Returns:
            Path that the file was downloaded to

        Raises:
            FileNotFoundError: Output directory doesn't exist and force=False
            FileExistsError: File already exists at destination
            NotADirectoryError: Output directory exists and is not a directory
            RuntimeError: Error downloading file
        """
        if output_directory.exists() and not output_directory.is_dir():
            raise NotADirectoryError(f"Output directory is not a directory: {output_directory}")

        api_uri = self._clients.catalog.get_dataset_file_uri(self._clients.auth_header, self.dataset_rid, self.id)
        destination = output_directory / filename_from_uri(api_uri.uri)

        logger.info("Downloading %s (%s) => %s", self.name, api_uri.uri, destination)
        download_presigned_uri(api_uri.uri, destination, force=force)
        return destination

    @ttl_cache(ttl=30.0)
    def _presigned_origin_files(self) -> Mapping[str, str]:
        """Returns a mapping of s3 paths to presigned s3 uris for all origin files"""
        return {
            uri.path: uri.uri
            for uri in self._clients.catalog.get_origin_file_uris(self._clients.auth_header, self.dataset_rid, self.id)
        }

    def _download_origin_file(self, origin_path: str, destination: pathlib.Path, force: bool, num_retries: int) -> None:
        """Download the origin file with the given origin path to the given destination.

        Args:
            origin_path: Path to the file to download
            destination: Path to the location to download the file to
            force: If true, create any parent directories and delete any existing files in the destination
            num_retries: Number of retries to use when downloading the file in case of transient networking errors.

        Raises:
            RuntimeError: Unable to download the origin file
        """
        last_exception = None
        for attempt in range(num_retries):
            origin_uri = self._presigned_origin_files().get(origin_path)
            if origin_uri is None:
                raise RuntimeError(f"No such origin path: {origin_path}")

            logger.info("Downloading %s => %s (%d / %d)", origin_path, destination, attempt + 1, num_retries)
            try:
                download_presigned_uri(origin_uri, destination, force=force)

                # Success-- return
                return
            except Exception as ex:
                last_exception = ex
                logger.error(
                    "Failed to download %s => %s (%d / %d)",
                    origin_path,
                    destination,
                    attempt + 1,
                    num_retries,
                    exc_info=ex,
                )

            # Delete any partially downloaded response
            destination.unlink(missing_ok=True)

            # Sleep to allow backend to catch up
            time.sleep(3)

        # All attempts failed, raise exception
        raise RuntimeError(
            f"Failed to download {origin_path} => {destination} in {num_retries} tries"
        ) from last_exception

    def download_original_files(
        self, output_directory: pathlib.Path, force: bool = True, parallel_downloads: int = 8, num_retries: int = 3
    ) -> Sequence[pathlib.Path]:
        """Download the input file(s) for a containerized extractor to a destination on local disk.

        Args:
            output_directory: Download file(s) to the given directory
            force: If true, delete any files that exist / create parent directories if nonexistent
            parallel_downloads: Number of files to download concurrently
            num_retries: Number of retries to perform per file download if any exception occurs

        Returns:
            Path(s) that the file(s) were downloaded to

        Raises:
            NotADirectoryError: Output directory is not a directory

        NOTE: any file that fails to download will result in an error log and will not be returned
        """
        if output_directory.exists() and not output_directory.is_dir():
            raise NotADirectoryError(f"Output directory is not a directory: {output_directory}")

        origin_uris = self._clients.catalog.get_origin_file_uris(self._clients.auth_header, self.dataset_rid, self.id)
        if not origin_uris:
            logger.warning(
                "Dataset file %s (id=%s) has no origin files... was this from a containerized extractor?",
                self.name,
                self.id,
            )
            return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_downloads) as pool:
            futures = {}
            for uri in origin_uris:
                destination = output_directory / filename_from_uri(uri.uri)

                future = pool.submit(
                    self._download_origin_file,
                    uri.path,
                    destination,
                    force,
                    num_retries,
                )
                futures[future] = (uri, destination)

            results = []
            for idx, future in enumerate(concurrent.futures.as_completed(futures)):
                uri, destination = futures[future]
                ex = future.exception()
                if ex is not None:
                    logger.error("Failed to download %s => %s", uri.path, destination, exc_info=ex)
                    continue

                logger.info("Successfully downloaded %s => %s (%d / %d)", uri.path, destination, idx + 1, len(futures))
                results.append(destination)
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
