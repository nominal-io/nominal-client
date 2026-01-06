from __future__ import annotations

import datetime
import logging
import pathlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Mapping, Protocol, Sequence
from urllib.parse import unquote, urlparse

from nominal_api import api, ingest_api, scout_catalog
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import RefreshableMixin
from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE
from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURLProvider,
)
from nominal.core.bounds import Bounds
from nominal.core.exceptions import NominalIngestError
from nominal.ts import (
    IntegralNanosecondsUTC,
    TypedTimestampType,
    _catalog_timestamp_type_to_typed_timestamp_type,
    _SecondsNanos,
)

logger = logging.getLogger(__name__)


def filename_from_uri(uri: str) -> str:
    return unquote(pathlib.Path(urlparse(uri).path).name).replace(":", "_")


@dataclass(frozen=True)
class DatasetFile(RefreshableMixin[scout_catalog.DatasetFile]):
    id: str
    dataset_rid: str
    name: str
    bounds: Bounds | None
    uploaded_at: IntegralNanosecondsUTC
    ingested_at: IntegralNanosecondsUTC | None
    deleted_at: IntegralNanosecondsUTC | None
    ingest_status: IngestStatus

    timestamp_channel: str | None
    timestamp_type: TypedTimestampType | None
    file_tags: Mapping[str, str] | None
    tag_columns: Mapping[str, str] | None

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def catalog(self) -> scout_catalog.CatalogService: ...
        @property
        def ingest(self) -> ingest_api.IngestService: ...

    def _get_latest_api(self) -> scout_catalog.DatasetFile:
        return self._clients.catalog.get_dataset_file(self._clients.auth_header, self.dataset_rid, self.id)

    def delete(self) -> None:
        """Deletes the dataset file, removing its data permanently from Nominal.

        NOTE: this cannot be undone outside of fully re-ingesting the file into Nominal.
        """
        self._clients.ingest.delete_file(self._clients.auth_header, self.dataset_rid, self.id)

    def poll_until_ingestion_completed(self, interval: datetime.timedelta = datetime.timedelta(seconds=1)) -> Self:
        """Block until dataset file ingestion has completed

        This method polls Nominal for ingest status after uploading a file to a dataset on an interval.

        """
        while True:
            api_file = self._get_latest_api()
            self._refresh_from_api(api_file)
            if self.ingest_status is IngestStatus.SUCCESS:
                break
            elif self.ingest_status is IngestStatus.IN_PROGRESS:
                pass
            elif self.ingest_status is IngestStatus.FAILED:
                # Get error message to display to user
                file_error = api_file.ingest_status.error
                if file_error is None:
                    raise NominalIngestError(
                        f"Ingest status marked as 'error' but with no details for file={self.id!r} and "
                        f"dataset_rid={self.dataset_rid!r}"
                    )
                else:
                    raise NominalIngestError(
                        f"Ingest failed for file={self.id!r} and dataset_rid={self.dataset_rid!r}: "
                        f"{file_error.message} ({file_error.error_type})"
                    )
            else:
                raise NominalIngestError(
                    f"Unknown ingest status {self.ingest_status} for file={self.id!r} and "
                    f"dataset_rid={self.dataset_rid!r}"
                )

            # Sleep for specified interval
            logger.debug("Sleeping for %f seconds before polling for ingest status", interval.total_seconds())
            time.sleep(interval.total_seconds())

        return self

    def _presigned_url_provider(self, ttl_secs: float = 60.0, skew_secs: float = 15.0) -> PresignedURLProvider:
        def fetch() -> str:
            return self._clients.catalog.get_dataset_file_uri(self._clients.auth_header, self.dataset_rid, self.id).uri

        return PresignedURLProvider(fetch_fn=fetch, ttl_secs=ttl_secs, skew_secs=skew_secs)

    def _origin_presigned_url_provider(
        self, origin_path: str, ttl_secs: float = 60.0, skew_secs: float = 15.0
    ) -> PresignedURLProvider:
        def fetch() -> str:
            for uri in self._clients.catalog.get_origin_file_uris(self._clients.auth_header, self.dataset_rid, self.id):
                if uri.path == origin_path:
                    return uri.uri

            raise ValueError(f"No such origin path: {origin_path}")

        return PresignedURLProvider(fetch_fn=fetch, ttl_secs=ttl_secs, skew_secs=skew_secs)

    def download(
        self,
        output_directory: PathLike,
        *,
        part_size: int = DEFAULT_CHUNK_SIZE,
        num_retries: int = 3,
    ) -> pathlib.Path:
        """Download the dataset file to a destination on local disk.

        Args:
            output_directory: Download file to the given directory
            part_size: Size (in bytes) of chunks to use when downloading file.
            num_retries: Number of retries to perform per part download if any exception occurs

        Returns:
            Path that the file was downloaded to

        Raises:
            FileNotFoundError: Output directory doesn't exist
            FileExistsError: File already exists at destination
            RuntimeError: Error downloading file
        """
        output_directory = pathlib.Path(output_directory)
        if output_directory.exists() and not output_directory.is_dir():
            raise NotADirectoryError(f"Output directory is not a directory: {output_directory}")

        logger.info("Getting initial presigned ")
        file_uri = self._clients.catalog.get_dataset_file_uri(self._clients.auth_header, self.dataset_rid, self.id).uri
        destination = output_directory / filename_from_uri(file_uri)
        item = DownloadItem(provider=self._presigned_url_provider(), destination=destination, part_size=part_size)
        with MultipartFileDownloader.create(max_part_retries=num_retries) as dl:
            return dl.download_file(item)

    def download_original_files(
        self,
        output_directory: PathLike,
        *,
        part_size: int = DEFAULT_CHUNK_SIZE,
        num_retries: int = 3,
    ) -> Sequence[pathlib.Path]:
        """Download the input file(s) for a containerized extractor to a destination on local disk.

        Args:
            output_directory: Download file(s) to the given directory
            part_size: Size (in bytes) of chunks to use when downloading files.
            num_retries: Number of retries to perform per part download if any exception occurs

        Returns:
            Path(s) that the file(s) were downloaded to

        Raises:
            NotADirectoryError: Output directory exists, but is not a directory
            FileNotFoundError: Output directory doesn't exist
            FileExistsError: File already exists at destination
            RuntimeError: Failed to determine metadata about files to download

        NOTE: any file that fails to download will result in an error log and will not be returned
              as an output path
        """
        output_directory = pathlib.Path(output_directory)
        if output_directory.exists() and not output_directory.is_dir():
            raise NotADirectoryError(f"Output directory is not a directory: {output_directory}")

        origin_uris = self._clients.catalog.get_origin_file_uris(self._clients.auth_header, self.dataset_rid, self.id)
        items = []
        for uri in origin_uris:
            dest = output_directory / filename_from_uri(uri.uri)
            items.append(
                DownloadItem(
                    provider=self._origin_presigned_url_provider(uri.path),
                    destination=dest,
                    part_size=part_size,
                )
            )

        if not items:
            logger.warning(
                "Dataset file %s (id=%s) has no origin files... was this from a containerized extractor?",
                self.name,
                self.id,
            )
            return []

        with MultipartFileDownloader.create(max_part_retries=num_retries) as dl:
            results = dl.download_files(items)

        for failed_path, ex in results.failed.items():
            logger.error("Failed to download %s", failed_path, exc_info=ex)

        logger.info("Successfully downloaded %d/%d files", len(results.succeeded), len(items))
        return results.succeeded

    def get_file_size(self) -> int | None:
        """Retrieves the size of the file in bytes, or None if it could not be determined

        NOTE: This has only been extensively tested on AWS-based environments, and may fail in some
              self-hosted environments-- a RuntimeError will be thrown in this case.
        """
        # TODO(drake): pull out functionality in a more re-usable way without requiring the downloader class
        with MultipartFileDownloader.create(max_workers=1) as downloader:
            size, _ = downloader._head_or_probe(self._presigned_url_provider())
            return size

    @classmethod
    def _from_conjure(cls, clients: _Clients, dataset_file: scout_catalog.DatasetFile) -> Self:
        upload_time = _SecondsNanos.from_flexible(dataset_file.uploaded_at).to_nanoseconds()
        ingest_time = (
            None
            if dataset_file.ingested_at is None
            else _SecondsNanos.from_flexible(dataset_file.ingested_at).to_nanoseconds()
        )
        delete_time = (
            None
            if dataset_file.deleted_at is None
            else _SecondsNanos.from_flexible(dataset_file.deleted_at).to_nanoseconds()
        )

        file_tags = None
        tag_columns = None
        if dataset_file.ingest_tag_metadata is not None:
            file_tags = dataset_file.ingest_tag_metadata.additional_file_tags
            tag_columns = dataset_file.ingest_tag_metadata.tag_columns

        timestamp_column = None
        timestamp_type = None
        if dataset_file.timestamp_metadata is not None:
            timestamp_column = dataset_file.timestamp_metadata.series_name
            timestamp_type = _catalog_timestamp_type_to_typed_timestamp_type(
                dataset_file.timestamp_metadata.timestamp_type
            )

        return cls(
            id=dataset_file.id,
            dataset_rid=dataset_file.dataset_rid,
            name=dataset_file.name,
            bounds=None if dataset_file.bounds is None else Bounds._from_conjure(dataset_file.bounds),
            uploaded_at=upload_time,
            ingested_at=ingest_time,
            deleted_at=delete_time,
            ingest_status=IngestStatus._from_conjure(dataset_file.ingest_status),
            timestamp_channel=timestamp_column,
            timestamp_type=timestamp_type,
            file_tags=file_tags,
            tag_columns=tag_columns,
            _clients=clients,
        )


# TODO(drake): rename to something more dataset-file specific, expose in nominal.core __init__.py
class IngestStatus(Enum):
    SUCCESS = "SUCCESS"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"

    @classmethod
    def _from_conjure(cls, status: api.IngestStatusV2) -> IngestStatus:
        if status.success is not None:
            return cls.SUCCESS
        elif status.in_progress is not None:
            return cls.IN_PROGRESS
        elif status.error is not None:
            return cls.FAILED
        raise ValueError(f"Unknown ingest status: {status.type}")


class IngestWaitType(Enum):
    FIRST_COMPLETED = "FIRST_COMPLETED"
    FIRST_EXCEPTION = "FIRST_EXCEPTION"
    ALL_COMPLETED = "ALL_COMPLETED"


def wait_for_files_to_ingest(
    files: Sequence[DatasetFile],
    *,
    poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
    timeout: datetime.timedelta | None = None,
    return_when: IngestWaitType = IngestWaitType.ALL_COMPLETED,
) -> tuple[Sequence[DatasetFile], Sequence[DatasetFile]]:
    """Blocks until all of the dataset files have completed their ingestion (or other specified conditions)
    in a similar fashion to `concurrent.futures.wait`.

    Any files that are already ingested (successfully or with errors) will be returned as "done", whereas any
    files still ingesting by the time of this function's exit will be returned as "not done".

    Args:
        files: Dataset files to monitor for ingestion completion.
        poll_interval: Interval to sleep between polling the remaining files under watch.
        timeout: If given, the maximum time to wait before returning
        return_when: Condition for this function to exit. By default, this function will block until all files
            have completed their ingestion (successfully or unsuccessfully), but this can be changed to return
            upon the first completed or first failing ingest. This behavior mirrors that of
            `concurrent.futures.wait`.

    Returns:
        Returns a tuple of (done, not done) dataset files.
    """
    start_time = datetime.datetime.now()
    done: list[DatasetFile] = []
    not_done: list[DatasetFile] = [*files]
    has_failed = False

    while not_done and (timeout is None or datetime.datetime.now() - start_time < timeout):
        logger.info("Polling for ingestion completion for %d files (%d total)", len(not_done), len(files))

        next_not_done = []
        for file in not_done:
            latest_api = file._get_latest_api()
            latest_file = file._refresh_from_api(latest_api)
            match file.ingest_status:
                case IngestStatus.SUCCESS:
                    done.append(latest_file)
                case IngestStatus.FAILED:
                    logger.warning(
                        "Dataset file %s from dataset %s failed to ingest! Error message: %s",
                        latest_file.id,
                        latest_file.dataset_rid,
                        latest_api.ingest_status.error.message if latest_api.ingest_status.error else "",
                    )
                    done.append(latest_file)
                    has_failed = True
                case IngestStatus.IN_PROGRESS:
                    next_not_done.append(latest_file)

        not_done = next_not_done

        if has_failed and return_when is IngestWaitType.FIRST_EXCEPTION:
            break
        elif done and return_when is IngestWaitType.FIRST_COMPLETED:
            break
        elif not not_done:
            break

        if timeout is not None and datetime.datetime.now() - start_time < timeout:
            logger.info(
                "Sleeping for %f seconds while awaiting ingestion for %d files (%d total)... ",
                len(not_done),
                len(files),
                poll_interval.total_seconds(),
            )
            time.sleep(poll_interval.total_seconds())

    return done, not_done


def as_files_ingested(
    files: Sequence[DatasetFile],
    *,
    poll_interval: datetime.timedelta = datetime.timedelta(seconds=1),
) -> Iterable[DatasetFile]:
    """Iterates over DatasetFiles as they complete their ingestion in a similar fashion to
    `concurrent.futures.as_completed`.

    Any files that are already ingested (successfully or with errors) will immediately be yielded.

    Args:
        files: Dataset files to monitor for ingestion completion.
        poll_interval: Interval to sleep between polling the remaining files under watch.

    Yields:
        Yields DatasetFiles as they are ingested. Due to the polling mechanics, the files are not yielded in
        strictly sorted order based on their ingestion completion time. Ensure to check the `ingest_status` of
        yielded dataset files if important.
    """
    to_poll: Sequence[DatasetFile] = [*files]
    while to_poll:
        logger.info("Awaiting ingestion for %d files (%d total)", len(to_poll), len(files))
        done, not_done = wait_for_files_to_ingest(
            to_poll, poll_interval=poll_interval, return_when=IngestWaitType.FIRST_COMPLETED
        )
        for file in done:
            yield file

        to_poll = not_done
        if to_poll:
            time.sleep(poll_interval.total_seconds())
