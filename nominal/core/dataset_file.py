from __future__ import annotations

import datetime
import logging
import pathlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Mapping, Protocol, Self, Sequence
from urllib.parse import unquote, urlparse

from nominal_api import ingest_api, scout_video

from nominal._utils.iterator_tools import batched
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import RefreshableGrpcMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE
from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURLProvider,
)
from nominal.core._utils.pagination_tools import paginate_grpc
from nominal.core.bounds import Bounds
from nominal.exceptions import NominalIngestError
from nominal.protos.datasets.v1 import dataset_files_pb2, dataset_files_pb2_grpc
from nominal.ts import (
    IntegralNanosecondsUTC,
    TypedTimestampType,
    _proto_timestamp_type_to_typed_timestamp_type,
)

logger = logging.getLogger(__name__)


def filename_from_uri(uri: str) -> str:
    return unquote(pathlib.Path(urlparse(uri).path).name).replace(":", "_")


@dataclass(frozen=True)
class DatasetFile(RefreshableGrpcMixin[dataset_files_pb2.DatasetFile]):
    """An uploaded dataset file and its ingestion status and time bounds."""

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
    _ingest_error_message: str | None = field(repr=False, default=None)

    class _Clients(HasScoutParams, Protocol):
        @property
        def grpc_dataset_files(self) -> dataset_files_pb2_grpc.DatasetFileServiceStub: ...
        @property
        def ingest(self) -> ingest_api.IngestService: ...
        # Used by the VideoDatasetFile subtype for video-channel timing updates.
        @property
        def video(self) -> scout_video.VideoService: ...

    def _get_latest_api(self) -> dataset_files_pb2.DatasetFile:
        return _get_dataset_file(self._clients, self.dataset_rid, self.id)

    def delete(self) -> None:
        """Deletes the dataset file, removing its data permanently from Nominal.

        Note:
            This cannot be undone outside of fully re-ingesting the file into Nominal.
        """
        self._clients.ingest.delete_file(self._clients.auth_header, self.dataset_rid, self.id)

    def get_ingest_error(self) -> NominalIngestError | None:
        """Returns the ingest exception if the file FAILED, or None if there is no error.

        Refreshes the file from the API before checking.
        """
        self.refresh()
        if self._ingest_error_message is None:
            return None
        else:
            return NominalIngestError(
                f"Ingest failed for file '{self.name}' with id '{self.id!r}' on dataset '{self.dataset_rid!r}': "
                f"{self._ingest_error_message}"
            )

    def poll_until_ingestion_completed(self, interval: datetime.timedelta = datetime.timedelta(seconds=1)) -> Self:
        """Block until dataset file ingestion has completed

        This method polls Nominal for ingest status after uploading a file to a dataset on an interval.

        """
        while True:
            api_file = self._get_latest_api()
            self._refresh_from_api(api_file)

            match self.ingest_status:
                case IngestStatus.SUCCESS | IngestStatus.DELETION_IN_PROGRESS | IngestStatus.DELETED:
                    # ingestion has completed successfully by this point
                    break
                case IngestStatus.FAILED:
                    raise NominalIngestError(
                        f"Ingest failed for file '{self.name}' with id '{self.id!r}' on dataset "
                        f"'{self.dataset_rid!r}': {self._ingest_error_message or 'no error details available'}"
                    )
                case IngestStatus.IN_PROGRESS | IngestStatus.QUEUED | IngestStatus.PARSING | IngestStatus.INGESTING:
                    # Ingestion still proceeding
                    pass
                case _:
                    logger.warning(
                        "Dataset file %s from dataset %s had unknown ingest status %s; treating as done.",
                        self.id,
                        self.dataset_rid,
                        self.ingest_status,
                    )
                    break

            # Sleep for specified interval
            logger.debug("Sleeping for %f seconds before polling for ingest status", interval.total_seconds())
            time.sleep(interval.total_seconds())

        return self

    def _get_download_uri(self) -> str:
        with translate_grpc_errors():
            return self._clients.grpc_dataset_files.GetDatasetFileDownloadUri(
                dataset_files_pb2.GetDatasetFileDownloadUriRequest(dataset_rid=self.dataset_rid, file_id=self.id)
            ).uri

    def _iter_origin_uris(self) -> Iterable[dataset_files_pb2.OriginFileUri]:
        def request(page_token: str | None) -> dataset_files_pb2.ListDatasetFileOriginUrisRequest:
            return dataset_files_pb2.ListDatasetFileOriginUrisRequest(
                dataset_rid=self.dataset_rid, file_id=self.id, page_token=page_token or ""
            )

        for page in paginate_grpc(self._clients.grpc_dataset_files.ListDatasetFileOriginUris, request_factory=request):
            yield from page.origin_file_uris

    def _presigned_url_provider(self, ttl_secs: float = 60.0, skew_secs: float = 15.0) -> PresignedURLProvider:
        return PresignedURLProvider(fetch_fn=self._get_download_uri, ttl_secs=ttl_secs, skew_secs=skew_secs)

    def _origin_presigned_url_provider(
        self, origin_path: str, ttl_secs: float = 60.0, skew_secs: float = 15.0
    ) -> PresignedURLProvider:
        def fetch() -> str:
            for uri in self._iter_origin_uris():
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
        file_uri = self._get_download_uri()
        destination = output_directory / filename_from_uri(file_uri)
        item = DownloadItem(provider=self._presigned_url_provider(), destination=destination, part_size=part_size)
        with MultipartFileDownloader.create(
            max_part_retries=num_retries, header_provider=self._clients.header_provider
        ) as dl:
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

        Note:
            Any file that fails to download will result in an error log and will not be returned
            as an output path
        """
        output_directory = pathlib.Path(output_directory)
        if output_directory.exists() and not output_directory.is_dir():
            raise NotADirectoryError(f"Output directory is not a directory: {output_directory}")

        origin_uris = self._iter_origin_uris()
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

        with MultipartFileDownloader.create(
            max_part_retries=num_retries, header_provider=self._clients.header_provider
        ) as dl:
            results = dl.download_files(items)

        for failed_path, ex in results.failed.items():
            logger.error("Failed to download %s", failed_path, exc_info=ex)

        logger.info("Successfully downloaded %d/%d files", len(results.succeeded), len(items))
        return results.succeeded

    def get_file_size(self) -> int | None:
        """Retrieves the size of the file in bytes, or None if it could not be determined

        Note:
            This has only been extensively tested on AWS-based environments, and may fail in some
            self-hosted environments-- a RuntimeError will be thrown in this case.
        """
        # TODO(drake): pull out functionality in a more re-usable way without requiring the downloader class
        with MultipartFileDownloader.create(max_workers=1, header_provider=self._clients.header_provider) as downloader:
            size, _ = downloader._head_or_probe(self._presigned_url_provider())
            return size

    @classmethod
    def _from_proto(cls, clients: _Clients, dataset_file: dataset_files_pb2.DatasetFile) -> Self:
        timestamp_metadata = dataset_file.timestamp_metadata if dataset_file.HasField("timestamp_metadata") else None
        tags = dataset_file.ingest_tag_metadata if dataset_file.HasField("ingest_tag_metadata") else None
        error = dataset_file.ingest_status.failed if dataset_file.ingest_status.HasField("failed") else None
        return cls(
            id=dataset_file.id,
            dataset_rid=dataset_file.dataset_rid,
            name=dataset_file.name,
            bounds=Bounds._from_proto(dataset_file.bounds) if dataset_file.HasField("bounds") else None,
            uploaded_at=dataset_file.uploaded_at.ToNanoseconds(),
            ingested_at=dataset_file.ingested_at.ToNanoseconds() if dataset_file.HasField("ingested_at") else None,
            deleted_at=dataset_file.deleted_at.ToNanoseconds() if dataset_file.HasField("deleted_at") else None,
            ingest_status=IngestStatus._from_proto(dataset_file.ingest_status),
            timestamp_channel=None if timestamp_metadata is None else timestamp_metadata.column,
            timestamp_type=None
            if timestamp_metadata is None
            else _proto_timestamp_type_to_typed_timestamp_type(timestamp_metadata.type),
            file_tags=None if tags is None else dict(tags.additional_file_tags),
            tag_columns=None if tags is None else dict(tags.tag_columns),
            _clients=clients,
            _ingest_error_message=None if error is None else f"{error.message} ({error.error_type})",
        )


def _get_dataset_file(clients: DatasetFile._Clients, dataset_rid: str, file_id: str) -> dataset_files_pb2.DatasetFile:
    with translate_grpc_errors():
        return clients.grpc_dataset_files.GetDatasetFile(
            dataset_files_pb2.GetDatasetFileRequest(dataset_rid=dataset_rid, file_id=file_id)
        ).dataset_file


def _dataset_file_from_proto(clients: DatasetFile._Clients, dataset_file: dataset_files_pb2.DatasetFile) -> DatasetFile:
    """Build the correct DatasetFile subtype for a Catalog row.

    Returns a VideoDatasetFile when the row carries video metadata, otherwise a plain DatasetFile.
    This is the only place that knows about both types; the class factories stay type-specific.
    """
    # Local import avoids an import cycle (video_dataset_file imports this module).
    from nominal.core.video_dataset_file import VideoDatasetFile

    metadata = dataset_file.metadata
    if metadata.HasField("video"):
        return VideoDatasetFile._from_proto(clients, dataset_file)
    return DatasetFile._from_proto(clients, dataset_file)


# TODO(drake): rename to something more dataset-file specific, expose in nominal.core __init__.py
class IngestStatus(Enum):
    SUCCESS = "SUCCESS"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    DELETION_IN_PROGRESS = "DELETION_IN_PROGRESS"
    DELETED = "DELETED"
    QUEUED = "QUEUED"
    PARSING = "PARSING"
    INGESTING = "INGESTING"
    UNKNOWN = "UNKNOWN"
    """Unknown or unrecognized status returned by a newer server."""

    @classmethod
    def _from_proto(cls, status: dataset_files_pb2.IngestStatus) -> IngestStatus:
        ingest_status = cls.UNKNOWN
        match status.WhichOneof("status"):
            case "succeeded":
                ingest_status = cls.SUCCESS
            case "failed":
                ingest_status = cls.FAILED
            case "deletion_in_progress":
                ingest_status = cls.DELETION_IN_PROGRESS
            case "deleted":
                ingest_status = cls.DELETED
            case "in_progress":
                match status.in_progress.phase:
                    case dataset_files_pb2.INGEST_PHASE_UNSPECIFIED:
                        ingest_status = cls.IN_PROGRESS
                    case dataset_files_pb2.INGEST_PHASE_QUEUED:
                        ingest_status = cls.QUEUED
                    case dataset_files_pb2.INGEST_PHASE_PARSING:
                        ingest_status = cls.PARSING
                    case dataset_files_pb2.INGEST_PHASE_INGESTING:
                        ingest_status = cls.INGESTING
        if ingest_status is cls.UNKNOWN:
            logger.warning(
                "Unrecognized ingest status %s (phase %s); treating as UNKNOWN.",
                status.WhichOneof("status"),
                status.in_progress.phase,
            )
        return ingest_status


class IngestWaitType(Enum):
    """The completion condition for `wait_for_files_to_ingest`."""

    FIRST_COMPLETED = "FIRST_COMPLETED"
    FIRST_EXCEPTION = "FIRST_EXCEPTION"
    ALL_COMPLETED = "ALL_COMPLETED"


def _batch_refresh_files(files: Sequence[DatasetFile], *, batch_size: int = 100) -> set[str]:
    """Batch-fetches the latest API state for all files and refreshes them in-place.

    Returns the set of file IDs that were absent from the batch response (i.e. not found on the server).
    """
    absent_ids: set[str] = set()
    by_dataset: dict[str, list[DatasetFile]] = defaultdict(list)
    for file in files:
        by_dataset[file.dataset_rid].append(file)
    for dataset_rid, dataset_files in by_dataset.items():
        clients = dataset_files[0]._clients
        for chunk in batched(dataset_files, batch_size):
            request = dataset_files_pb2.BatchGetDatasetFilesRequest(
                dataset_rid=dataset_rid,
                file_ids=[f.id for f in chunk],
            )
            with translate_grpc_errors():
                response = clients.grpc_dataset_files.BatchGetDatasetFiles(request)
            results = {file.id: file for file in response.dataset_files}
            for file in chunk:
                if (latest_api := results.get(file.id)) is not None:
                    file._refresh_from_api(latest_api)
                else:
                    absent_ids.add(file.id)
    return absent_ids


def _poll_files_once(files: Sequence[DatasetFile]) -> tuple[list[DatasetFile], list[DatasetFile], bool]:
    """Refresh `files` from the server once and partition them into (done, not done, any failed).

    A file that is absent from the batch response, has failed, or reports an unrecognized status is
    treated as done, so an unknown status can never wedge a caller's polling loop.
    """
    done: list[DatasetFile] = []
    not_done: list[DatasetFile] = []
    has_failed = False

    absent_ids = _batch_refresh_files(files)

    for file in files:
        if file.id in absent_ids:
            logger.warning(
                "Dataset file %s from dataset %s was absent from the batch response "
                "— it may have been deleted or never created successfully.",
                file.id,
                file.dataset_rid,
            )
            done.append(file)
            has_failed = True
            continue
        match file.ingest_status:
            case IngestStatus.SUCCESS | IngestStatus.DELETION_IN_PROGRESS | IngestStatus.DELETED:
                done.append(file)
            case IngestStatus.FAILED:
                logger.warning(
                    "Dataset file %s from dataset %s failed to ingest! Error: %s",
                    file.id,
                    file.dataset_rid,
                    file._ingest_error_message,
                )
                done.append(file)
                has_failed = True
            case IngestStatus.IN_PROGRESS | IngestStatus.QUEUED | IngestStatus.PARSING | IngestStatus.INGESTING:
                not_done.append(file)
            case _:
                logger.warning(
                    "Dataset file %s from dataset %s had unknown ingest status %s; treating as done.",
                    file.id,
                    file.dataset_rid,
                    file.ingest_status,
                )
                done.append(file)

    return done, not_done, has_failed


def _deadline_from(timeout: datetime.timedelta | None) -> float | None:
    """Turn a caller's timeout into a monotonic deadline, or None for an unbounded wait.

    Monotonic rather than wall-clock: a wait bounded by `datetime.now()` stretches past the budget the
    caller asked for when the system clock steps backwards, and expires early when it steps forwards.
    Pair with `_sleep_until_next_poll`, which reads the same clock — a deadline from anywhere else is
    not comparable to it.
    """
    return None if timeout is None else time.monotonic() + timeout.total_seconds()


def _sleep_until_next_poll(poll_interval: datetime.timedelta, deadline: float | None) -> bool:
    """Sleep until the next poll is due, capped at `deadline`. Returns whether any budget remained.

    Never sleeps past the deadline: waiting out a whole poll interval near the end of a caller's budget
    would overshoot the timeout they asked for. Callers own what an exhausted budget means — this only
    reports it, since one waiter raises on it and another returns what it has.
    """
    sleep_for = poll_interval.total_seconds()
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        sleep_for = min(sleep_for, remaining)

    logger.info("Sleeping for %f seconds until the next poll...", sleep_for)
    time.sleep(sleep_for)
    return True


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
        timeout: If given, the maximum time to wait before returning. Never sleeps past the deadline.
        return_when: Condition for this function to exit. By default, this function will block until all files
            have completed their ingestion (successfully or unsuccessfully), but this can be changed to return
            upon the first completed or first failing ingest. This behavior mirrors that of
            `concurrent.futures.wait`.

    Returns:
        Returns a tuple of (done, not done) dataset files.
    """
    deadline = _deadline_from(timeout)
    done: list[DatasetFile] = []
    not_done: list[DatasetFile] = [*files]
    has_failed = False

    while not_done and (deadline is None or time.monotonic() < deadline):
        logger.info("Polling for ingestion completion for %d files (%d total)", len(not_done), len(files))

        newly_done, not_done, newly_failed = _poll_files_once(not_done)
        done.extend(newly_done)
        has_failed = has_failed or newly_failed

        if has_failed and return_when is IngestWaitType.FIRST_EXCEPTION:
            break
        elif done and return_when is IngestWaitType.FIRST_COMPLETED:
            break
        elif not not_done:
            break

        if not _sleep_until_next_poll(poll_interval, deadline):
            break

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
