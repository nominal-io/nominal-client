from __future__ import annotations

import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import timedelta
from io import TextIOBase
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Mapping, Sequence

from typing_extensions import Self

from .._api.combined import (
    datasource,
    datasource_logset_api,
    ingest_api,
    scout_catalog,
    scout_video_api,
)
from .._checklist import Check, Checklist, ChecklistBuilder
from .._multipart import put_multipart_upload
from .._utils import FileType, FileTypes, update_dataclass
from ..exceptions import NominalIngestError, NominalIngestFailed, NominalIngestMultiError
from ..ts import IntegralNanosecondsUTC, LogTimestampType, _AnyTimestampType, _SecondsNanos, _to_typed_timestamp_type
from .attachment import Attachment
from .client import NominalClient
from .run import Run

__all__ = [
    "Attachment",
    "Check",
    "Checklist",
    "ChecklistBuilder",
    "Dataset",
    "LogSet",
    "NominalClient",
    "Run",
    "Video",
]


@dataclass(frozen=True)
class User:
    rid: str
    display_name: str
    email: str


@dataclass(frozen=True)
class Dataset:
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient = field(repr=False)

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until dataset ingestion has completed.
        This method polls Nominal for ingest status after uploading a dataset on an interval.

        Raises:
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known
        """

        while True:
            progress = self._client._catalog_client.get_ingest_progress_v2(self._client._auth_header, self.rid)
            if progress.ingest_status.type == "success":
                return
            elif progress.ingest_status.type == "inProgress":  # "type" strings are camelCase
                pass
            elif progress.ingest_status.type == "error":
                error = progress.ingest_status.error
                if error is not None:
                    raise NominalIngestFailed(
                        f"ingest failed for dataset {self.rid!r}: {error.message} ({error.error_type})"
                    )
                raise NominalIngestError(
                    f"ingest status type marked as 'error' but with no instance for dataset {self.rid!r}"
                )
            else:
                raise NominalIngestError(
                    f"unhandled ingest status {progress.ingest_status.type!r} for dataset {self.rid!r}"
                )
            time.sleep(interval.total_seconds())

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace dataset metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in dataset.labels:
                new_labels.append(old_label)
            dataset = dataset.update(labels=new_labels)
        """
        request = scout_catalog.UpdateDatasetMetadata(
            description=description,
            labels=None if labels is None else list(labels),
            name=name,
            properties=None if properties is None else dict(properties),
        )
        response = self._client._catalog_client.update_dataset_metadata(self._client._auth_header, self.rid, request)

        dataset = self.__class__._from_conjure(self._client, response)
        update_dataclass(self, dataset, fields=self.__dataclass_fields__)
        return self

    def add_csv_to_dataset(self, path: Path | str, timestamp_column: str, timestamp_type: _AnyTimestampType) -> None:
        """Append to a dataset from a csv on-disk."""
        path, file_type = _verify_csv_path(path)
        with open(path, "rb") as csv_file:
            self.add_to_dataset_from_io(csv_file, timestamp_column, timestamp_type, file_type)

    def add_to_dataset_from_io(
        self,
        dataset: BinaryIO,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        file_type: tuple[str, str] | FileType = FileTypes.CSV,
    ) -> None:
        """Append to a dataset from a file-like object.

        file_type: a (extension, mimetype) pair describing the type of file.
        """

        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset!r} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)

        self.poll_until_ingestion_completed()
        urlsafe_name = urllib.parse.quote_plus(self.name)
        filename = f"{urlsafe_name}{file_type.extension}"
        s3_path = put_multipart_upload(
            self._client._auth_header, dataset, filename, file_type.mimetype, self._client._upload_client
        )
        request = ingest_api.TriggerFileIngest(
            destination=ingest_api.IngestDestination(
                existing_dataset=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
            ),
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            source_metadata=ingest_api.IngestSourceMetadata(
                timestamp_metadata=ingest_api.TimestampMetadata(
                    series_name=timestamp_column,
                    timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
                ),
            ),
        )
        self._client._ingest_client.trigger_file_ingest(self._client._auth_header, request)

    @classmethod
    def _from_conjure(cls, client: NominalClient, dataset: scout_catalog.EnrichedDataset) -> Self:
        return cls(
            rid=dataset.rid,
            name=dataset.name,
            description=dataset.description,
            properties=MappingProxyType(dataset.properties),
            labels=tuple(dataset.labels),
            _client=client,
        )


@dataclass(frozen=True)
class LogSet:
    rid: str
    name: str
    timestamp_type: LogTimestampType
    description: str | None
    _client: NominalClient = field(repr=False)

    def _stream_logs_paginated(self) -> Iterable[datasource_logset_api.Log]:
        request = datasource_logset_api.SearchLogsRequest()
        while True:
            response = self._client._logset_client.search_logs(
                self._client._auth_header,
                log_set_rid=self.rid,
                request=request,
            )
            yield from response.logs
            if response.next_page_token is None:
                break
            request = datasource_logset_api.SearchLogsRequest(token=response.next_page_token)

    def stream_logs(self) -> Iterable[Log]:
        """Iterate over the logs."""
        for log in self._stream_logs_paginated():
            yield Log._from_conjure(log)

    @classmethod
    def _from_conjure(cls, client: NominalClient, log_set_metadata: datasource_logset_api.LogSetMetadata) -> Self:
        return cls(
            rid=log_set_metadata.rid,
            name=log_set_metadata.name,
            timestamp_type=_log_timestamp_type_from_conjure(log_set_metadata.timestamp_type),
            description=log_set_metadata.description,
            _client=client,
        )


@dataclass(frozen=True)
class Log:
    timestamp: IntegralNanosecondsUTC
    body: str

    def _to_conjure(self) -> datasource_logset_api.Log:
        return datasource_logset_api.Log(
            time=_SecondsNanos.from_nanoseconds(self.timestamp).to_api(),
            body=datasource_logset_api.LogBody(
                basic=datasource_logset_api.BasicLogBody(message=self.body, properties={}),
            ),
        )

    @classmethod
    def _from_conjure(cls, log: datasource_logset_api.Log) -> Self:
        if log.body.basic is None:
            raise RuntimeError(f"unhandled log body type: expected 'basic' but got {log.body.type!r}")
        return cls(timestamp=_SecondsNanos.from_api(log.time).to_nanoseconds(), body=log.body.basic.message)


@dataclass(frozen=True)
class Video:
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient = field(repr=False)

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until video ingestion has completed.
        This method polls Nominal for ingest status after uploading a video on an interval.

        Raises:
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known
        """

        while True:
            progress = self._client._video_client.get_ingest_status(self._client._auth_header, self.rid)
            if progress.type == "success":
                return
            elif progress.type == "inProgress":  # "type" strings are camelCase
                pass
            elif progress.type == "error":
                error = progress.error
                if error is not None:
                    error_messages = ", ".join([e.message for e in error.errors])
                    error_types = ", ".join([e.error_type for e in error.errors])
                    raise NominalIngestFailed(f"ingest failed for video {self.rid!r}: {error_messages} ({error_types})")
                raise NominalIngestError(
                    f"ingest status type marked as 'error' but with no instance for video {self.rid!r}"
                )
            else:
                raise NominalIngestError(f"unhandled ingest status {progress.type!r} for video {self.rid!r}")
            time.sleep(interval.total_seconds())

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace video metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in video.labels:
                new_labels.append(old_label)
            video = video.update(labels=new_labels)
        """
        # TODO(alkasm): properties SHOULD be optional here, but they're not.
        # For uniformity with other methods, will always "update" with current props on the client.
        request = scout_video_api.UpdateVideoMetadataRequest(
            description=description,
            labels=None if labels is None else list(labels),
            title=name,
            properties=dict(self.properties if properties is None else properties),
        )
        response = self._client._video_client.update_metadata(self._client._auth_header, request, self.rid)

        video = self.__class__._from_conjure(self._client, response)
        update_dataclass(self, video, fields=self.__dataclass_fields__)
        return self

    @classmethod
    def _from_conjure(cls, client: NominalClient, video: scout_video_api.Video) -> Self:
        return cls(
            rid=video.rid,
            name=video.title,
            description=video.description,
            properties=MappingProxyType(video.properties),
            labels=tuple(video.labels),
            _client=client,
        )


def _get_datasets(
    auth_header: str, client: scout_catalog.CatalogService, dataset_rids: Iterable[str]
) -> Iterable[scout_catalog.EnrichedDataset]:
    request = scout_catalog.GetDatasetsRequest(dataset_rids=list(dataset_rids))
    yield from client.get_enriched_datasets(auth_header, request)


def _verify_csv_path(path: Path | str) -> tuple[Path, FileType]:
    path = Path(path)
    file_type = FileType.from_path_dataset(path)
    if file_type.extension not in (".csv", ".csv.gz"):
        raise ValueError(f"file {path} must end with '.csv' or '.csv.gz'")
    return path, file_type


def _log_timestamp_type_from_conjure(log_timestamp_type: datasource.TimestampType) -> LogTimestampType:
    if log_timestamp_type == datasource.TimestampType.ABSOLUTE:
        return "absolute"
    elif log_timestamp_type == datasource.TimestampType.RELATIVE:
        return "relative"
    raise ValueError(f"unhandled timestamp type {log_timestamp_type}")


def poll_until_ingestion_completed(datasets: Iterable[Dataset], interval: timedelta = timedelta(seconds=1)) -> None:
    """Block until all dataset ingestions have completed (succeeded or failed).

    This method polls Nominal for ingest status on each of the datasets on an interval.
    No specific ordering is guaranteed, but all datasets will be checked at least once.

    Raises:
        NominalIngestMultiError: if any of the datasets failed to ingest
    """
    errors = {}
    for dataset in datasets:
        try:
            dataset.poll_until_ingestion_completed(interval=interval)
        except NominalIngestError as e:
            errors[dataset.rid] = e
    if errors:
        raise NominalIngestMultiError(errors)
