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

from nominal._api.combined import upload_api

from .._api.combined import ingest_api, scout_catalog
from .._utils import (
    CustomTimestampFormat,
    FileType,
    FileTypes,
    TimestampColumnType,
    _timestamp_type_to_conjure_ingest_api,
    update_dataclass,
)
from ..exceptions import NominalIngestError, NominalIngestFailed
from ._multipart import put_multipart_upload
from ._utils import verify_csv_path


@dataclass(frozen=True)
class Dataset:
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: _DatasetClient = field(repr=False)

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until dataset ingestion has completed.
        This method polls Nominal for ingest status after uploading a dataset on an interval.

        Raises:
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known
        """

        while True:
            progress = self._client.get_ingest_progress_v2(self.rid)
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
        response = self._client.update(name, description, properties, labels)
        dataset = self.__class__._from_conjure(self._client, response)
        update_dataclass(self, dataset, fields=self.__dataclass_fields__)
        return self

    def add_csv_to_dataset(self, path: Path | str, timestamp_column: str, timestamp_type: TimestampColumnType) -> None:
        """Append to a dataset from a csv on-disk."""
        path, file_type = verify_csv_path(path)
        with open(path, "rb") as csv_file:
            self.add_to_dataset_from_io(csv_file, timestamp_column, timestamp_type, file_type)

    def add_to_dataset_from_io(
        self,
        dataset: BinaryIO,
        timestamp_column: str,
        timestamp_type: TimestampColumnType,
        file_type: tuple[str, str] | FileType = FileTypes.CSV,
    ) -> None:
        """Append to a dataset from a file-like object.

        file_type: a (extension, mimetype) pair describing the type of file.
        """

        if not isinstance(timestamp_type, CustomTimestampFormat):
            if timestamp_type.startswith("relative"):
                raise ValueError(
                    "multifile datasets with relative timestamps are not yet supported by the client library"
                )

        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset!r} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)

        self.poll_until_ingestion_completed()
        urlsafe_name = urllib.parse.quote_plus(self.name)
        filename = f"{urlsafe_name}{file_type.extension}"

        return self._client.add_file(self.rid, dataset, filename, file_type.mimetype, timestamp_column, timestamp_type)

    @classmethod
    def _from_conjure(cls, client: _DatasetClient, dataset: scout_catalog.EnrichedDataset) -> Self:
        return cls(
            rid=dataset.rid,
            name=dataset.name,
            description=dataset.description,
            properties=MappingProxyType(dataset.properties),
            labels=tuple(dataset.labels),
            _client=client,
        )


@dataclass
class _DatasetClient:
    """Makes the API calls."""

    auth_header: str
    catalog_client: scout_catalog.CatalogService
    ingest_client: ingest_api.IngestService
    upload_client: upload_api.UploadService

    def get_datasets(self, dataset_rids: Iterable[str]) -> Iterable[scout_catalog.EnrichedDataset]:
        request = scout_catalog.GetDatasetsRequest(dataset_rids=list(dataset_rids))
        yield from self.catalog_client.get_enriched_datasets(self.auth_header, request)

    def get_dataset(self, dataset_rid: str) -> scout_catalog.EnrichedDataset:
        datasets = list(self.get_datasets([dataset_rid]))
        if not datasets:
            raise ValueError(f"dataset {dataset_rid!r} not found")
        if len(datasets) > 1:
            raise ValueError(f"expected exactly one dataset, got {len(datasets)}")
        return datasets[0]

    def get_ingest_progress_v2(self, dataset_rid: str) -> scout_catalog.IngestProgressV2:
        return self.catalog_client.get_ingest_progress_v2(dataset_rid)

    def add_file(
        self,
        dataset_rid: str,
        dataset: BinaryIO,
        filename: str,
        mimetype: str,
        timestamp_column: str,
        timestamp_type: ingest_api.TimestampType,
    ) -> ingest_api.TriggeredIngest:
        s3_path = put_multipart_upload(self.auth_header, dataset, filename, mimetype, self.upload_client)
        request = ingest_api.TriggerFileIngest(
            destination=ingest_api.IngestDestination(
                existing_dataset=ingest_api.ExistingDatasetIngestDestination(dataset_rid=dataset_rid)
            ),
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            source_metadata=ingest_api.IngestSourceMetadata(
                timestamp_metadata=ingest_api.TimestampMetadata(
                    series_name=timestamp_column,
                    timestamp_type=_timestamp_type_to_conjure_ingest_api(timestamp_type),
                ),
            ),
        )
        return self.ingest_client.trigger_file_ingest(self.auth_header, request)

    def update(
        self,
        dataset_rid: str,
        name: str | None,
        description: str | None,
        properties: Mapping[str, str] | None,
        labels: Sequence[str] | None,
    ) -> scout_catalog.EnrichedDataset:
        request = scout_catalog.UpdateDatasetMetadata(
            description=description,
            labels=None if labels is None else list(labels),
            name=name,
            properties=None if properties is None else dict(properties),
        )
        return self.catalog_client.update_dataset_metadata(self.auth_header, dataset_rid, request)
