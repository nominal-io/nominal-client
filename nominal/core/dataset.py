from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from io import TextIOBase
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Mapping, Sequence

from nominal_api import api, datasource_api, ingest_api, scout_catalog
from typing_extensions import Self, TypeAlias

from nominal._utils import deprecate_arguments
from nominal.core._multipart import path_upload_name, upload_multipart_file, upload_multipart_io
from nominal.core._utils import update_dataclass
from nominal.core.bounds import Bounds
from nominal.core.channel import Channel
from nominal.core.dataset_file import DatasetFile
from nominal.core.datasource import DataSource
from nominal.core.filetype import FileType, FileTypes
from nominal.exceptions import NominalIngestError, NominalIngestFailed, NominalIngestMultiError
from nominal.ts import (
    _AnyTimestampType,
    _to_typed_timestamp_type,
)

logger = logging.getLogger(__name__)

DatasetBounds: TypeAlias = Bounds


@dataclass(frozen=True)
class Dataset(DataSource):
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    bounds: DatasetBounds | None

    @property
    def nominal_url(self) -> str:
        """Returns a URL to the page in the nominal app containing this dataset"""
        # TODO (drake): move logic into _from_conjure() factory function to accomodate different URL schemes
        return f"https://app.gov.nominal.io/data-sources/{self.rid}"

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> Self:
        """Block until dataset ingestion has completed.
        This method polls Nominal for ingest status after uploading a dataset on an interval.

        Raises:
        ------
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known

        """
        while True:
            progress = self._clients.catalog.get_ingest_progress_v2(self._clients.auth_header, self.rid)
            if progress.ingest_status.type == "success":
                break
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

        # Update metadata now that data has successfully ingested
        return self.refresh()

    def refresh(self) -> Self:
        updated_dataset = self.__class__._from_conjure(
            self._clients,
            _get_dataset(self._clients.auth_header, self._clients.catalog, self.rid),
        )
        update_dataclass(self, updated_dataset, fields=self.__dataclass_fields__)
        return self

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
        self._clients.catalog.update_dataset_metadata(self._clients.auth_header, self.rid, request)

        return self.refresh()

    def add_csv_to_dataset(self, path: Path | str, timestamp_column: str, timestamp_type: _AnyTimestampType) -> None:
        """Append to a dataset from a csv on-disk."""
        self.add_data_to_dataset(path, timestamp_column, timestamp_type)

    def add_data_to_dataset(self, path: Path | str, timestamp_column: str, timestamp_type: _AnyTimestampType) -> None:
        """Append to a dataset from data on-disk."""
        path = Path(path)
        file_type = FileType.from_path_dataset(path)
        with open(path, "rb") as data_file:
            self.add_to_dataset_from_io(
                data_file, timestamp_column, timestamp_type, file_type, file_name=path_upload_name(path, file_type)
            )

    def add_to_dataset_from_io(
        self,
        dataset: BinaryIO,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        file_type: tuple[str, str] | FileType = FileTypes.CSV,
        file_name: str | None = None,
    ) -> None:
        """Append to a dataset from a file-like object.

        file_type: a (extension, mimetype) pair describing the type of file.
        """
        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset!r} must be open in binary mode, rather than text mode")

        self.poll_until_ingestion_completed()

        if file_name is None:
            file_name = self.name

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(
            self._clients.auth_header,
            dataset,
            file_name,
            file_type,
            self._clients.upload,
        )
        request = ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                csv=ingest_api.CsvOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                    target=ingest_api.DatasetIngestTarget(
                        existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
                    ),
                    timestamp_metadata=ingest_api.TimestampMetadata(
                        series_name=timestamp_column,
                        timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
                    ),
                )
            )
        )
        self._clients.ingest.ingest(self._clients.auth_header, request)

    def add_journal_json_to_dataset(
        self,
        path: Path | str,
    ) -> None:
        """Add a journald jsonl file to an existing dataset."""
        self.poll_until_ingestion_completed()
        log_path = Path(path)
        file_type = FileType.from_path_journal_json(log_path)
        s3_path = upload_multipart_file(
            self._clients.auth_header,
            log_path,
            self._clients.upload,
            file_type=file_type,
        )
        target = ingest_api.DatasetIngestTarget(
            existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
        )
        self._clients.ingest.ingest(
            self._clients.auth_header,
            ingest_api.IngestRequest(
                options=ingest_api.IngestOptions(
                    journal_json=ingest_api.JournalJsonOpts(
                        source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)), target=target
                    )
                )
            ),
        )

    def add_mcap_to_dataset(
        self,
        path: Path | str,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
    ) -> None:
        """Add an MCAP file to an existing dataset.

        Args:
        ----
            path: Path to the MCAP file to add to this dataset
            include_topics: If present, list of topics to restrict ingestion to.
                If not present, defaults to all protobuf-encoded topics present in the MCAP.
            exclude_topics: If present, list of topics to not ingest from the MCAP.
        """
        path = Path(path)
        with path.open("rb") as data_file:
            self.add_mcap_to_dataset_from_io(
                data_file,
                include_topics=include_topics,
                exclude_topics=exclude_topics,
                file_name=path_upload_name(path, FileTypes.MCAP),
            )

    def add_mcap_to_dataset_from_io(
        self,
        mcap: BinaryIO,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
        file_name: str | None = None,
    ) -> None:
        """Add data to this dataset from an MCAP file-like object.

        The mcap must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.

        Args:
        ----
            mcap: Binary file-like MCAP stream
            include_topics: If present, list of topics to restrict ingestion to.
                If not present, defaults to all protobuf-encoded topics present in the MCAP.
            exclude_topics: If present, list of topics to not ingest from the MCAP.
            file_name: If present, name to use when uploading file. Otherwise, defaults to dataset name.
        """
        if isinstance(mcap, TextIOBase):
            raise TypeError(f"mcap {mcap} must be open in binary mode, rather than text mode")

        self.poll_until_ingestion_completed()

        if file_name is None:
            file_name = self.name

        s3_path = upload_multipart_io(
            self._clients.auth_header,
            mcap,
            file_name,
            file_type=FileTypes.MCAP,
            upload_client=self._clients.upload,
        )

        channels = _create_mcap_channels(include_topics, exclude_topics)
        target = ingest_api.DatasetIngestTarget(
            existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
        )

        request = _create_mcap_ingest_request(s3_path, channels, target)
        resp = self._clients.ingest.ingest(self._clients.auth_header, request)
        if resp.details.dataset is None or resp.details.dataset.dataset_rid is None:
            raise NominalIngestError("error ingesting mcap: no dataset created or updated")

    def add_ardupilot_dataflash_to_dataset(
        self,
        path: Path | str,
    ) -> None:
        """Add a Dataflash file to an existing dataset."""
        self.poll_until_ingestion_completed()
        dataflash_path = Path(path)
        s3_path = upload_multipart_file(
            self._clients.auth_header,
            dataflash_path,
            self._clients.upload,
            file_type=FileTypes.DATAFLASH,
        )
        target = ingest_api.DatasetIngestTarget(
            existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
        )
        request = _create_dataflash_ingest_request(s3_path, target)
        self._clients.ingest.ingest(self._clients.auth_header, request)

    def archive(self) -> None:
        """Archive this dataset.
        Archived datasets are not deleted, but are hidden from the UI.
        """
        self._clients.catalog.archive_dataset(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchives this dataset, allowing it to show up in the 'All Datasets' pane in the UI."""
        self._clients.catalog.unarchive_dataset(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: DataSource._Clients, dataset: scout_catalog.EnrichedDataset) -> Self:
        return cls(
            rid=dataset.rid,
            name=dataset.name,
            description=dataset.description,
            properties=MappingProxyType(dataset.properties),
            labels=tuple(dataset.labels),
            bounds=None if dataset.bounds is None else DatasetBounds._from_conjure(dataset.bounds),
            _clients=clients,
        )

    @deprecate_arguments(
        deprecated_args=["exact_match", "fuzzy_search_text"],
        new_kwarg="names",
        new_method=DataSource.get_channels,
    )
    def get_channels(
        self,
        exact_match: Sequence[str] = (),
        fuzzy_search_text: str = "",
        *,
        names: Iterable[str] | None = None,
    ) -> Iterable[Channel]:
        """Look up the metadata for all matching channels associated with this dataset.

        Args:
        ----
            exact_match: Filter the returned channels to those whose names match all provided strings
                (case insensitive).
                For example, a channel named 'engine_turbine_rpm' would match against ['engine', 'turbine', 'rpm'],
                whereas a channel named 'engine_turbine_flowrate' would not!
            fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.
            names: List of channel names to look up metadata for. This parameter is preferred over
                exact_match and fuzzy_search_text, which are deprecated.

        Yields:
        ------
            Yields a sequence of channel metadata objects which match the provided query parameters

        """
        next_page_token = None
        while True:
            query = datasource_api.SearchChannelsRequest(
                data_sources=[self.rid],
                exact_match=list(exact_match),
                fuzzy_search_text=fuzzy_search_text,
                previously_selected_channels={},
                next_page_token=next_page_token,
                page_size=None,
                prefix=None,
            )
            response = self._clients.datasource.search_channels(self._clients.auth_header, query)
            for channel_metadata in response.results:
                # Skip series archetypes for now-- they aren't handled by the rest of the SDK in a graceful manner
                if channel_metadata.series_rid.logical_series is None:
                    continue
                yield Channel._from_conjure_datasource_api(self._clients, channel_metadata)

            if response.next_page_token is None:
                break
            else:
                next_page_token = response.next_page_token

    def list_files(self) -> Iterable[DatasetFile]:
        next_page_token = None
        while True:
            files_page = self._clients.catalog.list_dataset_files(self._clients.auth_header, self.rid, next_page_token)
            for file in files_page.files:
                if file.ingest_status.type == "success":
                    yield DatasetFile._from_conjure(file)
                else:
                    logger.debug(
                        "Ignoring dataset file %s (id=%s)-- status '%s' is not 'success'!",
                        file.name,
                        file.id,
                        file.ingest_status.type,
                    )

            if files_page.next_page is None:
                break
            else:
                next_page_token = files_page.next_page


def poll_until_ingestion_completed(datasets: Iterable[Dataset], interval: timedelta = timedelta(seconds=1)) -> None:
    """Block until all dataset ingestions have completed (succeeded or failed).

    This method polls Nominal for ingest status on each of the datasets on an interval.
    No specific ordering is guaranteed, but all datasets will be checked at least once.

    Raises:
    ------
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


def _get_datasets(
    auth_header: str, client: scout_catalog.CatalogService, dataset_rids: Iterable[str]
) -> Iterable[scout_catalog.EnrichedDataset]:
    request = scout_catalog.GetDatasetsRequest(dataset_rids=list(dataset_rids))
    yield from client.get_enriched_datasets(auth_header, request)


def _get_dataset(
    auth_header: str, client: scout_catalog.CatalogService, dataset_rid: str
) -> scout_catalog.EnrichedDataset:
    datasets = list(_get_datasets(auth_header, client, [dataset_rid]))
    if not datasets:
        raise ValueError(f"dataset {dataset_rid!r} not found")
    if len(datasets) > 1:
        raise ValueError(f"expected exactly one dataset, got {len(datasets)}")
    return datasets[0]


def _create_dataflash_ingest_request(s3_path: str, target: ingest_api.DatasetIngestTarget) -> ingest_api.IngestRequest:
    return ingest_api.IngestRequest(
        ingest_api.IngestOptions(
            dataflash=ingest_api.DataflashOpts(
                source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                target=target,
            )
        ),
    )


def _create_mcap_ingest_request(
    s3_path: str, channels: ingest_api.McapChannels, target: ingest_api.DatasetIngestTarget
) -> ingest_api.IngestRequest:
    return ingest_api.IngestRequest(
        ingest_api.IngestOptions(
            mcap_protobuf_timeseries=ingest_api.McapProtobufTimeseriesOpts(
                source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                target=target,
                channel_filter=channels,
                timestamp_type=ingest_api.McapTimestampType(ingest_api.LogTime()),
            )
        )
    )


def _create_mcap_channels(
    include_topics: Iterable[str] | None = None,
    exclude_topics: Iterable[str] | None = None,
) -> ingest_api.McapChannels:
    channels = ingest_api.McapChannels(all=api.Empty())
    if include_topics is not None and exclude_topics is not None:
        include_topics = [t for t in include_topics if t not in exclude_topics]
    if include_topics is not None:
        channels = ingest_api.McapChannels(include=[api.McapChannelLocator(topic=topic) for topic in include_topics])
    elif exclude_topics is not None:
        channels = ingest_api.McapChannels(exclude=[api.McapChannelLocator(topic=topic) for topic in exclude_topics])
    return channels
