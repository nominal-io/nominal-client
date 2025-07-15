from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from io import TextIOBase
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Mapping, Sequence

from nominal_api import api, ingest_api, scout_catalog
from typing_extensions import Self, TypeAlias

from nominal._utils import update_dataclass
from nominal.core._multipart import path_upload_name, upload_multipart_file, upload_multipart_io
from nominal.core.bounds import Bounds
from nominal.core.dataset_file import DatasetFile
from nominal.core.datasource import DataSource
from nominal.core.filetype import FileType, FileTypes
from nominal.core.log import LogPoint, _write_logs
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
        return f"{self._clients.app_base_url}/data-sources/{self.rid}"

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> Self:
        """Block until dataset file ingestion has completed.
        This method polls Nominal for ingest status after uploading a file to a dataset on an interval.

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

    def add_tabular_data(
        self,
        path: Path | str,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Append to a dataset from tabular data on-disk.

        Currently, the supported filetypes are:
            - .csv / .csv.gz
            - .parquet / .parquet.gz
            - .parquet.tar / .parquet.tar.gz / .parquet.zip

        Args:
            path: Path to the file on disk to add to the dataset.
            timestamp_column: Column within the file containing timestamp information.
                NOTE: this is omitted as a channel from the data added to Nominal, and is instead used
                      to set the timestamps for all other uploaded data channels.
            timestamp_type: Type of timestamp data contained within the `timestamp_column` e.g. 'epoch_seconds'.
            tag_columns: a dictionary mapping tag keys to column names.
            tags: key-value pairs to apply as tags to all data uniformly in the file
        """
        path = Path(path)
        file_type = FileType.from_tabular(path)
        with open(path, "rb") as data_file:
            self.add_from_io(
                data_file,
                timestamp_column,
                timestamp_type,
                file_type,
                file_name=path_upload_name(path, file_type),
                tag_columns=tag_columns,
                tags=tags,
            )

    # Backward compatibility
    add_tabular_data_to_dataset = add_tabular_data

    def add_from_io(
        self,
        dataset: BinaryIO,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        file_type: tuple[str, str] | FileType = FileTypes.CSV,
        file_name: str | None = None,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Append to a dataset from a file-like object.

        Args:
            dataset: a file-like object containing the data to append to the dataset.
            timestamp_column: the column in the dataset that contains the timestamp data.
            timestamp_type: the type of timestamp data in the dataset.
            file_type: a (extension, mimetype) pair describing the type of file.
            file_name: the name of the file to upload.
            tag_columns: a dictionary mapping tag keys to column names.
            tags: key-value pairs to apply as tags to all data uniformly in the file
        """
        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset!r} must be open in binary mode, rather than text mode")

        if file_name is None:
            file_name = self.name

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(
            self._clients.auth_header,
            self._clients.workspace_rid,
            dataset,
            file_name,
            file_type,
            self._clients.upload,
        )

        request = ingest_api.IngestRequest(
            options=_construct_existing_ingest_options(
                target_rid=self.rid,
                timestamp_column=timestamp_column,
                timestamp_type=timestamp_type,
                file_type=file_type,
                tag_columns=tag_columns,
                s3_path=s3_path,
                tags=tags,
            )
        )
        self._clients.ingest.ingest(self._clients.auth_header, request)

    # Backward compatibility
    add_to_dataset_from_io = add_from_io

    def add_journal_json(
        self,
        path: Path | str,
    ) -> None:
        """Add a journald jsonl file to an existing dataset."""
        log_path = Path(path)
        file_type = FileType.from_path_journal_json(log_path)
        s3_path = upload_multipart_file(
            self._clients.auth_header,
            self._clients.workspace_rid,
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

    # Backward compatibility
    add_journal_json_to_dataset = add_journal_json

    def add_mcap(
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
            self.add_mcap_from_io(
                data_file,
                include_topics=include_topics,
                exclude_topics=exclude_topics,
                file_name=path_upload_name(path, FileTypes.MCAP),
            )

    # Backward compatibility
    add_mcap_to_dataset = add_mcap

    def add_mcap_from_io(
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

        if file_name is None:
            file_name = self.name

        s3_path = upload_multipart_io(
            self._clients.auth_header,
            self._clients.workspace_rid,
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

    # Backward compatibility
    add_mcap_to_dataset_from_io = add_mcap_from_io

    def add_ardupilot_dataflash(
        self,
        path: Path | str,
    ) -> None:
        """Add a Dataflash file to an existing dataset."""
        dataflash_path = Path(path)
        s3_path = upload_multipart_file(
            self._clients.auth_header,
            self._clients.workspace_rid,
            dataflash_path,
            self._clients.upload,
            file_type=FileTypes.DATAFLASH,
        )
        target = ingest_api.DatasetIngestTarget(
            existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
        )
        request = _create_dataflash_ingest_request(s3_path, target)
        self._clients.ingest.ingest(self._clients.auth_header, request)

    # Backward compatibility
    add_ardupilot_dataflash_to_dataset = add_ardupilot_dataflash

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

    def _list_files(self) -> Iterable[scout_catalog.DatasetFile]:
        next_page_token = None
        while True:
            files_page = self._clients.catalog.list_dataset_files(self._clients.auth_header, self.rid, next_page_token)
            yield from files_page.files
            if files_page.next_page is None:
                break
            next_page_token = files_page.next_page

    def list_files(self, *, successful_only: bool = True) -> Iterable[DatasetFile]:
        """List files ingested to this dataset.

        If successful_only, yields files with a 'success' ingest status only.
        """
        files = self._list_files()
        if successful_only:
            files = filter(lambda f: f.ingest_status.type == "success", files)
        for file in files:
            yield DatasetFile._from_conjure(self._clients, file)

    def write_logs(self, logs: Iterable[LogPoint], channel_name: str = "logs", batch_size: int = 1000) -> None:
        r"""Stream logs to the datasource.

        This method executes synchronously, i.e. it blocks until all logs are sent to the API.
        Logs are sent in batches. The logs can be any iterable of LogPoints, including a generator.

        Args:
            logs: LogPoints to stream to Nominal.
            channel_name: Name of the channel to stream logs to.
            batch_size: Number of logs to send to the API at a time.

        Example:
            ```python
            from nominal.core import LogPoint

            def parse_logs_from_file(file_path: str) -> Iterable[LogPoint]:
                # 2025-04-08T14:26:28.679052Z [INFO] Sent ACTUATE_MOTOR command
                with open(file_path, "r") as f:
                    for line in f:
                        timestamp, message = line.removesuffix("\n").split(maxsplit=1)
                        yield LogPoint.create(timestamp, message)

            dataset = client.get_dataset("dataset_rid")
            logs = parse_logs_from_file("logs.txt")
            dataset.write_logs(logs)
            ```
        """
        _write_logs(
            auth_header=self._clients.auth_header,
            client=self._clients.storage_writer,
            data_source_rid=self.rid,
            logs=logs,
            channel_name=channel_name,
            batch_size=batch_size,
        )


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


def _create_dataset(
    auth_header: str,
    client: scout_catalog.CatalogService,
    name: str,
    *,
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    workspace_rid: str | None = None,
) -> scout_catalog.EnrichedDataset:
    request = scout_catalog.CreateDataset(
        name=name,
        description=description,
        labels=list(labels),
        properties={} if properties is None else dict(properties),
        is_v2_dataset=True,
        metadata={},
        origin_metadata=scout_catalog.DatasetOriginMetadata(),
        workspace=workspace_rid,
    )
    return client.create_dataset(auth_header, request)


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


def _build_channel_config(prefix_tree_delimiter: str | None) -> ingest_api.ChannelConfig | None:
    if prefix_tree_delimiter is None:
        return None
    else:
        return ingest_api.ChannelConfig(prefix_tree_delimiter=prefix_tree_delimiter)


def _construct_new_ingest_options(
    name: str,
    timestamp_column: str,
    timestamp_type: _AnyTimestampType,
    file_type: FileType,
    description: str | None,
    labels: Sequence[str],
    properties: Mapping[str, str],
    prefix_tree_delimiter: str | None,
    channel_prefix: str | None,
    tag_columns: Mapping[str, str] | None,
    s3_path: str,
    workspace_rid: str | None,
    tags: Mapping[str, str] | None,
) -> ingest_api.IngestOptions:
    source = ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path))
    target = ingest_api.DatasetIngestTarget(
        new=ingest_api.NewDatasetIngestDestination(
            labels=list(labels),
            properties=dict(properties),
            channel_config=_build_channel_config(prefix_tree_delimiter),
            dataset_description=description,
            dataset_name=name,
            workspace=workspace_rid,
        )
    )
    timestamp_metadata = ingest_api.TimestampMetadata(
        series_name=timestamp_column,
        timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
    )
    tag_columns = dict(tag_columns) if tag_columns else None

    if file_type.is_parquet():
        return ingest_api.IngestOptions(
            parquet=ingest_api.ParquetOpts(
                source=source,
                target=target,
                timestamp_metadata=timestamp_metadata,
                channel_prefix=channel_prefix,
                tag_columns=tag_columns,
                is_archive=file_type.is_parquet_archive(),
                additional_file_tags={**tags} if tags else None,
            )
        )
    else:
        if not file_type.is_csv():
            logger.warning("Expected filetype %s to be parquet or csv for creating a dataset from io", file_type)

        return ingest_api.IngestOptions(
            csv=ingest_api.CsvOpts(
                source=source,
                target=target,
                timestamp_metadata=timestamp_metadata,
                channel_prefix=channel_prefix,
                tag_columns=tag_columns,
                additional_file_tags={**tags} if tags else None,
            )
        )


def _construct_existing_ingest_options(
    target_rid: str,
    timestamp_column: str,
    timestamp_type: _AnyTimestampType,
    file_type: FileType,
    tag_columns: Mapping[str, str] | None,
    s3_path: str,
    tags: Mapping[str, str] | None,
) -> ingest_api.IngestOptions:
    source = ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path))
    target = ingest_api.DatasetIngestTarget(
        existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=target_rid)
    )
    timestamp_metadata = ingest_api.TimestampMetadata(
        series_name=timestamp_column,
        timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
    )
    tag_columns = dict(tag_columns) if tag_columns else None

    if file_type.is_parquet():
        return ingest_api.IngestOptions(
            parquet=ingest_api.ParquetOpts(
                source=source,
                target=target,
                timestamp_metadata=timestamp_metadata,
                tag_columns=tag_columns,
                is_archive=file_type.is_parquet_archive(),
                additional_file_tags={**tags} if tags else None,
            )
        )
    else:
        if not file_type.is_csv():
            logger.warning("Expected filetype %s to be parquet or csv for creating a dataset from io", file_type)

        return ingest_api.IngestOptions(
            csv=ingest_api.CsvOpts(
                source=source,
                target=target,
                timestamp_metadata=timestamp_metadata,
                tag_columns=tag_columns,
                additional_file_tags={**tags} if tags else None,
            )
        )
