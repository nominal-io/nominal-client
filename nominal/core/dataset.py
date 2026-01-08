from __future__ import annotations

import abc
import logging
from dataclasses import dataclass
from datetime import timedelta
from io import TextIOBase
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Mapping, Sequence, TypeAlias, overload

from nominal_api import api, ingest_api, scout_asset_api, scout_catalog
from typing_extensions import Self, deprecated

from nominal.core._stream.batch_processor import process_log_batch
from nominal.core._stream.write_stream import LogStream, WriteStream
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import RefreshableMixin
from nominal.core._utils.multipart import path_upload_name, upload_multipart_file, upload_multipart_io
from nominal.core.bounds import Bounds
from nominal.core.containerized_extractors import ContainerizedExtractor
from nominal.core.dataset_file import DatasetFile
from nominal.core.datasource import DataSource
from nominal.core.exceptions import NominalIngestError, NominalIngestMultiError, NominalMethodRemovedError
from nominal.core.filetype import FileType, FileTypes
from nominal.core.log import LogPoint, _write_logs
from nominal.ts import (
    _AnyTimestampType,
    _to_typed_timestamp_type,
)

logger = logging.getLogger(__name__)

DatasetBounds: TypeAlias = Bounds


@dataclass(frozen=True)
class Dataset(DataSource, RefreshableMixin[scout_catalog.EnrichedDataset]):
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    bounds: DatasetBounds | None

    @property
    def nominal_url(self) -> str:
        """Returns a URL to the page in the nominal app containing this dataset"""
        return f"{self._clients.app_base_url}/data-sources/{self.rid}"

    def _get_latest_api(self) -> scout_catalog.EnrichedDataset:
        return _get_dataset(self._clients.auth_header, self._clients.catalog, self.rid)

    @deprecated(
        "Calling `poll_until_ingestion_completed()` on a `nominal.Dataset` is deprecated and will be removed in "
        "a future release. Poll for ingestion completion instead on individual `nominal.DatasetFile`s, which are "
        "obtained when ingesting files or by calling `dataset.list_files()`."
    )
    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> Self:
        raise NominalMethodRemovedError(
            "nominal.core.Dataset.poll_until_ingestion_completed",
            "poll for ingestion completion on individual 'nominal.core.DatasetFile's, "
            "which are obtained when ingesting files or by calling "
            "'nominal.core.Dataset.list_files()' etc.",
        )

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
        updated_dataset = self._clients.catalog.update_dataset_metadata(self._clients.auth_header, self.rid, request)
        return self._refresh_from_api(updated_dataset)

    def _handle_ingest_response(self, response: ingest_api.IngestResponse) -> DatasetFile:
        if response.details.dataset is None:
            raise ValueError(f"Expected response to provide dataset details, received: {response.details.type}")

        if response.details.dataset.dataset_file_id is None:
            raise ValueError(
                f"Unexpected multi-file ingest response type for dataset {response.details.dataset.dataset_rid}"
            )

        return DatasetFile._from_conjure(
            self._clients,
            self._clients.catalog.get_dataset_file(
                self._clients.auth_header,
                response.details.dataset.dataset_rid,
                response.details.dataset.dataset_file_id,
            ),
        )

    def add_tabular_data(
        self,
        path: PathLike,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
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
            return self.add_from_io(
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
    ) -> DatasetFile:
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
        resp = self._clients.ingest.ingest(self._clients.auth_header, request)
        return self._handle_ingest_response(resp)

    # Backward compatibility
    add_to_dataset_from_io = add_from_io

    def add_avro_stream(
        self,
        path: PathLike,
    ) -> DatasetFile:
        """Upload an avro stream file with a specific schema, described below.

        This is a "stream-like" file format to support
        use cases where a columnar/tabular format does not make sense. This closely matches Nominal's streaming
        API, making it useful for use cases where network connection drops during streaming and a backup file needs
        to be created.

        If this schema is not used, will result in a failed ingestion.
        {
            "type": "record",
            "name": "AvroStream",
            "namespace": "io.nominal.ingest",
            "fields": [
                {
                    "name": "channel",
                    "type": "string",
                    "doc": "Channel/series name (e.g., 'vehicle_id', 'col_1', 'temperature')",
                },
                {
                    "name": "timestamps",
                    "type": {"type": "array", "items": "long"},
                    "doc": "Array of Unix timestamps in nanoseconds",
                },
                {
                    "name": "values",
                    "type": {"type": "array", "items": ["double", "string"]},
                    "doc": "Array of values. Can either be doubles or strings",
                },
                {
                    "name": "tags",
                    "type": {"type": "map", "values": "string"},
                    "default": {},
                    "doc": "Key-value metadata tags",
                },
            ],
        }

        Args:
            path: Path to the .avro file to upload

        Returns:
            Reference to the ingesting DatasetFile

        """
        avro_path = Path(path)
        s3_path = upload_multipart_file(
            self._clients.auth_header,
            self._clients.workspace_rid,
            avro_path,
            self._clients.upload,
            file_type=FileTypes.AVRO_STREAM,
        )
        target = ingest_api.DatasetIngestTarget(
            existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
        )
        resp = self._clients.ingest.ingest(
            self._clients.auth_header,
            ingest_api.IngestRequest(
                options=ingest_api.IngestOptions(
                    avro_stream=ingest_api.AvroStreamOpts(
                        source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)),
                        target=target,
                    )
                )
            ),
        )
        return self._handle_ingest_response(resp)

    def add_journal_json(
        self,
        path: PathLike,
    ) -> DatasetFile:
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
        resp = self._clients.ingest.ingest(
            self._clients.auth_header,
            ingest_api.IngestRequest(
                options=ingest_api.IngestOptions(
                    journal_json=ingest_api.JournalJsonOpts(
                        source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)), target=target
                    )
                )
            ),
        )
        return self._handle_ingest_response(resp)

    # Backward compatibility
    add_journal_json_to_dataset = add_journal_json

    def add_mcap(
        self,
        path: PathLike,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
    ) -> DatasetFile:
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
            return self.add_mcap_from_io(
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
    ) -> DatasetFile:
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
        return self._handle_ingest_response(resp)

    # Backward compatibility
    add_mcap_to_dataset_from_io = add_mcap_from_io

    def add_ardupilot_dataflash(
        self,
        path: PathLike,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Add a Dataflash file to an existing dataset.

        Args:
            path: Path to the Dataflash file to add to this dataset.
            tags: key-value pairs to apply as tags to all data uniformly in the file.
        """
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
        request = _create_dataflash_ingest_request(s3_path, target, tags)
        resp = self._clients.ingest.ingest(self._clients.auth_header, request)
        return self._handle_ingest_response(resp)

    # Backward compatibility
    add_ardupilot_dataflash_to_dataset = add_ardupilot_dataflash

    @overload
    def add_containerized(
        self,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        tag: str | None = None,
        *,
        arguments: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile: ...
    @overload
    def add_containerized(
        self,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        tag: str | None = None,
        *,
        arguments: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
    ) -> DatasetFile: ...
    def add_containerized(
        self,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        tag: str | None = None,
        *,
        arguments: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyTimestampType | None = None,
    ) -> DatasetFile:
        """Add data from proprietary data formats using a pre-registered custom extractor.

        Args:
            extractor: ContainerizedExtractor instance (or rid of one) to use for extracting and ingesting data.
            sources: Mapping of environment variables to source files to use with the extractor.
                NOTE: these must match the registered inputs of the containerized extractor exactly
            tag: Tag of the Docker container which hosts the extractor.
                NOTE: if not provided, the default registered docker tag will be used.
            arguments: Mapping of key-value pairs of input arguments to the extractor.
            tags: Key-value pairs of tags to apply to all data ingested from the containerized extractor run.
            timestamp_column: the column in the dataset that contains the timestamp data.
                NOTE: this is applied uniformly to all output files
                NOTE: must be provided with a `timestamp_type` or a ValueError will be raised
            timestamp_type: the type of timestamp data in the dataset.
                NOTE: this is applied uniformly to all output files
                NOTE: must be provided with a `timestamp_column` or a ValueError will be raised
        """
        timestamp_metadata = None
        if timestamp_column is not None and timestamp_type is not None:
            timestamp_metadata = ingest_api.TimestampMetadata(
                series_name=timestamp_column,
                timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
            )
        elif (timestamp_column is None) != (timestamp_type is None):
            raise ValueError("Only one of `timestamp_column` and `timestamp_type` provided!")

        if isinstance(extractor, str):
            extractor = ContainerizedExtractor._from_conjure(
                self._clients,
                self._clients.containerized_extractors.get_containerized_extractor(
                    self._clients.auth_header, extractor
                ),
            )
        # Ensure all required inputs are present
        registered_inputs = set()
        for extractor_input in extractor.inputs:
            registered_inputs.add(extractor_input.environment_variable)
            if extractor_input.required and extractor_input.environment_variable not in sources:
                raise ValueError(f"Required input '{extractor_input.environment_variable}' not present in sources!")

        # Upload all inputs to s3 before ingestion
        s3_inputs = {}
        for source, source_path in sources.items():
            logger.info("Uploading %s (%s) to s3", source_path, source)
            s3_path = upload_multipart_file(
                self._clients.auth_header,
                self._clients.workspace_rid,
                Path(source_path),
                self._clients.upload,
            )
            logger.info("Uploaded %s -> %s", source_path, s3_path)
            s3_inputs[source] = s3_path

        logger.info("Triggering custom extractor %s (tag=%s) with %s", extractor.name, tag, s3_inputs)
        resp = self._clients.ingest.ingest(
            self._clients.auth_header,
            trigger_ingest=ingest_api.IngestRequest(
                options=ingest_api.IngestOptions(
                    containerized=ingest_api.ContainerizedOpts(
                        arguments={**(arguments or {})},
                        extractor_rid=extractor.rid,
                        sources={
                            source: ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path))
                            for source, s3_path in s3_inputs.items()
                        },
                        target=ingest_api.DatasetIngestTarget(
                            existing=ingest_api.ExistingDatasetIngestDestination(self.rid)
                        ),
                        tag=tag,
                        additional_file_tags={**(tags or {})},
                        timestamp_metadata=timestamp_metadata,
                    )
                )
            ),
        )

        return self._handle_ingest_response(resp)

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

    def get_dataset_file(self, dataset_file_id: str) -> DatasetFile:
        """Retrieve the given dataset file by ID

        Args:
            dataset_file_id: ID of the file to retrieve from the dataset

        Returns:
            Metadata for the requested dataset file

        Raises:
            FileNotFoundError: Details about the requested file could not be found
        """
        try:
            raw_file = self._clients.catalog.get_dataset_file(self._clients.auth_header, self.rid, dataset_file_id)
            return DatasetFile._from_conjure(self._clients, raw_file)
        except Exception as ex:
            raise FileNotFoundError(
                f"Failed to retrieve dataset file {dataset_file_id} from dataset {self.rid}"
            ) from ex

    def get_log_stream(
        self,
        batch_size: int = 50_000,
        max_wait: timedelta = timedelta(seconds=1),
    ) -> LogStream:
        """Stream to asynchronously write log data to a dataset.

        Args:
            batch_size: Number of records to upload at a time to Nominal.
                NOTE: Raising this may improve performance in high latency scenarios
            max_wait: Maximum number of seconds to allow data to be locally buffered
                before streaming to Nominal.

        Returns:
            Write stream object configured to send logs to nominal. This may be used as a context manager
            (so that resources are automatically released upon exiting the context), or if not used as a context
            manager, should be explicitly `close()`-ed once no longer needed.
        """
        return WriteStream.create(
            batch_size=batch_size,
            max_wait=max_wait,
            process_batch=lambda batch: process_log_batch(
                batch, self.rid, auth_header=self._clients.auth_header, storage_writer=self._clients.storage_writer
            ),
        )

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


def _unify_tags(datascope_tags: Mapping[str, str], provided_tags: Mapping[str, str] | None) -> Mapping[str, str]:
    return {**datascope_tags, **(provided_tags or {})}


class _DatasetWrapper(abc.ABC):
    """A lightweight façade over `nominal.core.Dataset` that routes ingest calls through a *data scope*.

    `_DatasetWrapper` resolves `data_scope_name` to a backing `nominal.core.Dataset` and then delegates to the
    corresponding `Dataset` method.

    How this differs from `Dataset`
    -------------------------------
    - All "add data" methods take an extra first argument, `data_scope_name`, which selects the target dataset.
    - For methods that accept `tags`, this wrapper merges the scope's required tags into the provided tags.
      User-provided tags take precedence on key collisions.
    - Some formats cannot be safely tagged with scope tags; those wrapper methods raise `RuntimeError` when the selected
      scope requires tags.

    Subclasses must implement `_list_dataset_scopes`, which is used to resolve scopes.
    """

    # static typing for required field
    _clients: Dataset._Clients

    @abc.abstractmethod
    def _list_dataset_scopes(self) -> Sequence[scout_asset_api.DataScope]:
        """Return the data scopes available to this wrapper.

        Subclasses provide the authoritative list of `scout_asset_api.DataScope` objects used to
        resolve `data_scope_name` in wrapper methods.
        """

    def _get_dataset_scope(self, data_scope_name: str) -> tuple[Dataset, Mapping[str, str]]:
        """Resolve a data scope name to its backing dataset and required series tags.

        Returns:
            A tuple of the resolved `Dataset` and the scope's required `series_tags`.

        Raises:
            ValueError: If no scope exists with the given `data_scope_name`, or if the scope is not backed by a dataset.
        """
        dataset_scopes = {scope.data_scope_name: scope for scope in self._list_dataset_scopes()}
        data_scope = dataset_scopes.get(data_scope_name)
        if data_scope is None:
            raise ValueError(f"No such data scope found with data_scope_name {data_scope_name}")
        elif data_scope.data_source.dataset is None:
            raise ValueError(f"Datascope {data_scope_name} is not a dataset!")

        dataset = Dataset._from_conjure(
            self._clients,
            _get_dataset(self._clients.auth_header, self._clients.catalog, data_scope.data_source.dataset),
        )
        return dataset, data_scope.series_tags

    ################
    # Add Data API #
    ################

    def add_tabular_data(
        self,
        data_scope_name: str,
        path: PathLike,
        *,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Append tabular data on-disk to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_tabular_data`, except that the data scope's required
        tags are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).

        For supported file types, argument semantics, and return value details, see
        `nominal.core.Dataset.add_tabular_data`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_tabular_data(
            path,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            tag_columns=tag_columns,
            tags=_unify_tags(scope_tags, tags),
        )

    def add_avro_stream(
        self,
        data_scope_name: str,
        path: PathLike,
    ) -> DatasetFile:
        """Upload an avro stream file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_avro_stream`, with one important difference:
        avro stream ingestion does not support applying scope tags. If the selected scope requires tags, this method
        raises `RuntimeError` rather than ingesting (potentially) untagged data. This file may still be ingested
        directly on the dataset itself if it is known to contain the correct set of tags.

        For schema requirements and return value details, see
        `nominal.core.Dataset.add_avro_stream`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)

        # TODO(drake): remove once avro stream supports ingest with tags
        if scope_tags:
            raise RuntimeError(
                f"Cannot add avro files to datascope {data_scope_name}-- data would not get "
                f"tagged with required tags: {scope_tags}"
            )

        return dataset.add_avro_stream(path)

    def add_journal_json(
        self,
        data_scope_name: str,
        path: PathLike,
    ) -> DatasetFile:
        """Add a journald json file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_journal_json`, with one important difference:
        journal json ingestion does not support applying scope tags as args. If the selected scope requires tags,
        this method raises `RuntimeError` rather than potentially ingesting untagged data. This file may still be
        ingested directly on the dataset itself if it is known to contain the correct set of args.

        For file expectations and return value details, see
        `nominal.core.Dataset.add_journal_json`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)

        # TODO(drake): remove once journal json supports ingest with tags
        if scope_tags:
            raise RuntimeError(
                f"Cannot add journal json files to datascope {data_scope_name}-- data would not get "
                f"tagged with required arguments: {scope_tags}"
            )

        return dataset.add_journal_json(path)

    def add_mcap(
        self,
        data_scope_name: str,
        path: PathLike,
        *,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
    ) -> DatasetFile:
        """Add an MCAP file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_mcap`, with one important difference:
        MCAP ingestion does not support applying scope tags. If the selected scope requires tags, this method raises
        `RuntimeError` rather than ingesting untagged data.

        For topic-filtering semantics and return value details, see
        `nominal.core.Dataset.add_mcap`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)

        # TODO(drake): remove once MCAP supports ingest with tags
        if scope_tags:
            raise RuntimeError(
                f"Cannot add mcap files to datascope {data_scope_name}-- data would not get "
                f"tagged with required tags: {scope_tags}"
            )

        return dataset.add_mcap(path, include_topics=include_topics, exclude_topics=exclude_topics)

    def add_ardupilot_dataflash(
        self,
        data_scope_name: str,
        path: PathLike,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Add a Dataflash file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_ardupilot_dataflash`, except that the data scope's
        required tags are merged into `tags` before ingest (with user-provided tags taking precedence on key
        collisions).

        For file expectations and return value details, see
        `nominal.core.Dataset.add_ardupilot_dataflash`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_ardupilot_dataflash(path, tags=_unify_tags(scope_tags, tags))

    @overload
    def add_containerized(
        self,
        data_scope_name: str,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        tag: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile: ...
    @overload
    def add_containerized(
        self,
        data_scope_name: str,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        tag: str | None = None,
        tags: Mapping[str, str] | None = None,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
    ) -> DatasetFile: ...
    def add_containerized(
        self,
        data_scope_name: str,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        tag: str | None = None,
        tags: Mapping[str, str] | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyTimestampType | None = None,
    ) -> DatasetFile:
        """Add data from proprietary formats using a pre-registered custom extractor.

        This method behaves like `nominal.core.Dataset.add_containerized`, except that the data scope's required
        tags are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).

        This wrapper also enforces that `timestamp_column` and `timestamp_type` are provided together (or omitted
        together) before delegating.

        For extractor inputs, tagging semantics, timestamp metadata behavior, and return value details, see
        `nominal.core.Dataset.add_containerized`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        if timestamp_column is None and timestamp_type is None:
            return dataset.add_containerized(
                extractor,
                sources,
                tag=tag,
                tags=_unify_tags(scope_tags, tags),
            )
        elif timestamp_column is not None and timestamp_type is not None:
            return dataset.add_containerized(
                extractor,
                sources,
                tag=tag,
                tags=_unify_tags(scope_tags, tags),
                timestamp_column=timestamp_column,
                timestamp_type=timestamp_type,
            )
        else:
            raise ValueError(
                "Only one of `timestamp_column` and `timestamp_type` were provided to `add_containerized`, "
                "either both must or neither must be provided."
            )

    def add_from_io(
        self,
        data_scope_name: str,
        data_stream: BinaryIO,
        file_type: tuple[str, str] | FileType,
        *,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        file_name: str | None = None,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Append to the dataset selected by `data_scope_name` from a file-like object.

        This method behaves like `nominal.core.Dataset.add_from_io`, except that the data scope's required tags
        are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).

        For stream requirements, supported file types, argument semantics, and return value details, see
        `nominal.core.Dataset.add_from_io`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_from_io(
            data_stream,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=file_type,
            file_name=file_name,
            tag_columns=tag_columns,
            tags=_unify_tags(scope_tags, tags),
        )


@deprecated(
    "poll_until_ingestion_completed() is deprecated and will be removed in a future release. "
    "Instead, call poll_until_ingestion_completed() on individual DatasetFiles."
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
            for dataset_file in dataset.list_files():
                dataset_file.poll_until_ingestion_completed(interval=interval)
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
        marking_rids=[],
    )
    return client.create_dataset(auth_header, request)


def _create_dataflash_ingest_request(
    s3_path: str,
    target: ingest_api.DatasetIngestTarget,
    tags: Mapping[str, str] | None = None,
) -> ingest_api.IngestRequest:
    return ingest_api.IngestRequest(
        ingest_api.IngestOptions(
            dataflash=ingest_api.DataflashOpts(
                source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                target=target,
                additional_file_tags={**tags} if tags else None,
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
            marking_rids=[],
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
