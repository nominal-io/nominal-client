from __future__ import annotations

import logging
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import timedelta
from io import TextIOBase
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Iterable, Mapping, Sequence

from typing_extensions import Self

from .._api.combined import datasource_api, ingest_api, scout_catalog, timeseries_logicalseries_api
from .._utils import FileType, FileTypes
from ..exceptions import NominalIngestError, NominalIngestFailed, NominalIngestMultiError
from ..ts import _AnyTimestampType, _to_typed_timestamp_type
from ._clientsbunch import ClientsBunch
from ._conjure_utils import _available_units, _build_unit_update
from ._multipart import put_multipart_upload
from ._utils import HasRid, update_dataclass
from .channel import Channel

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Dataset(HasRid):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _clients: ClientsBunch = field(repr=False)

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until dataset ingestion has completed.
        This method polls Nominal for ingest status after uploading a dataset on an interval.

        Raises:
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known
        """

        while True:
            progress = self._clients.catalog.get_ingest_progress_v2(self._clients.auth_header, self.rid)
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
        response = self._clients.catalog.update_dataset_metadata(self._clients.auth_header, self.rid, request)

        dataset = self.__class__._from_conjure(self._clients, response)
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
            self._clients.auth_header, dataset, filename, file_type.mimetype, self._clients.upload
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
        self._clients.ingest.trigger_file_ingest(self._clients.auth_header, request)

    def get_channels(
        self,
        exact_match: Sequence[str] = (),
        fuzzy_search_text: str = "",
    ) -> Iterable[Channel]:
        """Look up the metadata for all matching channels associated with this dataset.
        NOTE: Provided channels may also be associated with other datasets-- use with caution.
        Args:
            exact_match: Filter the returned channels to those whose names match all provided strings (case insensitive).
                For example, a channel named 'engine_turbine_rpm' would match against ['engine', 'turbine', 'rpm'],
                whereas a channel named 'engine_turbine_flowrate' would not!
            fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided string.
        Yields:
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
                yield Channel._from_conjure(channel_metadata)

            if response.next_page_token is None:
                break
            else:
                next_page_token = response.next_page_token

    def set_channel_units(self, channels_to_units: Mapping[str, str | None], validate_schema: bool = False) -> None:
        """Set units for channels based on a provided mapping of channel names to units.
        Args:
            channels_to_units: A mapping of channel names to unit symbols.
                NOTE: any existing units may be cleared from a channel by providing None as a symbol.
            validate_schema: If true, raise a ValueError if non-existant channel names are provided in `channels_to_units`
        Raises:
            ValueError: Unsupported unit symbol provided
            conjure_python_client.ConjureHTTPError: Error completing requests.
        """

        # Get the set of all available unit symbols
        # NOTE: weird lambda to escape mypy type validation
        all_units = sorted(_available_units(self._clients), key=lambda unit: unit.name if unit.name else "")
        supported_symbols = set([unit.symbol for unit in all_units])

        # Validate that all user provided unit symbols are valid
        for channel_name, unit_symbol in channels_to_units.items():
            # User is clearing the unit for this channel-- don't validate
            if unit_symbol is None:
                continue

            if unit_symbol not in supported_symbols:
                raise ValueError(
                    f"Provided unit '{unit_symbol}' for channel '{channel_name}' does not resolve to a unit recognized by nominal."
                    "For more information on valid symbols, see https://ucum.org/ucum"
                )

        # Get metadata (specifically, RIDs) for all requested channels
        found_channels = {channel.name: channel for channel in self.get_channels() if channel.name in channels_to_units}

        # For each channel / unit combination, create an update request to set the series's unit
        # to that symbol
        update_requests = []
        for channel_name, unit in channels_to_units.items():
            # No data uploaded to channel yet ...
            if channel_name not in found_channels:
                if validate_schema:
                    raise ValueError(
                        f"Unable to set unit for {channel_name} to {unit_symbol}: no data uploaded for channel"
                    )
                else:
                    logger.info("Not setting unit for channel %s: no data uploaded for channel", channel_name)
                    continue

            channel = found_channels[channel_name]
            channel_request = timeseries_logicalseries_api.UpdateLogicalSeries(
                logical_series_rid=channel.rid,
                unit_update=_build_unit_update(unit),
            )
            update_requests.append(channel_request)

        # Set units in database
        request = timeseries_logicalseries_api.BatchUpdateLogicalSeriesRequest(update_requests)
        self._clients.logical_series.batch_update_logical_series(self._clients.auth_header, request)

    @classmethod
    def _from_conjure(cls, clients: ClientsBunch, dataset: scout_catalog.EnrichedDataset) -> Self:
        return cls(
            rid=dataset.rid,
            name=dataset.name,
            description=dataset.description,
            properties=MappingProxyType(dataset.properties),
            labels=tuple(dataset.labels),
            _clients=clients,
        )


def _verify_csv_path(path: Path | str) -> tuple[Path, FileType]:
    path = Path(path)
    file_type = FileType.from_path_dataset(path)
    if file_type.extension not in (".csv", ".csv.gz"):
        raise ValueError(f"file {path} must end with '.csv' or '.csv.gz'")
    return path, file_type


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
