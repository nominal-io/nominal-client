from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import BinaryIO, Iterable, Literal, Mapping, Sequence, TextIO

from conjure_python_client import RequestsClient, ServiceConfiguration

from ._api.combined import attachments_api
from ._api.combined import scout
from ._api.combined import scout_catalog
from ._api.combined import scout_run_api
from ._api.ingest import upload_api

IntegralNanosecondsUTC = int
IntegralNanosecondsRelative = int


class AbsoluteTimestamp:
    def __init__(self, epoch_nanoseconds: IntegralNanosecondsUTC) -> None:
        self.epoch_nanoseconds = epoch_nanoseconds

    def to_datetime(self) -> datetime:
        """Convert to a Python datetime object.

        Note: Python datetimes are only microsecond-precise, so this may truncate precision.
        """
        seconds, nanos = self.to_seconds_nanos()
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
            microsecond=nanos // 1000
        )

    def to_seconds_nanos(self) -> tuple[int, int]:
        return divmod(self.epoch_nanoseconds, 1_000_000_000)

    @classmethod
    def from_datetime(cls, dt: datetime) -> AbsoluteTimestamp:
        dt = dt.astimezone(timezone.utc)
        seconds = int(dt.timestamp())
        nanos = dt.microsecond * 1000
        return cls.from_seconds_nanos(seconds, nanos)

    @classmethod
    def from_seconds_nanos(cls, seconds: int, nanos: int) -> AbsoluteTimestamp:
        return cls(epoch_nanoseconds=seconds * 1_000_000_000 + nanos)

    def _to_conjure(self) -> scout_run_api.UtcTimestamp:
        seconds, nanos = self.to_seconds_nanos()
        return scout_run_api.UtcTimestamp(
            seconds_since_epoch=seconds, offset_nanoseconds=nanos
        )

    @classmethod
    def _from_conjure(cls, timestamp: scout_run_api.UtcTimestamp) -> AbsoluteTimestamp:
        return cls.from_seconds_nanos(
            timestamp.seconds_since_epoch, timestamp.offset_nanoseconds or 0
        )

    @classmethod
    def _from_flexible(
        cls, t: datetime | IntegralNanosecondsUTC | AbsoluteTimestamp
    ) -> AbsoluteTimestamp:
        if isinstance(t, AbsoluteTimestamp):
            return t
        elif isinstance(t, datetime):
            return cls.from_datetime(t)
        elif isinstance(t, IntegralNanosecondsUTC):
            return cls(t)
        raise TypeError(
            f"expected datetime, IntegralNanosecondsUTC (int), or AbsoluteTimestamp, got {type(t)}"
        )


@dataclass
class CustomTimestampFormat:
    format: str
    default_year: int = 0


_TimestampColumnType = (
    Literal[
        "iso_8601",
        "epoch_days",
        "epoch_hours",
        "epoch_minutes",
        "epoch_seconds",
        "epoch_milliseconds",
        "epoch_microseconds",
        "epoch_nanoseconds",
        "relative_days",
        "relative_hours",
        "relative_minutes",
        "relative_seconds",
        "relative_milliseconds",
        "relative_microseconds",
        "relative_nanoseconds",
    ]
    | CustomTimestampFormat
)


def _timestamp_type_to_conjure(
    ts_type: _TimestampColumnType,
) -> scout_catalog.TimestampType:
    if isinstance(ts_type, CustomTimestampFormat):
        return scout_catalog.TimestampType(
            absolute=scout_catalog.AbsoluteTimestamp(
                custom_format=scout_catalog.CustomTimestamp(
                    format=ts_type.format, default_year=ts_type.default_year
                )
            )
        )
    elif ts_type == "iso_8601":
        return scout_catalog.TimestampType(
            absolute=scout_catalog.AbsoluteTimestamp(
                iso8601=scout_catalog.Iso8601Timestamp()
            )
        )
    relation, unit = ts_type.split("_", 1)
    time_unit = scout_catalog.TimeUnit[unit.upper()]
    if relation == "epoch":
        return scout_catalog.TimestampType(
            absolute=scout_catalog.AbsoluteTimestamp(
                epoch_of_time_unit=scout_catalog.EpochTimestamp(time_unit=time_unit)
            )
        )
    elif relation == "relative":
        return scout_catalog.TimestampType(
            relative=scout_catalog.RelativeTimestamp(time_unit=time_unit)
        )
    raise ValueError(f"invalid timestamp type: {ts_type}")


@dataclass(frozen=True)
class Run:
    rid: str
    title: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    start: AbsoluteTimestamp
    end: AbsoluteTimestamp | None
    _client: NominalClient

    def add_dataset(self) -> None:
        raise NotImplementedError()

    def create_dataset(self) -> Dataset:
        raise NotImplementedError()

    def list_datasets(self) -> list[Dataset]:
        raise NotImplementedError()

    def add_attachment(self) -> None:
        raise NotImplementedError()

    def create_attachment(self) -> Dataset:
        raise NotImplementedError()

    def list_attachments(self) -> list[Attachment]:
        raise NotImplementedError()

    def replace(
        self,
        *,
        title: str | None,
        description: str | None,
        properties: Mapping[str, str] | None,
        labels: Sequence[str] | None,
    ) -> None:
        raise NotImplementedError()

    @classmethod
    def _from_conjure(cls, client: NominalClient, run: scout_run_api.Run) -> Run:
        return cls(
            rid=run.rid,
            title=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=AbsoluteTimestamp._from_conjure(run.start_time),
            end=AbsoluteTimestamp._from_conjure(run.end_time) if run.end_time else None,
            _client=client,
        )


@dataclass(frozen=True)
class Dataset:
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient

    def replace(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> None:
        raise NotImplementedError()

    @classmethod
    def _from_conjure(
        cls, client: NominalClient, ds: scout_catalog.EnrichedDataset
    ) -> Dataset:
        return cls(
            rid=ds.rid,
            name=ds.name,
            description=ds.description,
            properties=MappingProxyType(ds.properties),
            labels=tuple(ds.labels),
            _client=client,
        )


@dataclass(frozen=True)
class Attachment:
    rid: str
    title: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient

    def replace(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> None:
        raise NotImplementedError()

    @classmethod
    def _from_conjure(
        cls, client: NominalClient, attachment: attachments_api.Attachment
    ) -> Attachment:
        return cls(
            rid=attachment.rid,
            title=attachment.title,
            description=attachment.description,
            properties=MappingProxyType(attachment.properties),
            labels=tuple(attachment.labels),
            _client=client,
        )


@dataclass(frozen=True)
class NominalClient:
    _auth_header: str
    _run_client: scout.RunService
    _upload_client: upload_api.UploadService
    _catalog_client: scout_catalog.CatalogService

    @classmethod
    def create(cls, base_url: str, token: str) -> NominalClient:
        cfg = ServiceConfiguration(uris=[base_url])
        # TODO: add library version to user agent
        agent = "nominal-python"
        run_client = RequestsClient.create(scout.RunService, agent, cfg)
        upload_client = RequestsClient.create(upload_api.UploadService, agent, cfg)
        catalog_client = RequestsClient.create(scout_catalog.CatalogService, agent, cfg)
        auth_header = f"Bearer {token}"
        return cls(
            _auth_header=auth_header,
            _run_client=run_client,
            _upload_client=upload_client,
            _catalog_client=catalog_client,
        )

    def create_run(
        self,
        title: str,
        start_time: datetime | IntegralNanosecondsUTC | AbsoluteTimestamp,
        description: str = "",
        datasets: Mapping[str, str] | None = None,
        end_time: datetime | IntegralNanosecondsUTC | AbsoluteTimestamp | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        attachment_rids: Sequence[str] = (),
    ) -> Run:
        """Creates a run in the Nominal platform.

        Datasets map "ref names" (their name within the run) to dataset RIDs.
            The same type of datasets should use the same ref name across runs,
            since checklists and templates use ref names to reference datasets.
            The RIDs are retrieved from creating or getting a `Dataset` object.
        """
        start_abs = AbsoluteTimestamp._from_flexible(start_time)
        end_abs = AbsoluteTimestamp._from_flexible(end_time) if end_time else None
        datasets = datasets or {}
        request = scout_run_api.CreateRunRequest(
            attachments=list(attachment_rids),
            data_sources={
                ref_name: scout_run_api.CreateRunDataSource(
                    data_source=scout_run_api.DataSource(dataset=rid),
                    series_tags={},
                    offset=None,  # TODO: support per-dataset offsets
                )
                for ref_name, rid in datasets.items()
            },
            description=description,
            labels=list(labels),
            links=[],  # TODO: support links
            properties={} if properties is None else dict(properties),
            start_time=start_abs._to_conjure(),
            title=title,
            end_time=end_abs._to_conjure() if end_abs else None,
        )
        response = self._run_client.create_run(self._auth_header, request)
        return Run._from_conjure(self, response)

    def get_run(self, run_rid: str) -> Run:
        response = self._run_client.get_run(self._auth_header, run_rid)
        return Run._from_conjure(self, response)

    def _list_runs_paginated(
        self, request: scout_run_api.SearchRunsRequest
    ) -> Iterable[scout_run_api.Run]:
        while True:
            response = self._run_client.search_runs(self._auth_header, request)
            yield from response.results
            if response.next_page_token is None:
                break
            request = scout_run_api.SearchRunsRequest(
                page_size=request.page_size,
                query=request.query,
                sort=request.sort,
                next_page_token=response.next_page_token,
            )

    def list_runs(self) -> list[Run]:
        # TODO: search filters
        request = scout_run_api.SearchRunsRequest(
            page_size=100,
            query=scout_run_api.SearchQuery(),
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
        )
        return [
            Run._from_conjure(self, run) for run in self._list_runs_paginated(request)
        ]

    def create_dataset(
        self,
        name: str,
        filename: str,
        csvfile: TextIO | BinaryIO | str | bytes,
        timestamp_column_name: str,
        timestamp_column_type: _TimestampColumnType,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Dataset:
        s3_path = self._upload_client.upload_file(
            self._auth_header, csvfile, file_name=filename
        )
        s3_handle = _s3_path_to_conjure(s3_path)
        request = scout_catalog.CreateDataset(
            handle=scout_catalog.Handle(s3=s3_handle),
            labels=list(labels),
            metadata={},  # deprecated over properties
            name=name,
            origin_metadata=scout_catalog.DatasetOriginMetadata(
                timestamp_metadata=scout_catalog.TimestampMetadata(
                    series_name=timestamp_column_name,
                    timestamp_type=_timestamp_type_to_conjure(timestamp_column_type),
                ),
            ),
            properties=dict(properties or {}),
            description=description,
        )
        response = self._catalog_client.create_dataset(self._auth_header, request)
        return Dataset._from_conjure(self, response)

    def get_dataset(self, dataset_rid: str) -> Dataset:
        raise NotImplementedError()

    def list_datasets(self) -> list[Dataset]:
        raise NotImplementedError()

    def create_attachment(self) -> Attachment:
        raise NotImplementedError()

    def get_attachment(self, attachment_rid: str) -> Attachment:
        raise NotImplementedError()

    def list_attachments(self) -> list[Attachment]:
        raise NotImplementedError()


def _s3_path_to_conjure(s3_path: str) -> scout_catalog.S3Handle:
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    return scout_catalog.S3Handle(bucket=bucket, key=key)
