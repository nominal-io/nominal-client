from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import BinaryIO, Iterable, Literal, Mapping, Sequence, TextIO

from conjure_python_client import RequestsClient, ServiceConfiguration

from ._api.combined import attachments_api
from ._api.combined import scout
from ._api.combined import scout_run_api
from ._api.ingest import ingest_api
from ._api.ingest import upload_api

IntegralNanosecondsUTC = int
_AllowedFileExtensions = Literal[".csv", ".csv.gz", ".parquet"]


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


def _timestamp_type_to_conjure_ingest_api(
    ts_type: _TimestampColumnType,
) -> ingest_api.TimestampType:
    if isinstance(ts_type, CustomTimestampFormat):
        return ingest_api.TimestampType(
            absolute=ingest_api.AbsoluteTimestamp(
                custom_format=ingest_api.CustomTimestamp(format=ts_type.format, default_year=ts_type.default_year)
            )
        )
    elif ts_type == "iso_8601":
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(iso8601=ingest_api.Iso8601Timestamp()))
    relation, unit = ts_type.split("_", 1)
    time_unit = ingest_api.TimeUnit[unit.upper()]
    if relation == "epoch":
        return ingest_api.TimestampType(
            absolute=ingest_api.AbsoluteTimestamp(epoch_of_time_unit=ingest_api.EpochTimestamp(time_unit=time_unit))
        )
    elif relation == "relative":
        return ingest_api.TimestampType(relative=ingest_api.RelativeTimestamp(time_unit=time_unit))
    raise ValueError(f"invalid timestamp type: {ts_type}")


@dataclass(frozen=True)
class Run:
    rid: str
    title: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
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
    def _from_conjure_scout_run_api(cls, client: NominalClient, run: scout_run_api.Run) -> Run:
        return cls(
            rid=run.rid,
            title=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_conjure_time_to_integral_nanoseconds(run.start_time),
            end=(_conjure_time_to_integral_nanoseconds(run.end_time) if run.end_time else None),
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

    # @classmethod
    # def _from_conjure...(cls, client: NominalClient, ds: scout_catalog.Dataset) -> Dataset:
    #     return cls(
    #         rid=ds.rid,
    #         name=ds.name,
    #         description=ds.description,
    #         properties=MappingProxyType(ds.properties),
    #         labels=tuple(ds.labels),
    #         _client=client,
    #     )


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
    def _from_conjure(cls, client: NominalClient, attachment: attachments_api.Attachment) -> Attachment:
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
    _ingest_client: ingest_api.IngestService

    @classmethod
    def create(cls, base_url: str, token: str) -> NominalClient:
        cfg = ServiceConfiguration(uris=[base_url])
        # TODO: add library version to user agent
        agent = "nominal-python"
        run_client = RequestsClient.create(scout.RunService, agent, cfg)
        upload_client = RequestsClient.create(upload_api.UploadService, agent, cfg)
        ingest_client = RequestsClient.create(ingest_api.IngestService, agent, cfg)
        auth_header = f"Bearer {token}"
        return cls(
            _auth_header=auth_header,
            _run_client=run_client,
            _upload_client=upload_client,
            _ingest_client=ingest_client,
        )

    def create_run(
        self,
        title: str,
        start_time: datetime | IntegralNanosecondsUTC,
        description: str = "",
        datasets: Mapping[str, str] | None = None,
        end_time: datetime | IntegralNanosecondsUTC | None = None,
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
        start_abs = _flexible_time_to_conjure_scout_run_api(start_time)
        end_abs = _flexible_time_to_conjure_scout_run_api(end_time) if end_time else None
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
            start_time=start_abs,
            title=title,
            end_time=end_abs,
        )
        response = self._run_client.create_run(self._auth_header, request)
        response.rid
        return Run._from_conjure_scout_run_api(self, response)

    def get_run(self, run_rid: str) -> Run:
        response = self._run_client.get_run(self._auth_header, run_rid)
        return Run._from_conjure_scout_run_api(self, response)

    def _list_runs_paginated(self, request: scout_run_api.SearchRunsRequest) -> Iterable[scout_run_api.Run]:
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
        return [Run._from_conjure_scout_run_api(self, run) for run in self._list_runs_paginated(request)]

    def create_dataset_from_io(
        self,
        name: str,
        csvfile: TextIO | BinaryIO,
        timestamp_column_name: str,
        timestamp_column_type: _TimestampColumnType,
        file_extension: _AllowedFileExtensions = ".csv",
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> str:
        s3_path = self._upload_client.upload_file(self._auth_header, csvfile, file_name=f"{name}{file_extension}")
        request = ingest_api.TriggerIngest(
            labels=list(labels),
            properties=dict(properties or {}),
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            dataset_description=description,
            dataset_name=name,
            timestamp_metadata=ingest_api.TimestampMetadata(
                series_name=timestamp_column_name,
                timestamp_type=_timestamp_type_to_conjure_ingest_api(timestamp_column_type),
            ),
        )
        response = self._ingest_client.trigger_ingest(self._auth_header, request)
        return response.dataset_rid

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


def _flexible_time_to_conjure_scout_run_api(
    timestamp: datetime | IntegralNanosecondsUTC,
) -> scout_run_api.UtcTimestamp:
    if isinstance(timestamp, datetime):
        seconds, nanos = _datetime_to_seconds_nanos(timestamp)
        return scout_run_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)
    elif isinstance(timestamp, IntegralNanosecondsUTC):
        seconds, nanos = divmod(timestamp, 1_000_000_000)
        return scout_run_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)
    raise TypeError(f"expected {datetime} or {IntegralNanosecondsUTC}, got {type(timestamp)}")


def _conjure_time_to_integral_nanoseconds(
    ts: scout_run_api.UtcTimestamp,
) -> IntegralNanosecondsUTC:
    return ts.seconds_since_epoch * 1_000_000_000 + (ts.offset_nanoseconds or 0)


def _datetime_to_seconds_nanos(dt: datetime) -> tuple[int, int]:
    dt = dt.astimezone(timezone.utc)
    seconds = int(dt.timestamp())
    nanos = dt.microsecond * 1000
    return seconds, nanos
