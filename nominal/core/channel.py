from __future__ import annotations

import concurrent.futures
import enum
import functools
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import BinaryIO, Iterable, Iterator, Mapping, Protocol, Self, Sequence, assert_never, overload

from google.protobuf.field_mask_pb2 import FieldMask
from google.protobuf.timestamp_pb2 import Timestamp
from nominal_api import (
    api,
    datasource_api,
    scout_compute_api,
    scout_datasource,
)

from nominal._utils.iterator_tools import batched
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import RefreshableGrpcMixin, build_compute_tag_filter, create_api_tags
from nominal.core._utils.dataexport import export_timestamp_format, stream_export
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import paginate_rpc
from nominal.core.log import LogPoint, _log_filter_operator
from nominal.core.unit import UnitLike, _unit_symbol_from_unit_like
from nominal.protos.channelmetadata.v1 import channel_metadata_pb2, channel_metadata_pb2_grpc
from nominal.protos.compute.v1 import constants_pb2, expressions_pb2, source_selection_pb2
from nominal.protos.dataexports.v1 import data_exports_pb2, data_exports_pb2_grpc
from nominal.protos.seriesmetadata.v1 import series_metadata_pb2
from nominal.protos.types.series import series_pb2
from nominal.ts import (
    _MAX_TIMESTAMP,
    _MIN_TIMESTAMP,
    IntegralNanosecondsUTC,
    _InferrableTimestampType,
    _LiteralTimeUnit,
    _SecondsNanos,
    _to_export_timestamp_type,
)

logger = logging.getLogger(__name__)


class ChannelDataType(enum.Enum):
    """The type of values stored in a channel."""

    DOUBLE = "DOUBLE"
    STRING = "STRING"
    LOG = "LOG"
    INT = "INT"
    UINT = "UINT"
    DOUBLE_ARRAY = "DOUBLE_ARRAY"
    STRING_ARRAY = "STRING_ARRAY"
    STRUCT = "STRUCT"
    VIDEO = "VIDEO"
    UNKNOWN = "UNKNOWN"

    def _to_proto(self) -> series_metadata_pb2.NominalDataType.ValueType:
        match self:
            case ChannelDataType.DOUBLE:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_DOUBLE
            case ChannelDataType.STRING:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_STRING
            case ChannelDataType.LOG:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_LOG
            case ChannelDataType.INT:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_INT64
            case ChannelDataType.UINT:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_UINT64
            case ChannelDataType.DOUBLE_ARRAY:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_DOUBLE_ARRAY
            case ChannelDataType.STRING_ARRAY:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_STRING_ARRAY
            case ChannelDataType.STRUCT:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_STRUCT
            case ChannelDataType.VIDEO:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_VIDEO
            case ChannelDataType.UNKNOWN:
                data_type = series_metadata_pb2.NOMINAL_DATA_TYPE_UNSPECIFIED
            case _:
                assert_never(self)
        return data_type

    @classmethod
    def _from_proto(cls, data_type: series_pb2.SeriesDataType.ValueType) -> ChannelDataType:
        try:
            type_name = series_pb2.SeriesDataType.Name(data_type)
        except ValueError:
            return cls.UNKNOWN
        return cls.__members__.get(type_name, cls.UNKNOWN)

    @classmethod
    def _from_conjure(cls, data_type: api.SeriesDataType) -> Self:
        if data_type.value in cls.__members__:
            return cls(data_type.value)
        else:
            return cls("UNKNOWN")


@dataclass(frozen=True)
class CreateChannelRequest:
    """Channel metadata to create or update through a dataset's batch methods."""

    name: str
    data_type: ChannelDataType
    description: str | None = None
    unit: str | None = None

    def _to_proto(
        self,
        data_source_rid: str,
        *,
        if_exists: series_metadata_pb2.IfSeriesMetadataExists.ValueType,
    ) -> series_metadata_pb2.CreateSeriesMetadataRequest:
        nominal_data_type = self.data_type._to_proto()
        locator = series_metadata_pb2.LocatorTemplate(
            nominal=series_metadata_pb2.NominalLocatorTemplate(channel=self.name, data_type=nominal_data_type)
        )
        return series_metadata_pb2.CreateSeriesMetadataRequest(
            channel=self.name,
            data_source_rid=data_source_rid,
            locator_template=locator,
            description=self.description,
            unit=self.unit,
            if_exists=if_exists,
        )


@dataclass(frozen=True)
class BatchAddChannelsResult:
    """Results of a batch channel create or update, including unresolved requests.

    Requests in ``missing`` were not found by the channel lookup after the write.
    """

    channels: Sequence[Channel]
    missing: Sequence[CreateChannelRequest]


@dataclass
class Channel(RefreshableGrpcMixin[channel_metadata_pb2.ChannelMetadata]):
    """Metadata for working with channels."""

    name: str
    data_source: str
    data_type: ChannelDataType | None
    unit: str | None
    description: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def datasource(self) -> scout_datasource.DataSourceService: ...
        @property
        def grpc_dataexport(self) -> data_exports_pb2_grpc.DataExportServiceStub: ...
        @property
        def compute(self) -> scout_compute_api.ComputeService: ...
        @property
        def channel_metadata(self) -> channel_metadata_pb2_grpc.ChannelMetadataServiceStub: ...

    class _NotProvided:
        """Sentinel class for detecting when a user has or has not provided a value during updates"""

    def _channel_identifier(self) -> channel_metadata_pb2.ChannelIdentifier:
        return channel_metadata_pb2.ChannelIdentifier(
            channel_name=self.name,
            data_source_rid=self.data_source,
        )

    def _get_latest_api(self) -> channel_metadata_pb2.ChannelMetadata:
        request = channel_metadata_pb2.GetChannelMetadataRequest(channel_identifier=self._channel_identifier())
        with translate_grpc_errors():
            return self._clients.channel_metadata.GetChannelMetadata(request).channel_metadata

    def update(
        self,
        *,
        description: str | None = None,
        unit: UnitLike | _NotProvided = _NotProvided(),
    ) -> Self:
        """Replace channel metadata within Nominal, and updates / returns the local instance.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Args:
            description: Human-readable description of data within the channel
            unit: Unit symbol to apply to the channel. If unit is a string or a `Unit`, this will update the unit symbol
                for the channel. If unit is None, this will clear the unit symbol for the channel. If not provided (or
                `_NotProvided`), this will leave the unit unaffected.

                **Note:** This is in contrast to other fields in other `update()` calls where `None` is treated as a
                "no-op".
        """
        metadata = channel_metadata_pb2.ChannelMetadata(channel_identifier=self._channel_identifier())
        paths = []
        if description is not None:
            metadata.description = description
            paths.append("description")
        if not isinstance(unit, self._NotProvided):
            symbol = _unit_symbol_from_unit_like(unit)
            if symbol is not None:
                metadata.unit = symbol
            paths.append("unit")
        if not paths:
            return self.refresh()
        request = channel_metadata_pb2.UpdateChannelMetadataRequest(
            channel_metadata=metadata, update_mask=FieldMask(paths=paths)
        )
        with translate_grpc_errors():
            response = self._clients.channel_metadata.UpdateChannelMetadata(request)
        return self._refresh_from_api(response.channel_metadata)

    @overload
    def search_logs(
        self,
        *,
        tags: Mapping[str, str] | None = None,
        regex_match: str,
        start: _InferrableTimestampType | None = None,
        end: _InferrableTimestampType | None = None,
    ) -> Iterable[LogPoint]: ...

    @overload
    def search_logs(
        self,
        *,
        tags: Mapping[str, str] | None = None,
        insensitive_match: str,
        start: _InferrableTimestampType | None = None,
        end: _InferrableTimestampType | None = None,
    ) -> Iterable[LogPoint]: ...

    @overload
    def search_logs(
        self,
        *,
        tags: Mapping[str, str] | None = None,
        start: _InferrableTimestampType | None = None,
        end: _InferrableTimestampType | None = None,
    ) -> Iterable[LogPoint]: ...

    def search_logs(
        self,
        *,
        regex_match: str | None = None,
        insensitive_match: str | None = None,
        tags: Mapping[str, str] | None = None,
        start: _InferrableTimestampType | None = None,
        end: _InferrableTimestampType | None = None,
    ) -> Iterable[LogPoint]:
        """Yields logpoints from the current channel that match the provided arguments

        Args:
            regex_match: If provided, a regex match to filter potential log messages by.

                **Note:** Must not be present with `insensitive_match`.
            insensitive_match: If provided, a case insensitive string that yielded logs match exactly.

                **Note:** Must not be present with `regex_match`.
            tags: Tags to filter logs from the channel with
            start: Timestamp to start yielding results from. If not present, searches starting from unix epoch
            end: Timestamp after which to stop yielding results from. If not present, searches until end of time.
        """
        # Must be <= 500
        PAGE_SIZE = 200

        if self.data_type is not ChannelDataType.LOG:
            raise TypeError(f"Not searching channel {self.name} for logs-- not a log channel!")

        api_start = (_SecondsNanos.from_flexible(start) if start else _MIN_TIMESTAMP).to_api()
        api_end = (_SecondsNanos.from_flexible(end) if end else _MAX_TIMESTAMP).to_api()

        filtered_series = scout_compute_api.LogSeries(
            filter=scout_compute_api.LogFilterSeries(
                input=scout_compute_api.LogSeries(channel=self._to_channel_series(tags=tags)),
                operator=_log_filter_operator(regex_match=regex_match, insensitive_match=insensitive_match),
            )
        )
        compute_series = scout_compute_api.Series(log=filtered_series)

        def request_factory(page_token: scout_compute_api.PageToken | None) -> scout_compute_api.ComputeNodeRequest:
            return scout_compute_api.ComputeNodeRequest(
                context=scout_compute_api.Context(dataset_references={}, variables={}, function_variables={}),
                start=api_start,
                end=api_end,
                node=scout_compute_api.ComputableNode(
                    series=scout_compute_api.SummarizeSeries(
                        input=compute_series,
                        numeric_aggregations={},
                        summarization_strategy=scout_compute_api.SummarizationStrategy(
                            page=scout_compute_api.PageStrategy(
                                page_info=scout_compute_api.PageInfo(page_size=PAGE_SIZE, page_token=page_token)
                            )
                        ),
                    )
                ),
            )

        def token_factory(response: scout_compute_api.ComputeNodeResponse) -> scout_compute_api.PageToken | None:
            if response.paged_log:
                return response.paged_log.next_page_token
            else:
                raise RuntimeError(f"Expected response to support paging, received {response.type}")

        for resp in paginate_rpc(
            self._clients.compute.compute,
            self._clients.auth_header,
            request_factory=request_factory,
            token_factory=token_factory,
        ):
            if resp.paged_log:
                for timestamp, log in zip(resp.paged_log.timestamps, resp.paged_log.values):
                    yield LogPoint._from_compute_api(log, timestamp)
            else:
                raise RuntimeError(f"Expected response type to be `paged_log`, received: `{resp.type}`")

    def get_available_tags(
        self,
        start_time: _InferrableTimestampType | None = None,
        end_time: _InferrableTimestampType | None = None,
        initial_tags: Mapping[str, str] | None = None,
    ) -> Mapping[str, set[str]]:
        """Return available tag key-values for the channel over the given time period

        Args:
            start_time: Start time to start scanning for tags from (defaults to the beginning of time)
            end_time: End time to stop scanning for tags until (defaults to the end of time)
            initial_tags: Initial tags to filter results by

        Returns:
            Mapping of all tag names to all tag values present

        Example:
            Given data with the following tags per-point within the specified time range:

            ```python
            {"tag_a": "123", "tag_b": "xyz"}
            {"tag_a": "123", "tag_b": "abc"}
            {"tag_a": "234", "tag_b": "qqq"}
            ```

            With an initial filter of ``{"tag_a": "123"}``, we would return:

            ```python
            {"tag_a": set(["123"]), "tag_b": set(["abc", "xyz"])}
            ```

            Because we filtered data to only include data where "tag_a" is "123"

        Note:
            It is not accurate to say that the cartesian product of all returned tag key-value pairs
            is present in the data, only that for each unique key-value pair present, that specific tag key/value
            pair is present on *at least* one point in the data.

        Note:
            This may be used to determine if the given set of initial tags *fully constrains* data in a channel over
            a given timespan by checking to see if the length of all of the tag-value sets is 1.
        """
        if start_time is None:
            start_time = _MIN_TIMESTAMP.to_nanoseconds()
        api_start = _SecondsNanos.from_flexible(start_time).to_scout_run_api()

        if end_time is None:
            end_time = _MAX_TIMESTAMP.to_nanoseconds()
        api_end = _SecondsNanos.from_flexible(end_time).to_scout_run_api()

        request = datasource_api.GetAvailableTagsForChannelRequest(
            channel_with_tag_filters=datasource_api.ChannelWithTagFilters(
                channel=self.name,
                data_source_rid=self.data_source,
                tag_filters={**(initial_tags or {})},
            ),
            start_time=api_start,
            end_time=api_end,
        )
        resp = self._clients.datasource.get_available_tags_for_channel(self._clients.auth_header, request)
        return {tag_name: set(tag_values) for tag_name, tag_values in resp.available_tags.available_tags.items()}

    @classmethod
    def _from_conjure_datasource_api(cls, clients: _Clients, channel: datasource_api.ChannelMetadata) -> Self:
        # NOTE: intentionally ignoring archetype RID as it does not correspond to a Channel in the same way that a
        #   logical series does
        channel_unit = channel.unit.symbol if channel.unit else None
        channel_data_type = ChannelDataType._from_conjure(channel.data_type) if channel.data_type else None
        return cls(
            name=channel.name,
            data_source=channel.data_source,
            unit=channel_unit,
            description=channel.description,
            data_type=channel_data_type,
            _clients=clients,
        )

    @classmethod
    def _from_proto(cls, clients: _Clients, channel: channel_metadata_pb2.ChannelMetadata) -> Self:
        return cls(
            name=channel.channel_identifier.channel_name,
            data_source=channel.channel_identifier.data_source_rid,
            unit=channel.unit if channel.HasField("unit") else None,
            description=channel.description if channel.HasField("description") else None,
            # Preserve the query type used by compute, rather than the underlying storage type.
            data_type=ChannelDataType._from_proto(channel.data_type) if channel.HasField("data_type") else None,
            _clients=clients,
        )

    def _to_channel_series(self, tags: Mapping[str, str] | None = None) -> scout_compute_api.ChannelSeries:
        return scout_compute_api.ChannelSeries(
            data_source=scout_compute_api.DataSourceChannel(
                channel=scout_compute_api.StringConstant(literal=self.name),
                data_source_rid=scout_compute_api.StringConstant(literal=self.data_source),
                tags=create_api_tags(tags),
                tags_to_group_by=[],
                group_by_tags=[],
            )
        )

    def _to_compute_series(self, tags: Mapping[str, str] | None = None) -> scout_compute_api.Series:
        channel_series = self._to_channel_series(tags=tags)
        return _create_series_from_channel(channel_series, self.data_type)

    def _to_proto_compute_series(self, tags: Mapping[str, str] | None = None) -> expressions_pb2.SeriesExpression:
        channel = source_selection_pb2.ChannelSelection(
            data_source=source_selection_pb2.DataSourceChannelSelection(
                data_source=source_selection_pb2.DataSourceReferenceExpression(literal=self.data_source),
                channel=constants_pb2.StringConstant(literal=self.name),
                tags=source_selection_pb2.ChannelTagConstraints(
                    exact_values=source_selection_pb2.ExactTagValues(
                        values={k: constants_pb2.StringConstant(literal=v) for k, v in (tags or {}).items()}
                    )
                ),
            )
        )
        if self.data_type is ChannelDataType.STRING:
            return expressions_pb2.SeriesExpression(
                categorical=expressions_pb2.CategoricalExpression(channel_selection=channel)
            )
        if self.data_type in (ChannelDataType.DOUBLE, ChannelDataType.INT):
            return expressions_pb2.SeriesExpression(
                numeric=expressions_pb2.NumericExpression(channel_selection=channel)
            )
        if self.data_type is ChannelDataType.LOG:
            return expressions_pb2.SeriesExpression(log=expressions_pb2.LogExpression(channel_selection=channel))
        raise ValueError(f"Unsupported channel data type: {self.data_type}")

    def _decimate_request(
        self,
        start: str | datetime | IntegralNanosecondsUTC,
        end: str | datetime | IntegralNanosecondsUTC,
        tags: Mapping[str, str] | None = None,
        buckets: int | None = None,
        resolution: int | None = None,
    ) -> scout_compute_api.ComputeNodeResponse:
        request = scout_compute_api.ComputeNodeRequest(
            start=_SecondsNanos.from_flexible(start).to_api(),
            end=_SecondsNanos.from_flexible(end).to_api(),
            node=scout_compute_api.ComputableNode(
                series=scout_compute_api.SummarizeSeries(
                    input=self._to_compute_series(tags=tags),
                    numeric_aggregations={},
                    buckets=buckets,
                    resolution=resolution,
                )
            ),
            context=scout_compute_api.Context(
                dataset_references={},
                variables={},
                function_variables={},
            ),
        )
        response = self._clients.compute.compute(self._clients.auth_header, request)
        return response

    def _get_series_values_csv(
        self,
        start: api.Timestamp,
        end: api.Timestamp,
        relative_to: datetime | IntegralNanosecondsUTC | None = None,
        relative_resolution: _LiteralTimeUnit = "nanoseconds",
        *,
        enable_gzip: bool = True,
        tags: Mapping[str, str] | None = None,
    ) -> BinaryIO:
        """Get the channel data as a CSV file-like object.

        Args:
            start: Start timestamp
            end: End timestamp
            relative_to: If provided, timestamps are returned relative to the given timestamp
            relative_resolution: If timestamps are returned in relative time, the resolution to use.
            enable_gzip: If true, use gzip when exporting data from Nominal. This will almost always make export
                faster and use less bandwidth.
            tags: Tags to filter the series by

        Returns:
            A binary file-like object containing the CSV data
        """
        request = data_exports_pb2.ExportDefinition(
            time_domain=data_exports_pb2.TimeDomainColumns(
                columns=[
                    data_exports_pb2.ExportColumn(name=self.name, series=self._to_proto_compute_series(tags=tags))
                ],
                gap_fill=data_exports_pb2.GapFill(leave_empty=data_exports_pb2.LeaveEmpty()),
                timestamp_format=export_timestamp_format(_to_export_timestamp_type(relative_to, relative_resolution)),
            ),
            start_time=Timestamp(seconds=start.seconds, nanos=start.nanos),
            end_time=Timestamp(seconds=end.seconds, nanos=end.nanos),
            context=expressions_pb2.ComputeContext(),
            format=data_exports_pb2.ExportFileFormat(csv=data_exports_pb2.CsvFormat()),
            resolution=data_exports_pb2.ExportResolution(full=data_exports_pb2.FullResolution()),
            compression=data_exports_pb2.EXPORT_COMPRESSION_GZIP
            if enable_gzip
            else data_exports_pb2.EXPORT_COMPRESSION_UNSPECIFIED,
        )
        return stream_export(self._clients.grpc_dataexport, request)


def _create_series_from_channel(
    channel_series: scout_compute_api.ChannelSeries, data_type: ChannelDataType | None
) -> scout_compute_api.Series:
    """Create a Series object based on the channel's data type.

    Args:
        channel_series: The channel series to use
        data_type: The data type of the channel

    Returns:
        A Series object appropriate for the channel's data type

    Raises:
        ValueError: If the channel's data type is not supported
    """
    if data_type == ChannelDataType.STRING:
        return scout_compute_api.Series(enum=scout_compute_api.EnumSeries(channel=channel_series))
    elif data_type in (ChannelDataType.DOUBLE, ChannelDataType.INT):
        return scout_compute_api.Series(numeric=scout_compute_api.NumericSeries(channel=channel_series))
    elif data_type == ChannelDataType.LOG:
        return scout_compute_api.Series(log=scout_compute_api.LogSeries(channel=channel_series))
    else:
        raise ValueError(f"Unsupported channel data type: {data_type}")


def _batch_check_channels_have_data(
    clients: Channel._Clients,
    channels: Sequence[Channel],
    tag_filters: scout_compute_api.TagFilters | None,
    start: api.Timestamp,
    end: api.Timestamp,
) -> tuple[list[Channel], list[str]]:
    """Check a batch of channels for data presence via a single batchGetSeriesCount call.

    Returns a tuple of:
        - channels confirmed to have data (series_count > 0)
        - names of channels with underconstrained tags (series_count > 1)

    Note:
        Request may fail if rate limits are breached, too many series are queried, bounds are invalid
        (such as end < start), timeouts are reached, or otherwise if tag filters are invalid or contradictory.
        This will raise various ConjureHttpError exceptions.
    """
    time_range = api.Range(start=start, end=end)
    request = datasource_api.BatchGetSeriesCountRequest(
        requests=[
            datasource_api.GetSeriesCountRequest(
                data_source_rid=channel.data_source,
                channel=channel.name,
                range=time_range,
                tag_filters=tag_filters,
            )
            for channel in channels
        ]
    )
    response = clients.datasource.batch_get_series_count(clients.auth_header, request)

    channels_with_data: list[Channel] = []
    underconstrained: list[str] = []
    for channel, result in zip(channels, response.responses, strict=True):
        count = result.series_count
        if count is not None and count > 0:
            channels_with_data.append(channel)
            # Multiple series for a single channel+tags means the tags don't fully
            # constrain the data — results may contain duplicate timestamps.
            if count > 1:
                underconstrained.append(channel.name)

    return channels_with_data, underconstrained


def filter_channels_with_data(
    channels: Sequence[Channel],
    *,
    tags: Mapping[str, str] | None = None,
    start_time: datetime | IntegralNanosecondsUTC,
    end_time: datetime | IntegralNanosecondsUTC,
    num_workers: int = 8,
    batch_size: int = 200,
) -> Iterator[Channel]:
    """Yield channels that have data in the given time range, optionally filtered by tags.

    Uses batchGetSeriesCount to query data presence server-side in batched API calls. If a
    batch call fails, its channels are re-submitted individually into the same pool; a
    single-channel call that fails is treated as having no data and excluded.

    Args:
        channels: Channels to check for data presence.
        tags: If provided, only yield channels matching these tag key-value pairs.
        start_time: Start of the time range to check for data.
        end_time: End of the time range to check for data.
        num_workers: Number of concurrent API requests.
        batch_size: Max channels per API call.

    Yields:
        Channel objects for each input channel confirmed to have data in the range.
    """
    if not channels:
        return

    clients = channels[0]._clients
    api_start = _SecondsNanos.from_flexible(start_time).to_api()
    api_end = _SecondsNanos.from_flexible(end_time).to_api()
    tag_filters = build_compute_tag_filter(tags)

    matched_keys: set[tuple[str, str]] = set()
    all_underconstrained: list[str] = []
    still_failing: list[Channel] = []

    check_batch = functools.partial(
        _batch_check_channels_have_data,
        clients,
        tag_filters=tag_filters,
        start=api_start,
        end=api_end,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        pending: dict[concurrent.futures.Future[tuple[list[Channel], list[str]]], Sequence[Channel]] = {
            pool.submit(check_batch, batch): batch for batch in batched(channels, batch_size)
        }

        while pending:
            done, _ = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                batch = pending.pop(future)
                try:
                    matched, underconstrained = future.result()
                except Exception:
                    # Series count calculations may fail for a myriad of reasons, such as
                    # A single-channel request that failed is already a retry (or was submitted
                    # that way by the caller via batch_size=1); don't retry indefinitely.
                    if len(batch) == 1:
                        still_failing.extend(batch)
                        continue
                    logger.warning(
                        "Batch data-presence check failed for %d channels; retrying individually",
                        len(batch),
                    )
                    for ch in batch:
                        pending[pool.submit(check_batch, [ch])] = [ch]
                    continue

                for ch in matched:
                    matched_keys.add((ch.data_source, ch.name))
                all_underconstrained.extend(underconstrained)

    if still_failing:
        # Both the batch and the individual retry failed — we can't verify data exists, so
        # exclude these channels rather than admitting. Admitting would run expensive PPS
        # compute downstream on channels the export would likely fail on anyway.
        logger.warning(
            "Data-presence check failed for %d channels even after individual retry; "
            "treating as having no data and excluding from export: %s",
            len(still_failing),
            ", ".join(ch.name for ch in still_failing),
        )

    if all_underconstrained:
        sample = all_underconstrained[:10]
        joined = ", ".join(sample)
        suffix = f", ... ({len(all_underconstrained) - 10} more)" if len(all_underconstrained) > 10 else ""
        logger.warning(
            "%d channels have underconstrained tags (multiple series matched): %s%s",
            len(all_underconstrained),
            joined,
            suffix,
        )

    # Yield in original input order
    for channel in channels:
        if (channel.data_source, channel.name) in matched_keys:
            yield channel
