from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import BinaryIO, Iterable, Mapping, Protocol, cast, overload

from nominal_api import (
    api,
    datasource_api,
    scout_compute_api,
    scout_dataexport_api,
    timeseries_channelmetadata,
    timeseries_channelmetadata_api,
    timeseries_logicalseries_api,
)
from typing_extensions import Self

from nominal._utils import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import create_api_tags
from nominal.core._utils.pagination_tools import paginate_rpc
from nominal.core.log import LogPoint, _log_filter_operator
from nominal.core.unit import UnitLike, _build_unit_update
from nominal.ts import (
    _MAX_TIMESTAMP,
    _MIN_TIMESTAMP,
    IntegralNanosecondsUTC,
    _InferrableTimestampType,
    _LiteralTimeUnit,
    _SecondsNanos,
    _time_unit_to_conjure,
)

logger = logging.getLogger(__name__)


class ChannelDataType(enum.Enum):
    # TODO (drake): support DOUBLE_ARRAY and STRING_ARRAY
    DOUBLE = "DOUBLE"
    STRING = "STRING"
    LOG = "LOG"
    INT = "INT"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _from_conjure(cls, data_type: api.SeriesDataType) -> Self:
        if data_type.value in cls.__members__:
            return cls(data_type.value)
        else:
            return cls("UNKNOWN")


@dataclass
class Channel:
    """Metadata for working with channels."""

    name: str
    data_source: str
    data_type: ChannelDataType | None
    unit: str | None
    description: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def dataexport(self) -> scout_dataexport_api.DataExportService: ...
        @property
        def compute(self) -> scout_compute_api.ComputeService: ...
        @property
        def channel_metadata(self) -> timeseries_channelmetadata.ChannelMetadataService: ...

    class _NotProvided:
        """Sentinel class for detecting when a user has or has not provided a value during updates"""

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
                NOTE: this is in contrast to other fields in other `update()` calls where `None` is treated as a
                      "no-op".
        """
        channel_metadata = self._clients.channel_metadata.update_channel_metadata(
            self._clients.auth_header,
            timeseries_channelmetadata_api.UpdateChannelMetadataRequest(
                channel_identifier=timeseries_channelmetadata_api.ChannelIdentifier(
                    channel_name=self.name,
                    data_source_rid=self.data_source,
                ),
                description=description,
                unit_update=_build_unit_update(unit) if not isinstance(unit, self._NotProvided) else None,
            ),
        )
        updated_channel = self.__class__._from_channel_metadata_api(self._clients, channel_metadata)
        update_dataclass(self, updated_channel, fields=self.__dataclass_fields__)
        return self

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
            regex_match: If provided, a regex match to filter potential log messages by
                NOTE: must not be present with `insensitive_match`
            insensitive_match: If provided, a case insensitive string that yielded logs match exactly
                NOTE: must not be present with `regex_match`
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
                context=scout_compute_api.Context(function_variables={}, variables={}),
                start=api_start,
                end=api_end,
                node=scout_compute_api.ComputableNode(
                    series=scout_compute_api.SummarizeSeries(
                        input=compute_series,
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
    def _from_conjure_logicalseries_api(
        cls, clients: _Clients, series: timeseries_logicalseries_api.LogicalSeries
    ) -> Self:
        channel_data_type = ChannelDataType._from_conjure(series.series_data_type) if series.series_data_type else None
        return cls(
            name=series.channel,
            data_source=series.data_source_rid,
            unit=series.unit,
            description=series.description,
            data_type=channel_data_type,
            _clients=clients,
        )

    @classmethod
    def _from_channel_metadata_api(
        cls, clients: _Clients, channel: timeseries_channelmetadata_api.ChannelMetadata
    ) -> Self:
        channel_data_type = ChannelDataType._from_conjure(channel.data_type) if channel.data_type else None
        return cls(
            name=channel.channel_identifier.channel_name,
            data_source=channel.channel_identifier.data_source_rid,
            unit=channel.unit,
            description=channel.description,
            data_type=channel_data_type,
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

    def _to_time_domain_channel(self, tags: Mapping[str, str] | None = None) -> scout_dataexport_api.TimeDomainChannel:
        return scout_dataexport_api.TimeDomainChannel(
            column_name=self.name, compute_node=self._to_compute_series(tags=tags)
        )

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
                    buckets=buckets,
                    resolution=resolution,
                )
            ),
            context=scout_compute_api.Context(
                function_variables={},
                variables={},
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
        request = scout_dataexport_api.ExportDataRequest(
            channels=scout_dataexport_api.ExportChannels(
                time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                    channels=[self._to_time_domain_channel(tags=tags)],
                    merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                        # only one series will be returned, so no need to merge
                        none=scout_dataexport_api.NoneStrategy(),
                    ),
                    output_timestamp_format=_create_timestamp_format(relative_to, relative_resolution),
                )
            ),
            start_time=start,
            end_time=end,
            context=scout_compute_api.Context(
                function_variables={},
                variables={},
            ),
            format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
            resolution=scout_dataexport_api.ResolutionOption(
                undecimated=scout_dataexport_api.UndecimatedResolution(),
            ),
            compression=scout_dataexport_api.CompressionFormat.GZIP if enable_gzip else None,
        )
        response = self._clients.dataexport.export_channel_data(self._clients.auth_header, request)
        # note: the response is the same as the requests.Response.raw field, with stream=True on the request;
        # this acts like a file-like object in binary-mode.
        return cast(BinaryIO, response)


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


def _create_timestamp_format(
    relative_to: datetime | IntegralNanosecondsUTC | None = None,
    relative_resolution: _LiteralTimeUnit = "nanoseconds",
) -> scout_dataexport_api.TimestampFormat:
    if relative_to is None:
        return scout_dataexport_api.TimestampFormat(iso8601=scout_dataexport_api.Iso8601TimestampFormat())
    else:
        return scout_dataexport_api.TimestampFormat(
            relative=scout_dataexport_api.RelativeTimestampFormat(
                relative_to=_SecondsNanos.from_flexible(relative_to).to_api(),
                time_unit=_time_unit_to_conjure(relative_resolution),
            )
        )
