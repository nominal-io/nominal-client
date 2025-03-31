from __future__ import annotations

import enum
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, BinaryIO, Protocol, cast

import typing_extensions
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

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import update_dataclass
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

if TYPE_CHECKING:
    import pandas as pd


class ChannelDataType(enum.Enum):
    DOUBLE = "DOUBLE"
    STRING = "STRING"
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
    _rid: str

    @property
    def rid(self) -> str:
        """Get the rid value with a deprecation warning."""
        warnings.warn("Accessing Channel.rid is deprecated and now returns an empty string.", UserWarning, stacklevel=2)
        return self._rid

    class _Clients(HasAuthHeader, Protocol):
        @property
        def dataexport(self) -> scout_dataexport_api.DataExportService: ...
        @property
        def compute(self) -> scout_compute_api.ComputeService: ...
        @property
        def channel_metadata(self) -> timeseries_channelmetadata.ChannelMetadataService: ...

    def update(
        self,
        *,
        description: str | None = None,
        unit: str | None = None,
    ) -> Self:
        """Replace channel metadata within Nominal, and updates / returns the local instance.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Args:
            description: Human-readable description of data within the channel
            unit: Unit symbol to apply to the channel
        """
        channel_metadata = self._clients.channel_metadata.update_channel_metadata(
            self._clients.auth_header,
            timeseries_channelmetadata_api.UpdateChannelMetadataRequest(
                channel_identifier=timeseries_channelmetadata_api.ChannelIdentifier(
                    channel_name=self.name,
                    data_source_rid=self.data_source,
                ),
                description=description,
                unit_update=timeseries_logicalseries_api.UnitUpdate(unit=unit) if unit else None,
            ),
        )
        updated_channel = self.__class__._from_channel_metadata_api(self._clients, channel_metadata)
        update_dataclass(self, updated_channel, fields=self.__dataclass_fields__)
        return self

    @typing_extensions.deprecated(
        "`channel.to_pandas` is deprecated and will be removed in a future version. "
        "Use `nominal.thirdparty.pandas.channel_to_series` instead."
    )
    def to_pandas(
        self,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
    ) -> pd.Series[Any]:
        """Retrieve the channel data as a pandas.Series."""
        from nominal.thirdparty.pandas import channel_to_series

        return channel_to_series(self, start=start, end=end)

    @classmethod
    def _from_conjure_datasource_api(cls, clients: _Clients, channel: datasource_api.ChannelMetadata) -> Self:
        # NOTE: intentionally ignoring archetype RID as it does not correspond to a Channel in the same way that a
        #   logical series does
        channel_unit = channel.unit.symbol if channel.unit else None
        channel_data_type = ChannelDataType._from_conjure(channel.data_type) if channel.data_type else None
        return cls(
            _rid="",
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
            _rid="",
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
            _rid="",
            name=channel.channel_identifier.channel_name,
            data_source=channel.channel_identifier.data_source_rid,
            unit=channel.unit,
            description=channel.description,
            data_type=channel_data_type,
            _clients=clients,
        )

    @typing_extensions.deprecated(
        "`channel.get_decimated` is deprecated and will be removed in a future version. "
        "Use `nominal.thirdparty.pandas.channel_to_dataframe_decimated` instead."
    )
    def get_decimated(
        self,
        start: str | datetime | IntegralNanosecondsUTC,
        end: str | datetime | IntegralNanosecondsUTC,
        *,
        buckets: int | None = None,
        resolution: int | None = None,
    ) -> pd.DataFrame:
        """Retrieve the channel data as a pandas.DataFrame, decimated to the given buckets or resolution."""
        from nominal.thirdparty.pandas import channel_to_dataframe_decimated

        return channel_to_dataframe_decimated(self, start=start, end=end, buckets=buckets, resolution=resolution)

    def _decimate_request(
        self,
        start: str | datetime | IntegralNanosecondsUTC,
        end: str | datetime | IntegralNanosecondsUTC,
        buckets: int | None = None,
        resolution: int | None = None,
    ) -> scout_compute_api.ComputeNodeResponse:
        channel_series = scout_compute_api.ChannelSeries(
            data_source=scout_compute_api.DataSourceChannel(
                channel=scout_compute_api.StringConstant(literal=self.name),
                data_source_rid=scout_compute_api.StringConstant(literal=self.data_source),
                tags={},
            )
        )

        series = _create_series_from_channel(channel_series, self.data_type)
        request = scout_compute_api.ComputeNodeRequest(
            start=_SecondsNanos.from_flexible(start).to_api(),
            end=_SecondsNanos.from_flexible(end).to_api(),
            node=scout_compute_api.ComputableNode(
                series=scout_compute_api.SummarizeSeries(
                    input=series,
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
    ) -> BinaryIO:
        """Get the channel data as a CSV file-like object.

        Args:
            start: Start timestamp
            end: End timestamp

        Returns:
            A binary file-like object containing the CSV data
        """
        channel_series = scout_compute_api.ChannelSeries(
            data_source=scout_compute_api.DataSourceChannel(
                channel=scout_compute_api.StringConstant(literal=self.name),
                data_source_rid=scout_compute_api.StringConstant(literal=self.data_source),
                tags={},
            )
        )
        series = _create_series_from_channel(channel_series, self.data_type)

        request = scout_dataexport_api.ExportDataRequest(
            channels=scout_dataexport_api.ExportChannels(
                time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                    channels=[
                        scout_dataexport_api.TimeDomainChannel(
                            column_name=self.name,
                            compute_node=series,
                        )
                    ],
                    merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                        # only one series will be returned, so no need to merge
                        none=scout_dataexport_api.NoneStrategy(),
                    ),
                    output_timestamp_format=scout_dataexport_api.TimestampFormat(
                        iso8601=scout_dataexport_api.Iso8601TimestampFormat()
                    ),
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
        )
        response = self._clients.dataexport.export_channel_data(self._clients.auth_header, request)
        # note: the response is the same as the requests.Response.raw field, with stream=True on the request;
        # this acts like a file-like object in binary-mode.
        return cast(BinaryIO, response)


def _get_series_values_csv(
    auth_header: str,
    client: scout_dataexport_api.DataExportService,
    rid_to_name: dict[str, str],
    start: api.Timestamp,
    end: api.Timestamp,
) -> BinaryIO:
    request = scout_dataexport_api.ExportDataRequest(
        channels=scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=[
                    scout_dataexport_api.TimeDomainChannel(
                        column_name=name,
                        compute_node=scout_compute_api.Series(raw=scout_compute_api.Reference(name=name)),
                    )
                    for name in rid_to_name.values()
                ],
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    # only one series will be returned, so no need to merge
                    none=scout_dataexport_api.NoneStrategy(),
                ),
                output_timestamp_format=scout_dataexport_api.TimestampFormat(
                    iso8601=scout_dataexport_api.Iso8601TimestampFormat()
                ),
            )
        ),
        start_time=start,
        end_time=end,
        context=scout_compute_api.Context(
            function_variables={},
            variables={
                name: scout_compute_api.VariableValue(series=scout_compute_api.SeriesSpec(rid=rid))
                for rid, name in rid_to_name.items()
            },
        ),
        format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
        resolution=scout_dataexport_api.ResolutionOption(
            undecimated=scout_dataexport_api.UndecimatedResolution(),
        ),
    )
    response = client.export_channel_data(auth_header, request)
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
    elif data_type == ChannelDataType.DOUBLE:
        return scout_compute_api.Series(numeric=scout_compute_api.NumericSeries(channel=channel_series))
    else:
        raise ValueError(f"Unsupported channel data type: {data_type}")
