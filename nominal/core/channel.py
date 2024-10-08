from __future__ import annotations

import dataclasses
import enum

from typing_extensions import Self

from .._api.combined import datasource_api, timeseries_logicalseries_api
from ._utils import HasRid


class ChannelDataType(enum.Enum):
    DOUBLE = "DOUBLE"
    STRING = "STRING"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _from_conjure(
        cls, data_type: datasource_api.SeriesDataType | timeseries_logicalseries_api.SeriesDataType
    ) -> Self:
        if data_type.value in cls:
            return cls(data_type.value)
        else:
            return cls("UNKNOWN")


@dataclasses.dataclass
class Channel(HasRid):
    """Metadata for working with channels."""

    rid: str
    name: str
    data_source: str
    data_type: ChannelDataType | None
    unit: str | None
    description: str | None

    @classmethod
    def _from_conjure(
        cls, conjure_obj: datasource_api.ChannelMetadata | timeseries_logicalseries_api.LogicalSeries
    ) -> Self:
        if isinstance(conjure_obj, timeseries_logicalseries_api.LogicalSeries):
            return cls._from_logicalseries(conjure_obj)
        elif isinstance(conjure_obj, datasource_api.ChannelMetadata):
            return cls._from_channel_metadata(conjure_obj)
        else:
            raise TypeError(f"_from_conjure not supported for input type {type(conjure_obj)}!")

    @classmethod
    def _from_channel_metadata(cls, channel: datasource_api.ChannelMetadata) -> Self:
        channel_rid = (
            channel.series_rid.series_archetype
            if channel.series_rid.series_archetype
            else channel.series_rid.logical_series
        )
        if channel_rid is None:
            raise ValueError(f"Cannot create ChannelMetadata for channel {channel.name}: no defined RID")

        channel_unit = channel.unit.symbol if channel.unit else None
        channel_data_type = ChannelDataType._from_conjure(channel.data_type) if channel.data_type else None
        return cls(
            rid=channel_rid,
            name=channel.name,
            data_source=channel.data_source,
            unit=channel_unit,
            description=channel.description,
            data_type=channel_data_type,
        )

    @classmethod
    def _from_logicalseries(cls, series: timeseries_logicalseries_api.LogicalSeries) -> Self:
        channel_data_type = ChannelDataType._from_conjure(series.series_data_type) if series.series_data_type else None
        return cls(
            rid=series.rid,
            name=series.channel,
            data_source=series.data_source_rid,
            unit=series.unit,
            description=series.description,
            data_type=channel_data_type,
        )
