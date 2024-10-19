from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, BinaryIO, cast

import pandas as pd
from typing_extensions import Self

from .._api.combined import (
    datasource_api,
    scout_compute_api,
    scout_dataexport_api,
    timeseries_logicalseries_api,
)
from ..ts import _SecondsNanos
from ._clientsbunch import ClientsBunch
from ._utils import HasRid

# long max is 9,223,372,036,854,775,807, backend converts to long nanoseconds, so this is the last valid timestamp
# that can be represented in the API. (2262-04-11 19:47:16.854775807)
_MIN_TIMESTAMP = _SecondsNanos(seconds=0, nanos=0).to_api()
_MAX_TIMESTAMP = _SecondsNanos(seconds=9223372036, nanos=854775807).to_api()


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


@dataclass
class Channel(HasRid):
    """Metadata for working with channels."""

    rid: str
    name: str
    data_source: str
    data_type: ChannelDataType | None
    unit: str | None
    description: str | None
    _clients: ClientsBunch = field(repr=False)

    def to_pandas(self) -> pd.Series[Any]:
        """Retrieve the channel data as a pandas.Series.

        The index of the series is the timestamp of the data.
        The index name is "timestamp" and the series name is the channel name.

        Example:
        ```
        s = channel.to_pandas()
        print(s.name, "mean:", s.mean())
        ```
        """
        body = _get_series_values_csv(self._clients.auth_header, self._clients.dataexport, self.rid, self.name)
        df = pd.read_csv(body, parse_dates=["timestamp"], index_col="timestamp")
        return df[self.name]

    @classmethod
    def _from_conjure_datasource_api(cls, clients: ClientsBunch, channel: datasource_api.ChannelMetadata) -> Self:
        # NOTE: intentionally ignoring archetype RID as it does not correspond to a Channel in the same way that a logical series does
        if channel.series_rid.logical_series is None:
            raise ValueError(f"Cannot create ChannelMetadata for channel {channel.name}: no defined RID")

        channel_unit = channel.unit.symbol if channel.unit else None
        channel_data_type = ChannelDataType._from_conjure(channel.data_type) if channel.data_type else None
        return cls(
            rid=channel.series_rid.logical_series,
            name=channel.name,
            data_source=channel.data_source,
            unit=channel_unit,
            description=channel.description,
            data_type=channel_data_type,
            _clients=clients,
        )

    @classmethod
    def _from_conjure_logicalseries_api(
        cls, clients: ClientsBunch, series: timeseries_logicalseries_api.LogicalSeries
    ) -> Self:
        channel_data_type = ChannelDataType._from_conjure(series.series_data_type) if series.series_data_type else None
        return cls(
            rid=series.rid,
            name=series.channel,
            data_source=series.data_source_rid,
            unit=series.unit,
            description=series.description,
            data_type=channel_data_type,
            _clients=clients,
        )


def _get_series_values_csv(
    auth_header: str, client: scout_dataexport_api.DataExportService, rid: str, name: str
) -> BinaryIO:
    request = scout_dataexport_api.ExportDataRequest(
        channels=scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=[
                    scout_dataexport_api.TimeDomainChannel(
                        column_name=name,
                        compute_node=scout_compute_api.SeriesNode(
                            raw=scout_compute_api.RawUntypedSeriesNode(name=name)
                        ),
                    ),
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
        start_time=_MIN_TIMESTAMP,
        end_time=_MAX_TIMESTAMP,
        context=scout_compute_api.Context(
            function_variables={},
            variables={name: scout_compute_api.VariableValue(series=scout_compute_api.SeriesSpec(rid=rid))},
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
