from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from nominal_api import scout_compute_api
from nominal_api._impl import scout_compute_api_ChannelSeries as ChannelSeries


class ChannelIdentifier(ABC):
    @abstractmethod
    def to_compute_channel_series(self) -> scout_compute_api.ChannelSeries: ...


@dataclass(frozen=True)
class AssetChannelIdentifier(ChannelIdentifier):
    asset_rid: str
    data_scope_name: str
    channel_name: str
    additional_tags: typing.Mapping[str, str] = field(default_factory=dict)

    def to_compute_channel_series(self) -> scout_compute_api.ChannelSeries:
        return scout_compute_api.ChannelSeries(
            asset=scout_compute_api.AssetChannel(
                additional_tags={
                    k: scout_compute_api.StringConstant(literal=v) for k, v in self.additional_tags.items()
                },
                asset_rid=scout_compute_api.StringConstant(literal=self.asset_rid),
                data_scope_name=scout_compute_api.StringConstant(literal=self.data_scope_name),
                channel=scout_compute_api.StringConstant(literal=self.channel_name),
                group_by_tags=[],
                tags_to_group_by=[],
                additional_tag_filters=None,
            )
        )


@dataclass(frozen=True)
class DataSourceChannelIdentifier(ChannelIdentifier):
    datasource_rid: str
    channel_name: str
    tags: typing.Mapping[str, str] = field(default_factory=dict)

    def to_compute_channel_series(self) -> ChannelSeries:
        return scout_compute_api.ChannelSeries(
            data_source=scout_compute_api.DataSourceChannel(
                channel=scout_compute_api.StringConstant(literal=self.channel_name),
                data_source_rid=scout_compute_api.StringConstant(literal=self.datasource_rid),
                tags={k: scout_compute_api.StringConstant(literal=v) for k, v in self.tags.items()},
                group_by_tags=[],
                tags_to_group_by=[],
                tag_filters=None,
            )
        )


@dataclass(frozen=True)
class RunChannelIdentifier(ChannelIdentifier):
    run_rid: str
    data_scope_name: str
    channel_name: str
    additional_tags: typing.Mapping[str, str] = field(default_factory=dict)

    def to_compute_channel_series(self) -> scout_compute_api.ChannelSeries:
        return scout_compute_api.ChannelSeries(
            run=scout_compute_api.RunChannel(
                additional_tags={
                    k: scout_compute_api.StringConstant(literal=v) for k, v in self.additional_tags.items()
                },
                run_rid=scout_compute_api.StringConstant(literal=self.run_rid),
                data_scope_name=scout_compute_api.StringConstant(literal=self.data_scope_name),
                channel=scout_compute_api.StringConstant(literal=self.channel_name),
                group_by_tags=[],
                tags_to_group_by=[],
                additional_tag_filters=None,
            )
        )
