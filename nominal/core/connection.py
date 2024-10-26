from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

from nominal._api.combined import datasource_api, scout_datasource_connection_api, timeseries_logicalseries_api
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._utils import HasRid
from nominal.core.channel import Channel


@dataclass(frozen=True)
class Connection(HasRid):
    rid: str
    name: str
    description: str | None
    _tags: Mapping[str, Sequence[str]]
    _clients: ClientsBunch = field(repr=False)

    @classmethod
    def _from_conjure(cls, clients: ClientsBunch, response: scout_datasource_connection_api.Connection) -> Connection:
        return cls(
            rid=response.rid,
            name=response.display_name,
            description=response.description,
            _tags=response.available_tag_values,
            _clients=clients,
        )

    def _get_series_achetypes_paginated(self) -> Iterable[datasource_api.ChannelMetadata]:
        request = datasource_api.SearchChannelsRequest(
            data_sources=[self.rid], exact_match=[], fuzzy_search_text="", previously_selected_channels={}
        )
        while True:
            response = self._clients.datasource.search_channels(self._clients.auth_header, request)
            for channel_metadata in response.results:
                if channel_metadata.series_rid.type == "seriesArchetype":
                    yield channel_metadata
                else:
                    logging.debug("ignoring channel with series_rid type %s", channel_metadata.series_rid.type)
            if response.next_page_token is None:
                break
            request = datasource_api.SearchChannelsRequest(
                data_sources=[self.rid],
                exact_match=[],
                fuzzy_search_text="",
                previously_selected_channels={},
                next_page_token=response.next_page_token,
            )

    def _resolve_archetypes_to_channels(self, channel_names: Sequence[str]) -> Iterable[Channel]:
        """Given archetype names ("shirts", "sweaters") and existing tag values
        ({color: [red, green], size: [S, M, L]}), resolve all possible combinations of archetype names and tag values
        to logical series. We will try to resolve the following logical series:
            * shirts, {color: red, size: S}
            * shirts, {color: red, size: M}
            * shirts, {color: red, size: L}
            * shirts, {color: green, size: S}
            * shirts, {color: green, size: M}
            * shirts, {color: green, size: L}
            * sweaters, {color: red, size: S}
            * sweaters, {color: red, size: M}
            * sweaters, {color: red, size: L}
            * sweaters, {color: green, size: S}
            * sweaters, {color: green, size: M}
            * sweaters, {color: green, size: L}
        """
        req = timeseries_logicalseries_api.BatchResolveSeriesRequest(
            requests=[
                timeseries_logicalseries_api.ResolveSeriesRequest(datasource=self.rid, name=name, tags=tags)
                for name in channel_names
                for tags in _tag_product(self._tags)
            ]
        )
        resp = self._clients.logical_series.resolve_batch(self._clients.auth_header, req)
        # TODO(alkasm): is there a batch get_logical_series ?
        for resolved_series in resp.series:
            if resolved_series.type == "error" and resolved_series.error is not None:
                raise RuntimeError(f"error resolving series: {resolved_series.error}")
            elif resolved_series.rid is None:
                raise RuntimeError(f"error resolving series for series {resolved_series}: no rid returned")
            series = self._clients.logical_series.get_logical_series(self._clients.auth_header, resolved_series.rid)
            yield Channel._from_conjure_logicalseries_api(self._clients, series)

    def _get_channels(self) -> Iterable[Channel]:
        """Retrieve all channels associated with this connection."""
        channel_names = [channel.name for channel in self._get_series_achetypes_paginated()]
        return self._resolve_archetypes_to_channels(channel_names)

    def get_channel(self, name: str, tags: dict[str, str] | None = None) -> Channel:
        """Retrieve a channel with the given name and tags."""
        req = timeseries_logicalseries_api.BatchResolveSeriesRequest(
            requests=[
                timeseries_logicalseries_api.ResolveSeriesRequest(
                    datasource=self.rid, name=name, tags={} if tags is None else tags
                )
            ]
        )
        resp = self._clients.logical_series.resolve_batch(self._clients.auth_header, req)
        if len(resp.series) == 0:
            raise ValueError("no channel found with name {name!r} and tags {tags!r}")
        elif len(resp.series) > 1:
            raise ValueError("multiple channels found with name {name!r} and tags {tags!r}")

        resolved_series = resp.series[0]
        if resolved_series.type == "error" and resolved_series.error is not None:
            raise RuntimeError(f"error resolving series: {resolved_series.error}")
        elif resolved_series.rid is None:
            raise RuntimeError(f"error resolving series for series {resolved_series}: no rid returned")
        series = self._clients.logical_series.get_logical_series(self._clients.auth_header, resolved_series.rid)
        return Channel._from_conjure_logicalseries_api(self._clients, series)


def _tag_product(tags: Mapping[str, Sequence[str]]) -> list[dict[str, str]]:
    # {color: [red, green], size: [S, M, L]} -> [{color: red, size: S}, {color: red, size: M}, ...,
    #                                            {color: green, size: L}]
    return [dict(zip(tags.keys(), values)) for values in itertools.product(*tags.values())]
