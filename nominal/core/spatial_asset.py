from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, Protocol, Sequence

from nominal_api import scout_spatial, scout_spatial_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableMixin
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class SpatialAsset(HasRid, RefreshableMixin[scout_spatial_api.Spatial]):
    """A spatial asset (e.g. a point cloud) tracked by Nominal."""

    rid: str
    name: str
    description: str | None
    labels: Sequence[str]
    properties: Mapping[str, str]
    is_archived: bool
    dagger_uuid: str
    sensor_model: str | None
    created_at: IntegralNanosecondsUTC

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def spatial(self) -> scout_spatial.SpatialService: ...

    def _get_latest_api(self) -> scout_spatial_api.Spatial:
        return self._clients.spatial.get(self._clients.auth_header, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace spatial asset metadata in-place and return the updated asset.

        Only the fields passed in are replaced; the rest are left untouched.
        """
        request = scout_spatial_api.UpdateSpatialMetadataRequest(
            title=name,
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
        )
        updated = self._clients.spatial.update_metadata(self._clients.auth_header, request, self.rid)
        return self._refresh_from_api(updated)

    def archive(self) -> None:
        """Archive this spatial asset, hiding it from search (reversible)."""
        self._clients.spatial.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive a previously archived spatial asset."""
        self._clients.spatial.unarchive(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, raw_spatial: scout_spatial_api.Spatial) -> Self:
        point_cloud = raw_spatial.type_metadata.point_cloud
        return cls(
            rid=raw_spatial.rid,
            name=raw_spatial.title,
            description=raw_spatial.description,
            labels=tuple(raw_spatial.labels),
            properties=MappingProxyType(raw_spatial.properties),
            is_archived=raw_spatial.is_archived,
            dagger_uuid=raw_spatial.dagger_uuid,
            sensor_model=point_cloud.sensor_model if point_cloud is not None else None,
            created_at=_SecondsNanos.from_flexible(raw_spatial.created_at).to_nanoseconds(),
            _clients=clients,
            created_by_rid=raw_spatial.created_by,
        )


def _get_spatial(clients: SpatialAsset._Clients, rid: str) -> scout_spatial_api.Spatial:
    return clients.spatial.get(clients.auth_header, rid)
