from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Literal, Mapping, Protocol, Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import DEFAULT_PAGE_SIZE, paginate_grpc
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2, asset_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class AssetTypeProperty:
    """A string or numeric property rule for members of an asset type.

    Suggested properties are optional; required properties must be present. The
    type's primary key is a separate, required nonempty string property.
    """

    value_type: Literal["string", "number"] = "string"
    required: bool = False

    def _to_proto(self) -> asset_pb2.PropertyConfig:
        match self.value_type:
            case "string":
                value_type = asset_pb2.STRING
            case "number":
                value_type = asset_pb2.NUMBER
            case _:
                raise ValueError(f"Unknown asset-type property value type: {self.value_type!r}")
        return asset_pb2.PropertyConfig(
            requirement=asset_pb2.REQUIRED if self.required else asset_pb2.SUGGESTED,
            value_type=value_type,
        )

    @classmethod
    def _from_proto(cls, value: asset_pb2.PropertyConfig) -> Self:
        match value.value_type:
            case asset_pb2.STRING:
                value_type: Literal["string", "number"] = "string"
            case asset_pb2.NUMBER:
                value_type = "number"
            case _:
                raise ValueError(f"Unknown asset-type property value type: {value.value_type}")
        if value.requirement not in (asset_pb2.REQUIRED, asset_pb2.SUGGESTED):
            raise ValueError(f"Unknown asset-type property requirement: {value.requirement}")
        return cls(value_type=value_type, required=value.requirement == asset_pb2.REQUIRED)


@dataclass(frozen=True)
class AssetType(HasRid, RefreshableGrpcMixin[asset_pb2.Type]):
    """An asset identity definition and the property rules its members must satisfy.

    The primary key names a required nonempty string property, unique among active
    assets of this type. Other property definitions are stored in ``properties``.
    """

    rid: str
    name: str
    description: str | None
    primary_key: str
    properties: Mapping[str, AssetTypeProperty]
    created_at: IntegralNanosecondsUTC
    is_archived: bool
    icon_name: str | None
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def assets(self) -> asset_pb2_grpc.AssetServiceStub: ...

    @property
    def nominal_url(self) -> str:
        """Link to this asset type in the Nominal app."""
        return f"{self._clients.app_base_url.rstrip('/')}/asset-types/{self.rid}"

    def _get_latest_api(self) -> asset_pb2.Type:
        return _get_asset_type(self._clients, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        primary_key: str | None = None,
        properties: Mapping[str, AssetTypeProperty] | None = None,
        icon_name: str | None = None,
    ) -> Self:
        """Replace supplied definition fields and refresh this instance in place.

        ``None`` leaves a field unchanged; an empty properties mapping removes all
        additional property rules. The primary key is separate from those rules.
        The server rejects definitions incompatible with existing active members,
        and primary-key renames once the type has memberships.

        Raises:
            NominalError: If the definition is invalid or the update fails.
        """
        request = asset_pb2.UpdateTypeRequest(
            type_rid=self.rid,
            name=name,
            description=description,
            primary_key=primary_key,
            icon_name=icon_name,
            property_configs=None
            if properties is None
            else asset_pb2.PropertyConfigMap(
                property_configs={key: value._to_proto() for key, value in properties.items()},
            ),
        )
        with translate_grpc_errors():
            response = self._clients.assets.UpdateType(request)
        return self._refresh_from_api(response.type)

    def archive(self) -> None:
        """Archive this type. Call ``refresh()`` to update the local snapshot.

        Existing memberships remain; archived types cannot be added to new members.
        """
        with translate_grpc_errors():
            self._clients.assets.ArchiveType(asset_pb2.ArchiveTypeRequest(type_rid=self.rid))

    def unarchive(self) -> None:
        """Restore this type. Call ``refresh()`` to update the local snapshot."""
        with translate_grpc_errors():
            self._clients.assets.UnarchiveType(asset_pb2.UnarchiveTypeRequest(type_rid=self.rid))

    @classmethod
    def _from_proto(cls, clients: _Clients, raw: asset_pb2.Type) -> Self:
        return cls(
            rid=raw.rid,
            name=raw.name,
            description=raw.description if raw.HasField("description") else None,
            primary_key=raw.primary_key,
            properties=MappingProxyType(
                {key: AssetTypeProperty._from_proto(value) for key, value in raw.property_configs.items()}
            ),
            created_at=raw.created_at.ToNanoseconds(),
            is_archived=raw.is_archived,
            icon_name=raw.icon_name if raw.HasField("icon_name") else None,
            _clients=clients,
        )


def _get_asset_type(clients: AssetType._Clients, rid: str) -> asset_pb2.Type:
    with translate_grpc_errors():
        response = clients.assets.GetTypes(asset_pb2.GetTypesRequest(rids=[rid]))
    if rid not in response.responses:
        raise NominalNotFoundError(f"No asset type found with RID {rid!r}")
    return response.responses[rid]


def _search_asset_types(
    clients: AssetType._Clients,
    search_text: str | None,
    workspace_rid: str | None,
    archive_status: ArchiveStatusFilter,
) -> Iterable[AssetType]:
    queries = []
    if search_text is not None:
        queries.append(asset_pb2.SearchTypesQuery(search_text=search_text))
    if workspace_rid is not None:
        queries.append(asset_pb2.SearchTypesQuery(workspace=workspace_rid))
    query = asset_pb2.SearchTypesQuery(
        **{"and": asset_pb2.SearchTypesQueryList(queries=queries)}  # type: ignore[arg-type]
    )

    def request(page_token: str | None) -> asset_pb2.SearchTypesRequest:
        return asset_pb2.SearchTypesRequest(
            query=query,
            sort=asset_pb2.TypeSortOptions(field=asset_pb2.TypeSortOptions.CREATED_AT, is_descending=True),
            archived_statuses=asset_pb2.ArchivedStatusSet(
                archived_statuses=archive_status.to_proto_archived_statuses()
            ),
            page_size=DEFAULT_PAGE_SIZE,
            next_page_token=page_token,
        )

    for response in paginate_grpc(clients.assets.SearchTypes, request_factory=request):
        for raw in response.results:
            yield AssetType._from_proto(clients, raw)
