from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, Self, Sequence

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin, rid_from_instance_or_string
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import DEFAULT_PAGE_SIZE, paginate_grpc
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2, asset_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC


class AssetTypePropertyValueType(Enum):
    """The kind of value accepted by an asset-type property rule."""

    STRING = "STRING"
    NUMBER = "NUMBER"

    def _to_proto(self) -> asset_pb2.PropertyValueType.ValueType:
        return {
            AssetTypePropertyValueType.STRING: asset_pb2.STRING,
            AssetTypePropertyValueType.NUMBER: asset_pb2.NUMBER,
        }[self]

    @classmethod
    def _from_proto(cls, value: asset_pb2.PropertyValueType.ValueType) -> AssetTypePropertyValueType:
        match value:
            case asset_pb2.STRING:
                return cls.STRING
            case asset_pb2.NUMBER:
                return cls.NUMBER
            case _:
                raise ValueError(f"Unknown asset-type property value type: {value}")


@dataclass(frozen=True)
class AssetTypeProperty:
    """A string or numeric property rule for members of an asset type.

    Suggested properties are optional; required properties must be present. The
    type's primary key is a separate, required nonempty string property.
    """

    value_type: AssetTypePropertyValueType = AssetTypePropertyValueType.STRING
    required: bool = False

    def _to_proto(self) -> asset_pb2.PropertyConfig:
        return asset_pb2.PropertyConfig(
            requirement=asset_pb2.REQUIRED if self.required else asset_pb2.SUGGESTED,
            value_type=self.value_type._to_proto(),
        )

    @classmethod
    def _from_proto(cls, value: asset_pb2.PropertyConfig) -> Self:
        value_type = AssetTypePropertyValueType._from_proto(value.value_type)
        if value.requirement not in (asset_pb2.REQUIRED, asset_pb2.SUGGESTED):
            raise ValueError(f"Unknown asset-type property requirement: {value.requirement}")
        return cls(value_type=value_type, required=value.requirement == asset_pb2.REQUIRED)


class AssetTypeSeriesType(Enum):
    """The kind of channel required by an asset type.

    NUMERIC includes floating-point, signed integer, and unsigned integer channels.
    """

    NUMERIC = "NUMERIC"
    STRING = "STRING"
    LOG = "LOG"
    NUMERIC_ARRAY = "NUMERIC_ARRAY"
    STRING_ARRAY = "STRING_ARRAY"
    STRUCT = "STRUCT"

    @classmethod
    def _from_proto(cls, value: asset_pb2.SeriesRequirement.SeriesType.ValueType) -> Self:
        return cls(asset_pb2.SeriesRequirement.SeriesType.Name(value))


def _data_requirements_to_proto(
    required_series: Mapping[str, AssetTypeSeriesType] | None,
) -> asset_pb2.DataRequirements | None:
    if required_series is None:
        return None
    return asset_pb2.DataRequirements(
        series=asset_pb2.SeriesRequirements(
            series=[
                asset_pb2.SeriesRequirement(name=name, type=asset_pb2.SeriesRequirement.SeriesType.Value(kind.value))
                for name, kind in required_series.items()
            ]
        )
    )


@dataclass(frozen=True)
class AssetTypeLink:
    """Link a source string property to the primary key of a target asset type.

    Omit ``rid`` when defining a new link. Saved links include their RID and
    read-only source type RID. Labels name the relationship from either end.
    """

    target_type_rid: str
    source_property: str
    forward_label: str | None = None
    inverse_label: str | None = None
    rid: str | None = None
    source_type_rid: str | None = None

    def _to_proto(self) -> asset_pb2.LinkDefinition:
        return asset_pb2.LinkDefinition(
            rid=self.rid,
            target_type_rid=self.target_type_rid,
            forward_label=self.forward_label,
            inverse_label=self.inverse_label,
            direct=asset_pb2.DirectLink(source_property=self.source_property),
        )

    @classmethod
    def _from_proto(cls, raw: asset_pb2.LinkDefinition) -> Self:
        if raw.WhichOneof("kind") != "direct":
            raise ValueError(f"Unknown asset-type link kind for link {raw.rid!r}")
        return cls(
            rid=raw.rid if raw.HasField("rid") else None,
            source_type_rid=raw.source_type_rid if raw.HasField("source_type_rid") else None,
            target_type_rid=raw.target_type_rid,
            forward_label=raw.forward_label if raw.HasField("forward_label") else None,
            inverse_label=raw.inverse_label if raw.HasField("inverse_label") else None,
            source_property=raw.direct.source_property,
        )


@dataclass(frozen=True)
class AssetType(HasRid, RefreshableGrpcMixin[asset_pb2.Type]):
    """An asset identity definition and the property rules its members must satisfy.

    The primary key names a required nonempty string property, unique among active
    assets of this type. Other property definitions are stored in ``properties``.

    Required series are checked when the type is attached. A type with no data
    requirements ever set has ``required_series=None``. Incoming links are read-only.

    Reading an unsupported property rule, data requirement, series type, or link
    raises ``ValueError``, including during search, refresh, or update.
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
    required_series: Mapping[str, AssetTypeSeriesType] | None = None
    outgoing_links: tuple[AssetTypeLink, ...] = ()
    incoming_links: tuple[AssetTypeLink, ...] = ()

    class _Clients(HasScoutParams, Protocol):
        @property
        def assets(self) -> asset_pb2_grpc.AssetServiceStub: ...

    @property
    def nominal_url(self) -> str:
        """Link to this asset type in the Nominal app."""
        # Type responses omit workspace; the legacy route resolves it from the RID.
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
        required_series: Mapping[str, AssetTypeSeriesType] | None = None,
        outgoing_links: Sequence[AssetTypeLink] | None = None,
    ) -> Self:
        """Replace supplied definition fields and refresh this instance in place.

        ``None`` leaves a field unchanged; an empty properties mapping removes all
        additional property rules. The primary key is separate from those rules.
        The server rejects property rules incompatible with existing active
        members and primary-key renames once the type has memberships.

        Pass an empty mapping to clear required series. Existing members are
        not rechecked. Supply the full outgoing link list to replace it, or an
        empty list to clear it. Retain a link's RID to update its labels.

        Raises:
            NominalError: If the definition is invalid or the update fails.
            ValueError: If the update succeeded but the SDK could not read the returned definition.
                This instance retains its previous snapshot.
        """
        request = asset_pb2.UpdateTypeRequest(
            type_rid=self.rid,
            data_requirements=_data_requirements_to_proto(required_series),
            links=None
            if outgoing_links is None
            else asset_pb2.LinkDefinitionList(links=[link._to_proto() for link in outgoing_links]),
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
        try:
            return self._refresh_from_api(response.type)
        except ValueError as exc:
            raise ValueError(
                f"Asset type {self.rid!r} was updated, but the local snapshot could not be refreshed: {exc}"
            ) from exc

    def delete(self) -> None:
        """Permanently delete this type and its incoming and outgoing link definitions.

        Raises:
            NominalPermissionDeniedError: If an asset uses the type, including an archived asset.
            NominalFailedPreconditionError: If a checklist references the type.
        """
        with translate_grpc_errors():
            self._clients.assets.DeleteType(asset_pb2.DeleteTypeRequest(type_rid=self.rid))

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
        required_series = None
        if raw.HasField("data_requirements"):
            if raw.data_requirements.WhichOneof("requirements") != "series":
                raise ValueError(f"Unknown data requirements for asset type {raw.rid!r}")
            required_series = MappingProxyType(
                {item.name: AssetTypeSeriesType._from_proto(item.type) for item in raw.data_requirements.series.series}
            )
        return cls(
            required_series=required_series,
            outgoing_links=tuple(AssetTypeLink._from_proto(link) for link in raw.outgoing_links),
            incoming_links=tuple(AssetTypeLink._from_proto(link) for link in raw.incoming_links),
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


def _search_type_rids(types: AssetType | str | Sequence[AssetType | str] | None) -> list[str] | None:
    if types is None:
        return None
    if isinstance(types, (AssetType, str)):
        return [rid_from_instance_or_string(types)]
    return [rid_from_instance_or_string(kind) for kind in types]


def _get_asset_type(clients: AssetType._Clients, rid: str) -> asset_pb2.Type:
    return _get_asset_types(clients, [rid])[0]


def _get_asset_types(clients: AssetType._Clients, rids: Sequence[str]) -> Sequence[asset_pb2.Type]:
    """Fetch all requested definitions in input order, failing if any are missing."""
    if not rids:
        return []
    with translate_grpc_errors():
        response = clients.assets.GetTypes(asset_pb2.GetTypesRequest(rids=list(rids)))
    missing = set(rids) - response.responses.keys()
    if missing:
        raise NominalNotFoundError(f"Asset types not found: {sorted(missing)!r}")
    return [response.responses[rid] for rid in rids]


def _search_asset_types(
    clients: AssetType._Clients,
    search_text: str | None,
    workspace_rid: str | None,
    archive_status: ArchiveStatusFilter,
    property_names: str | Sequence[str],
) -> Iterable[AssetType]:
    queries = []
    if search_text is not None:
        queries.append(asset_pb2.SearchTypesQuery(search_text=search_text))
    if workspace_rid is not None:
        queries.append(asset_pb2.SearchTypesQuery(workspace=workspace_rid))
    names = (property_names,) if isinstance(property_names, str) else property_names
    queries.extend(asset_pb2.SearchTypesQuery(property=name) for name in names)
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
