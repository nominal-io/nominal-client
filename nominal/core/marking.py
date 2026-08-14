from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Protocol, Sequence

from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin, rid_from_instance_or_string
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_markings_paginated
from nominal.core._utils.query_tools import create_search_markings_query
from nominal.core.elements import Color, Symbol
from nominal.core.exceptions import NominalNotFoundError
from nominal.protos.authorization.markings.v1 import markings_pb2, markings_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

_MARKING_ID = re.compile(r"^[a-z][a-z0-9-]*$")

# Sentinel distinguishing "leave this field alone" from "clear this field" (None) on update.
_UNCHANGED: Any = object()


def _validate_marking_id(id: str) -> str:
    if _MARKING_ID.match(id) is None:
        raise ValueError(
            f"marking id must be lowercase alphanumeric, optionally hyphen-separated, and start with a letter "
            f"(e.g. 'export-controlled'), got {id!r}"
        )
    return id


@dataclass(frozen=True)
class Marking(HasRid, RefreshableGrpcMixin[markings_pb2.Marking]):
    """A marking, restricting access to the data sources it is applied to.

    Markings are scoped to an organization. Creating, updating, and archiving markings requires
    organization admin permissions; any member of the organization can read them.
    """

    rid: str
    id: str
    """Human-readable identifier, unique within the organization. Markings have no separate name."""
    description: str
    symbol: Symbol | None
    color: Color | None
    created_at: IntegralNanosecondsUTC
    updated_at: IntegralNanosecondsUTC
    is_archived: bool

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def markings(self) -> markings_pb2_grpc.MarkingServiceStub: ...

    def _get_latest_api(self) -> markings_pb2.Marking:
        return _get_marking_proto(self._clients, self.rid)

    def authorized_group_rids(self) -> Sequence[str]:
        """The RIDs of groups authorized to access data sources carrying this marking.

        Groups the user does not have permission to read are omitted.

        Raises:
            NominalError: If the request fails.
        """
        request = markings_pb2.GetAuthorizedGroupsByMarkingRequest(marking_rids=[self.rid])
        with translate_grpc_errors():
            response = self._clients.markings.GetAuthorizedGroupsByMarking(request)
        return tuple(response.authorized_groups_by_marking[self.rid].group_rids)

    def update(
        self,
        *,
        id: str | None = None,
        description: str | None = None,
        authorized_group_rids: Sequence[str] | None = None,
        symbol: Symbol | None = _UNCHANGED,
        color: Color | None = _UNCHANGED,
    ) -> Self:
        """Replace marking metadata, updating the current instance and returning it.

        Args:
            id: New human-readable identifier. Must be unique within the organization.
            description: New description for the marking.
            authorized_group_rids: Group RIDs replacing the existing ones. An empty sequence clears them.
            symbol: New symbol. Pass None to remove the marking's symbol.
            color: New color. Pass None to remove the marking's color.

        Returns:
            This marking, updated in place.

        Note:
            Every argument left unset is omitted from the request and the corresponding field is left
            unchanged. That is distinct from passing None to `symbol`/`color` or an empty sequence to
            `authorized_group_rids`, which clear those fields.

        Raises:
            ValueError: If `id` is not a valid marking id.
            NominalError: If the update request fails, including when the marking is archived.
        """
        request = markings_pb2.UpdateMarkingRequest(
            rid=self.rid,
            id=None if id is None else _validate_marking_id(id),
            description=description,
            authorized_groups=(
                None
                if authorized_group_rids is None
                else markings_pb2.AuthorizedGroups(group_rids=list(authorized_group_rids))
            ),
            symbol=(
                None
                if symbol is _UNCHANGED
                else markings_pb2.UpdateMarkingRequest.UpdateMarkingSymbolWrapper(
                    value=None if symbol is None else symbol._to_proto()
                )
            ),
            color=(
                None
                if color is _UNCHANGED
                else markings_pb2.UpdateMarkingRequest.UpdateMarkingColorWrapper(
                    value=None if color is None else color._to_proto()
                )
            ),
        )
        with translate_grpc_errors():
            response = self._clients.markings.UpdateMarking(request)
        return self._refresh_from_api(response.marking)

    def archive(self) -> None:
        """Archive the marking, preventing it from being applied or modified.

        Archived markings are excluded from search but can still be retrieved by RID. Archiving fails
        if the marking is still applied to any resource.

        Note: this does not update the instance in place; call `refresh()` to see the change reflected.
        """
        with translate_grpc_errors():
            self._clients.markings.ArchiveMarkings(markings_pb2.ArchiveMarkingsRequest(marking_rids=[self.rid]))

    def unarchive(self) -> None:
        """Unarchive the marking, restoring the ability to apply and modify it.

        Note: this does not update the instance in place; call `refresh()` to see the change reflected.
        """
        with translate_grpc_errors():
            self._clients.markings.UnarchiveMarkings(markings_pb2.UnarchiveMarkingsRequest(marking_rids=[self.rid]))

    @classmethod
    def _from_proto(cls, clients: _Clients, marking: markings_pb2.Marking | markings_pb2.MarkingMetadata) -> Self:
        # `Marking` and `MarkingMetadata` carry the same field names (the latter simply omits the
        # authorized groups), so one constructor covers every response shape.
        return cls(
            rid=marking.rid,
            id=marking.id,
            description=marking.description,
            symbol=Symbol._from_proto(marking.symbol),
            color=Color._from_proto(marking.color),
            created_at=marking.created_at.ToNanoseconds(),
            updated_at=marking.updated_at.ToNanoseconds(),
            is_archived=marking.is_archived,
            _clients=clients,
        )


def _create_marking(
    clients: Marking._Clients,
    *,
    id: str,
    description: str | None,
    authorized_group_rids: Sequence[str],
    symbol: Symbol | None,
    color: Color | None,
) -> Marking:
    request = markings_pb2.CreateMarkingRequest(
        id=_validate_marking_id(id),
        description=description or "",
        authorized_groups=markings_pb2.AuthorizedGroups(group_rids=list(authorized_group_rids)),
        symbol=None if symbol is None else symbol._to_proto(),
        color=None if color is None else color._to_proto(),
    )
    with translate_grpc_errors():
        response = clients.markings.CreateMarking(request)
    return Marking._from_proto(clients, response.marking)


def _get_marking_proto(clients: Marking._Clients, rid: str) -> markings_pb2.Marking:
    """The marking with the given rid.

    Raises:
        NominalNotFoundError: If no marking has that rid, or it is not accessible to the user.
    """
    markings = _get_marking_protos(clients, [rid])
    if not markings:
        raise NominalNotFoundError(f"no marking found with RID {rid!r}")
    if len(markings) != 1:
        raise ValueError(f"Expected exactly one marking with rid {rid!r}, received {len(markings)}")
    return markings[0]


def _get_marking_protos(clients: Marking._Clients, rids: Sequence[str]) -> Sequence[markings_pb2.Marking]:
    request = markings_pb2.BatchGetMarkingsRequest(marking_rids=list(rids))
    with translate_grpc_errors():
        return tuple(clients.markings.BatchGetMarkings(request).markings)


def _get_marking(clients: Marking._Clients, rid: str) -> Marking:
    return Marking._from_proto(clients, _get_marking_proto(clients, rid))


def _get_markings(clients: Marking._Clients, rids: Iterable[str]) -> Sequence[Marking]:
    """Markings with the given rids. Markings that do not exist or are not readable are omitted."""
    return tuple(Marking._from_proto(clients, m) for m in _get_marking_protos(clients, list(rids)))


def _get_marking_by_id(clients: Marking._Clients, id: str) -> Marking:
    request = markings_pb2.GetMarkingByIdRequest(id=_validate_marking_id(id))
    with translate_grpc_errors():
        response = clients.markings.GetMarkingById(request)
    return Marking._from_proto(clients, response.marking)


def _get_marking_metadata(clients: Marking._Clients, rids: Sequence[str]) -> Sequence[Marking]:
    """Markings with the given rids, without their authorized groups.

    Cheaper than `_get_markings` when only the marking's metadata is needed.
    """
    if not rids:
        return ()
    request = markings_pb2.BatchGetMarkingMetadataRequest(marking_rids=list(rids))
    with translate_grpc_errors():
        response = clients.markings.BatchGetMarkingMetadata(request)
    return tuple(Marking._from_proto(clients, m) for m in response.marking_metadatas)


def _iter_search_markings(clients: Marking._Clients, query: markings_pb2.SearchMarkingsQuery) -> Iterable[Marking]:
    for marking in search_markings_paginated(clients.markings, query):
        yield Marking._from_proto(clients, marking)


def _search_markings(clients: Marking._Clients, *, id_substring: str | None = None) -> Sequence[Marking]:
    return tuple(_iter_search_markings(clients, create_search_markings_query(id_substring=id_substring)))


class MarkableMixin:
    """Markings applied to a data source.

    Concrete: classes gain these methods by listing this mixin as a base and exposing a `markings`
    client. Markings can only be applied to data sources — datasets, connections, and videos.
    """

    rid: str
    _clients: Marking._Clients

    def list_markings(self) -> Sequence[Marking]:
        """The markings currently applied to this resource.

        Markings the user does not have permission to read are omitted.

        Raises:
            NominalError: If the request fails.
        """
        return _get_marking_metadata(self._clients, _applied_marking_rids(self._clients, self.rid))

    def apply_markings(self, markings: Iterable[Marking | str]) -> None:
        """Apply markings to this resource, leaving any already-applied markings in place.

        Applying a marking that is already applied is a no-op rather than an error.

        Args:
            markings: Markings, or marking RIDs, to apply.

        Raises:
            NominalError: If the request fails, including when a marking is archived or the user
                lacks permission to change markings on this resource.
        """
        _update_markings_on_resource(self._clients, self.rid, apply=markings, remove=())

    def remove_markings(self, markings: Iterable[Marking | str]) -> None:
        """Remove markings from this resource.

        Removing a marking that is not applied is a no-op rather than an error.

        Args:
            markings: Markings, or marking RIDs, to remove.

        Raises:
            NominalError: If the request fails.
        """
        _update_markings_on_resource(self._clients, self.rid, apply=(), remove=markings)

    def set_markings(self, markings: Iterable[Marking | str]) -> None:
        """Replace the markings on this resource with exactly the given markings.

        The difference against the currently-applied markings is computed and sent as a single
        atomic update. An empty sequence removes every marking from the resource.

        Args:
            markings: Markings, or marking RIDs, the resource should carry.

        Raises:
            NominalError: If the request fails.
        """
        desired = {rid_from_instance_or_string(marking) for marking in markings}
        current = set(_applied_marking_rids(self._clients, self.rid))
        to_apply = desired - current
        to_remove = current - desired
        if not to_apply and not to_remove:
            return
        _update_markings_on_resource(self._clients, self.rid, apply=to_apply, remove=to_remove)


def _applied_marking_rids(clients: Marking._Clients, resource_rid: str) -> Sequence[str]:
    request = markings_pb2.GetMarkingsForResourcesRequest(resources=[resource_rid])
    with translate_grpc_errors():
        response = clients.markings.GetMarkingsForResources(request)
    applied = response.resource_to_markings[resource_rid].applied_markings
    return tuple(marking.marking_rid for marking in applied)


def _update_markings_on_resource(
    clients: Marking._Clients,
    resource_rid: str,
    *,
    apply: Iterable[Marking | str],
    remove: Iterable[Marking | str],
) -> None:
    request = markings_pb2.UpdateMarkingsOnResourceRequest(
        resource=resource_rid,
        markings_to_apply=[rid_from_instance_or_string(marking) for marking in apply],
        markings_to_remove=[rid_from_instance_or_string(marking) for marking in remove],
    )
    with translate_grpc_errors():
        clients.markings.UpdateMarkingsOnResource(request)
