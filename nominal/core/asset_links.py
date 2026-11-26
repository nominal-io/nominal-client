from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from nominal.core._utils.pagination_tools import DEFAULT_PAGE_SIZE, paginate_grpc
from nominal.protos.asset.v2 import asset_pb2, asset_pb2_grpc


@dataclass(frozen=True)
class ResolvedAssetLink:
    """A requested asset and the visible, active asset reached through a type link."""

    requested_asset_rid: str
    linked_asset_rid: str
    title: str
    primary_key_value: str


@dataclass(frozen=True)
class UnresolvedAssetLink:
    """A source asset and its property value that has no visible, active target."""

    requested_asset_rid: str
    unresolved_value: str


def _traverse_link_batch(
    service: asset_pb2_grpc.AssetServiceStub, link: asset_pb2.LinkRef, asset_rids: Sequence[str]
) -> Iterable[ResolvedAssetLink | UnresolvedAssetLink]:
    def request(page_token: str | None) -> asset_pb2.TraverseLinksRequest:
        return asset_pb2.TraverseLinksRequest(
            asset_rids=asset_rids, link=link, page_size=DEFAULT_PAGE_SIZE, page_token=page_token
        )

    for page in paginate_grpc(service.TraverseLinks, request_factory=request):
        for edge in page.edges:
            match edge.WhichOneof("end"):
                case "linked_asset":
                    linked = edge.linked_asset
                    yield ResolvedAssetLink(
                        edge.requested_asset_rid, linked.rid, linked.title, linked.primary_key_value
                    )
                case "unresolved_value":
                    yield UnresolvedAssetLink(edge.requested_asset_rid, edge.unresolved_value)
                case _:
                    raise ValueError(f"Unknown link edge endpoint for asset {edge.requested_asset_rid!r}")
