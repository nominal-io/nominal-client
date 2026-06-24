from __future__ import annotations

from unittest.mock import MagicMock

from nominal.core.containerized_extractor import ContainerizedExtractor, _search_images
from nominal.protos.ingest.v2 import containerized_extractor_pb2 as ext_pb2
from nominal.protos.registry.v2 import registry_pb2 as reg_pb2


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    return clients


def _ext(rid: str) -> ext_pb2.ContainerizedExtractor:
    return ext_pb2.ContainerizedExtractor(rid=rid, workspace_rid="ri.workspace.default", name=rid, is_archived=False)


def test_search_extractors_follows_pagination_cursors() -> None:
    """Search accumulates results across pages until the server returns an empty next_page_token."""
    clients = _clients()
    page1 = ext_pb2.SearchContainerizedExtractorsResponse(extractors=[_ext("a")], next_page_token="tok")
    page2 = ext_pb2.SearchContainerizedExtractorsResponse(extractors=[_ext("b")], next_page_token="")
    clients.containerized_extractor.SearchContainerizedExtractors.side_effect = [page1, page2]

    results = ContainerizedExtractor._search(clients, include_archived=False, file_extension=None, workspace_rid=None)

    assert [e.rid for e in results] == ["a", "b"]
    assert clients.containerized_extractor.SearchContainerizedExtractors.call_count == 2
    second_call_request = clients.containerized_extractor.SearchContainerizedExtractors.call_args_list[1].args[0]
    assert second_call_request.next_page_token == "tok"


def _img(r: str) -> reg_pb2.ContainerImage:
    return reg_pb2.ContainerImage(rid=r, tag="v1", extractor_rid="ri.ext", file_output_format=1)


def test_create_defaults_workspace_to_client_default() -> None:
    """When workspace_rid is omitted, the request uses the client's resolved default workspace."""
    clients = _clients()
    clients.containerized_extractor.CreateContainerizedExtractor.return_value = (
        ext_pb2.CreateContainerizedExtractorResponse(extractor=_ext("a"))
    )

    ContainerizedExtractor._create(clients, "a", description=None, workspace_rid=None)

    request = clients.containerized_extractor.CreateContainerizedExtractor.call_args.args[0]
    assert request.workspace_rid == "ri.workspace.default"


def test_search_images_follows_pagination_cursors() -> None:
    """Image search accumulates across pages and stops on an empty next_page_token."""
    clients = _clients()
    clients.registry.SearchImages.side_effect = [
        reg_pb2.SearchImagesResponse(images=[_img("i1")], next_page_token="n"),
        reg_pb2.SearchImagesResponse(images=[_img("i2")], next_page_token=""),
    ]

    results = _search_images(clients, filter=None, workspace_rid=None)

    assert [i.rid for i in results] == ["i1", "i2"]
    assert clients.registry.SearchImages.call_count == 2
