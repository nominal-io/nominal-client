from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from nominal_api import ingest_api

from nominal.cli.containerized_extractor import containerized_extractor_cmd
from nominal.core.client import WorkspaceSearchType
from nominal.core.containerized_extractors import ContainerizedExtractor, DockerImageSource, TagDetails


def _docker_image() -> DockerImageSource:
    return DockerImageSource(
        registry="registry.nominal.test",
        repository="extractor",
        tag_details=TagDetails(tags=["abc123"], default_tag="abc123"),
        authentication=None,
        command=None,
    )


def _raw_docker_image() -> ingest_api.DockerImageSource:
    return ingest_api.DockerImageSource(
        authentication=ingest_api.Authentication(public=ingest_api.PublicAuthentication()),
        registry="registry.nominal.test",
        repository="extractor",
        tag_details=ingest_api.TagDetails(default_tag="abc123", tags=["abc123"]),
    )


def _raw_extractor(
    *,
    rid: str = "ri.extractor.test.1",
    name: str = "updated",
    description: str | None = "updated description",
    labels: list[str] | None = None,
    properties: dict[str, str] | None = None,
    container_image_rid: str | None = "ri.container-image.test.1",
) -> ingest_api.ContainerizedExtractor:
    return ingest_api.ContainerizedExtractor(
        created_at="2026-05-13T00:00:00Z",
        image=_raw_docker_image(),
        inputs=[],
        is_archived=False,
        labels=[] if labels is None else labels,
        name=name,
        output_file_format=ingest_api.FileOutputFormat.PARQUET,
        parameters=[],
        properties={} if properties is None else properties,
        rid=rid,
        container_image_rid=container_image_rid,
        description=description,
    )


def _extractor(
    *,
    clients: SimpleNamespace | None = None,
    rid: str = "ri.extractor.test.1",
    name: str = "extractor",
    container_image_rid: str | None = "ri.container-image.test.1",
) -> ContainerizedExtractor:
    return ContainerizedExtractor(
        rid=rid,
        name=name,
        description="description",
        image=_docker_image(),
        container_image_rid=container_image_rid,
        inputs=[],
        parameters=[],
        properties={"team": "ingest"},
        labels=["prod"],
        default_timestamp_metadata=None,
        _clients=clients if clients is not None else _clients(),
    )


def _clients() -> SimpleNamespace:
    return SimpleNamespace(
        auth_header="Bearer token",
        resolve_default_workspace_rid=MagicMock(return_value="ri.workspace.default"),
        containerized_extractors=MagicMock(),
    )


def _client(**attrs: object) -> SimpleNamespace:
    return SimpleNamespace(**attrs)


def _invoke(command: list[str], *, client: SimpleNamespace, input_text: str | None = None):
    with patch("nominal.cli.util.global_decorators.NominalClient.from_profile", return_value=client):
        return CliRunner().invoke(containerized_extractor_cmd, command, input=input_text)


def test_register_reads_stdin_applies_overrides_and_uses_profile_workspace() -> None:
    clients = _clients()
    clients.containerized_extractors.register_containerized_extractor.return_value = SimpleNamespace(
        extractor_rid="ri.extractor.test.new"
    )
    payload = {
        "name": "from-json",
        "containerImageRid": "ri.container-image.test.old",
        "inputs": [],
        "labels": ["prod"],
        "parameters": [],
        "properties": {"team": "ingest"},
    }

    result = _invoke(
        [
            "register",
            "--profile",
            "test",
            "--name",
            "override-name",
            "--container-image-rid",
            "ri.container-image.test.new",
        ],
        client=_client(_clients=clients),
        input_text=json.dumps(payload),
    )

    assert result.exit_code == 0
    assert result.output == "ri.extractor.test.new\n"
    call = clients.containerized_extractors.register_containerized_extractor.call_args
    assert call.args[0] == "Bearer token"
    request = call.args[1]
    assert request.name == "override-name"
    assert request.container_image_rid == "ri.container-image.test.new"
    assert request.workspace == "ri.workspace.default"
    assert request.labels == ["prod"]
    assert request.properties == {"team": "ingest"}


def test_register_rejects_invalid_json() -> None:
    result = _invoke(
        ["register", "--profile", "test"],
        client=_client(_clients=_clients()),
        input_text="{",
    )

    assert result.exit_code != 0
    assert "invalid JSON" in result.output


def test_register_rejects_workspace_from_json() -> None:
    payload = {
        "name": "extractor",
        "containerImageRid": "ri.container-image.test.1",
        "workspace": "ri.workspace.bad",
        "inputs": [],
        "labels": [],
        "parameters": [],
        "properties": {},
    }

    result = _invoke(
        ["register", "--profile", "test"],
        client=_client(_clients=_clients()),
        input_text=json.dumps(payload),
    )

    assert result.exit_code != 0
    assert "workspace" in result.output
    assert "active config profile" in result.output


def test_search_emits_json_and_forwards_filters_to_client() -> None:
    client = _client(search_containerized_extractors=MagicMock(return_value=[_extractor()]))

    result = _invoke(
        [
            "search",
            "--profile",
            "test",
            "--search-text",
            "extractor",
            "--label",
            "prod",
            "--property",
            "team=ingest",
            "--workspace",
            "default",
            "--format",
            "json",
        ],
        client=client,
    )

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "rid": "ri.extractor.test.1",
        "name": "extractor",
        "description": "description",
        "image": {
            "authentication": {"public": {}, "type": "public"},
            "command": None,
            "registry": "registry.nominal.test",
            "repository": "extractor",
            "tagDetails": {"defaultTag": "abc123", "tags": ["abc123"]},
        },
        "containerImageRid": "ri.container-image.test.1",
        "inputs": [],
        "labels": ["prod"],
        "properties": {"team": "ingest"},
        "timestampMetadata": None,
    }
    client.search_containerized_extractors.assert_called_once_with(
        search_text="extractor",
        labels=["prod"],
        properties={"team": "ingest"},
        workspace=WorkspaceSearchType.DEFAULT,
    )


def test_update_emits_updated_json_and_forwards_replacement_fields() -> None:
    clients = _clients()
    clients.containerized_extractors.update_containerized_extractor.return_value = _raw_extractor(
        name="updated",
        labels=["new"],
        properties={},
    )
    extractor = _extractor(clients=clients)
    client = _client(get_containerized_extractor=MagicMock(return_value=extractor))

    result = _invoke(
        [
            "update",
            "--profile",
            "test",
            "--rid",
            "ri.extractor.test.1",
            "--name",
            "updated",
            "--label",
            "new",
            "--clear-properties",
            "--tag",
            "abc123",
            "--default-tag",
            "abc123",
            "--format",
            "json",
        ],
        client=client,
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["name"] == "updated"
    client.get_containerized_extractor.assert_called_once_with("ri.extractor.test.1")
    call = clients.containerized_extractors.update_containerized_extractor.call_args
    assert call.args[:2] == ("Bearer token", "ri.extractor.test.1")
    request = call.args[2]
    assert request.name == "updated"
    assert request.labels == ["new"]
    assert request.properties == {}
    assert request.tags == ["abc123"]
    assert request.default_tag == "abc123"


def test_archive_and_unarchive_emit_status_and_call_resource_methods() -> None:
    clients = _clients()
    extractor = _extractor(clients=clients)
    client = _client(get_containerized_extractor=MagicMock(return_value=extractor))

    archive_result = _invoke(
        ["archive", "--profile", "test", "--rid", "ri.extractor.test.1"],
        client=client,
    )
    unarchive_result = _invoke(
        ["unarchive", "--profile", "test", "--rid", "ri.extractor.test.1"],
        client=client,
    )

    assert archive_result.exit_code == 0
    assert archive_result.output == "archived ri.extractor.test.1\n"
    assert unarchive_result.exit_code == 0
    assert unarchive_result.output == "unarchived ri.extractor.test.1\n"
    assert clients.containerized_extractors.archive_containerized_extractor.call_args.args == (
        "Bearer token",
        "ri.extractor.test.1",
    )
    assert clients.containerized_extractors.unarchive_containerized_extractor.call_args.args == (
        "Bearer token",
        "ri.extractor.test.1",
    )
