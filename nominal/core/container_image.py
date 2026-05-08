from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from typing_extensions import Self

from nominal.core._clientsbunch import ClientsBunch
from nominal.core._utils.api_tools import HasRid
from nominal.core._utils.multipart import put_multipart_upload

logger = logging.getLogger(__name__)

_TAR_MIMETYPE = "application/x-tar"
_REGISTRY_IMAGES_PATH = "/registry/v1/images"


@dataclass(frozen=True)
class ContainerImage(HasRid):
    """A docker image uploaded to Nominal's self-hosted container registry.

    Container images are referenced by `containerImageRid` in containerized extractor registration
    requests, as an alternative to pulling from an external docker registry.
    """

    rid: str
    name: str
    tag: str

    @classmethod
    def _from_response(cls, body: dict[str, Any]) -> Self:
        return cls(rid=body["rid"], name=body["name"], tag=body["tag"])


def upload_container_image(
    clients: ClientsBunch,
    *,
    name: str,
    tag: str,
    file: Path,
    workspace_rid: str | None = None,
) -> ContainerImage:
    """Upload a docker image tarball to Nominal's self-hosted container registry.

    The tarball must be the uncompressed output of `docker save` (or an equivalent OCI tar). The
    image is keyed by (workspace, name, tag); re-uploading the same tag replaces it.

    Args:
        clients: Authenticated client bundle.
        name: Image name (e.g. the package name).
        tag: Image tag, typically a git short SHA.
        file: Path to the uncompressed tarball.
        workspace_rid: Workspace to upload into. Defaults to the client's default workspace.

    Returns:
        The newly created ContainerImage.
    """
    resolved_workspace = workspace_rid or clients.resolve_default_workspace_rid()

    # Use a unique-per-tag filename so the multipart object key cannot collide across tags.
    upload_filename = f"{name}-{tag}.tar"
    with file.open("rb") as f:
        object_path = put_multipart_upload(
            auth_header=clients.auth_header,
            workspace_rid=resolved_workspace,
            f=f,
            filename=upload_filename,
            mimetype=_TAR_MIMETYPE,
            upload_client=clients.upload,
            header_provider=clients.header_provider,
        )
    logger.info("uploaded container image tarball to %s", object_path)

    # The conjure service for the image registry (POST /registry/v1/images) is not yet generated
    # into nominal_api; fall back to raw HTTP. Replace with a typed conjure call once available.
    response = requests.post(
        f"{clients._api_base_url.rstrip('/')}{_REGISTRY_IMAGES_PATH}",
        headers={
            "Authorization": clients.auth_header,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": clients._user_agent,
        },
        json={
            "workspaceRid": resolved_workspace,
            "name": name,
            "tag": tag,
            "objectPath": object_path,
        },
    )
    if not response.ok:
        raise RuntimeError(
            f"failed to register container image: {response.status_code} {response.reason}: {response.text}"
        )
    body = response.json()
    image_body = body.get("image")
    if not isinstance(image_body, dict):
        raise RuntimeError(f"unexpected create-image response: {body!r}")
    return ContainerImage._from_response(image_body)
