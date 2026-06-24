"""Nominal Hosted (v2) containerized extractors and their container images.

A Nominal Hosted extractor runs a container image that Nominal stores in its internal registry.
The extractor itself carries only identity (name, description, archived flag); its execution
contract -- inputs, parameters, output format, default timestamp metadata -- lives on the
container images registered against it, exactly one of which is *active* at a time.

This module follows the repo's resource-object pattern: :class:`NominalHostedExtractor` and
:class:`ContainerImage` are immutable handles with instance methods for the operations on them,
and they are created/fetched/searched via :class:`nominal.core.NominalClient`. Everything is
wired through the v2 gRPC services -- ``nominal.ingest.v2.ContainerizedExtractorService`` and
``nominal.registry.v2.RegistryService`` -- exposed as shared-channel stubs on ``ClientsBunch``
(``nominal_hosted_extractors`` and ``registry``); auth, retry, and deadlines are applied by the
channel interceptors, so call sites just invoke the stub methods.

The extractor *schema* leaf types (:data:`FileExtractionInput`, :data:`FileExtractionParameter`,
:data:`FileOutputFormat`, :data:`TimestampMetadata`, :data:`ContainerImageStatus`) are the
generated protobuf messages from the ``registry.v2`` package, re-exported here so callers can
import them from one place.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Sequence

from typing_extensions import Self

from nominal._utils.dataclass_tools import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid
from nominal.protos.ingest.v2 import containerized_extractor_pb2 as _extractor_pb2
from nominal.protos.ingest.v2 import containerized_extractor_pb2_grpc as _extractor_grpc
from nominal.protos.registry.v2 import registry_pb2 as _registry_pb2
from nominal.protos.registry.v2 import registry_pb2_grpc as _registry_grpc

# The extractor schema leaf types and the image/search messages are the generated protobuf
# messages from the registry.v2 package. Re-export the leaf types here for one-import convenience.
FileExtractionInput = _registry_pb2.FileExtractionInput
FileExtractionParameter = _registry_pb2.FileExtractionParameter
FileOutputFormat = _registry_pb2.FileOutputFormat
TimestampMetadata = _registry_pb2.TimestampMetadata
ContainerImageStatus = _registry_pb2.ContainerImageStatus
# Filter for searching container images.
SearchFilter = _registry_pb2.SearchFilter

_DEFAULT_PAGE_SIZE = 100


class _Clients(HasScoutParams, Protocol):
    @property
    def nominal_hosted_extractors(self) -> _extractor_grpc.ContainerizedExtractorServiceStub: ...
    @property
    def registry(self) -> _registry_grpc.RegistryServiceStub: ...


@dataclass(frozen=True)
class ContainerImage(HasRid):
    """A container image registered against a Nominal Hosted extractor (``nominal.registry.v2``).

    Carries the execution contract for the extractor when active. Newly registered images start
    ``CONTAINER_IMAGE_STATUS_PENDING``; poll :meth:`NominalHostedExtractor.get_image` until the
    ``status`` reaches ``CONTAINER_IMAGE_STATUS_READY`` before activating it.
    """

    rid: str
    tag: str
    status: ContainerImageStatus
    extractor_rid: str
    _workspace_rid: str = field(repr=False)
    _clients: _Clients = field(repr=False)

    def delete(self) -> None:
        """Delete this image. Fails if an extractor still references it."""
        self._clients.registry.DeleteImage(
            _registry_pb2.DeleteImageRequest(rid=self.rid, workspace_rid=self._workspace_rid)
        )

    @classmethod
    def _from_proto(cls, clients: _Clients, workspace_rid: str, image: _registry_pb2.ContainerImage) -> Self:
        return cls(
            rid=image.rid,
            tag=image.tag,
            status=image.status,
            extractor_rid=image.extractor_rid,
            _workspace_rid=workspace_rid,
            _clients=clients,
        )

    @classmethod
    def _search(
        cls,
        clients: _Clients,
        *,
        filter: SearchFilter | None = None,
        workspace_rid: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> Sequence[Self]:
        resolved_workspace = workspace_rid if workspace_rid is not None else clients.resolve_default_workspace_rid()
        images: list[Self] = []
        next_page_token = ""
        while True:
            request = _registry_pb2.SearchImagesRequest(workspace_rid=resolved_workspace, page_size=page_size)
            if filter is not None:
                request.filter.CopyFrom(filter)
            if next_page_token:
                request.next_page_token = next_page_token
            response = clients.registry.SearchImages(request)
            images.extend(cls._from_proto(clients, resolved_workspace, image) for image in response.images)
            if not response.next_page_token:
                return images
            next_page_token = response.next_page_token


@dataclass(frozen=True)
class NominalHostedExtractor(HasRid):
    """A Nominal Hosted containerized extractor (``nominal.ingest.v2``).

    Identity only: the execution contract (inputs, parameters, output format, timestamp metadata)
    lives on the container images registered against it via :meth:`register_image`, exactly one of
    which is active at a time (:meth:`set_active_image`). Until an image is active, ingests fail.
    """

    rid: str
    name: str
    description: str | None
    is_archived: bool
    active_container_image_rid: str | None
    _workspace_rid: str = field(repr=False)
    _clients: _Clients = field(repr=False)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        is_archived: bool | None = None,
        active_container_image_rid: str | None = None,
    ) -> Self:
        """Partial update -- only the fields you pass are modified.

        Returns this instance with its fields refreshed from the server response.
        """
        request = _extractor_pb2.UpdateContainerizedExtractorRequest(rid=self.rid, workspace_rid=self._workspace_rid)
        if name is not None:
            request.name = name
        if description is not None:
            request.description = description
        if is_archived is not None:
            request.is_archived = is_archived
        if active_container_image_rid is not None:
            request.active_container_image_rid = active_container_image_rid
        response = self._clients.nominal_hosted_extractors.UpdateContainerizedExtractor(request)
        update_dataclass(self, self._from_proto(self._clients, response.extractor), self.__dataclass_fields__)
        return self

    def archive(self) -> Self:
        """Archive this extractor (hidden from default search, rejects new ingests)."""
        return self.update(is_archived=True)

    def unarchive(self) -> Self:
        """Restore a previously archived extractor."""
        return self.update(is_archived=False)

    def set_active_image(self, image: ContainerImage | str) -> Self:
        """Select the image this extractor runs. The image must be ``READY`` and built for it."""
        image_rid = image.rid if isinstance(image, ContainerImage) else image
        return self.update(active_container_image_rid=image_rid)

    def register_image(
        self,
        *,
        tag: str,
        object_path: str,
        inputs: Sequence[FileExtractionInput],
        file_output_format: FileOutputFormat,
        default_timestamp_metadata: TimestampMetadata,
        parameters: Sequence[FileExtractionParameter] = (),
    ) -> ContainerImage:
        """Register a previously uploaded image tarball and start its push into the registry.

        Upload the ``docker save`` tarball via the upload API first and pass its object-storage
        path as ``object_path``. The returned image starts ``PENDING``; poll :meth:`get_image`
        until it reaches ``READY``, then :meth:`set_active_image`.
        """
        request = _registry_pb2.CreateImageRequest(
            workspace_rid=self._workspace_rid,
            tag=tag,
            object_path=object_path,
            extractor_rid=self.rid,
            inputs=list(inputs),
            parameters=list(parameters),
            file_output_format=file_output_format,
            default_timestamp_metadata=default_timestamp_metadata,
        )
        response = self._clients.registry.CreateImage(request)
        return ContainerImage._from_proto(self._clients, self._workspace_rid, response.image)

    def get_image(self, rid: str) -> ContainerImage:
        """Fetch a container image registered against this extractor, including its push ``status``."""
        response = self._clients.registry.GetImage(
            _registry_pb2.GetImageRequest(rid=rid, workspace_rid=self._workspace_rid)
        )
        return ContainerImage._from_proto(self._clients, self._workspace_rid, response.image)

    @classmethod
    def _create(cls, clients: _Clients, name: str, *, description: str | None, workspace_rid: str | None) -> Self:
        resolved_workspace = workspace_rid if workspace_rid is not None else clients.resolve_default_workspace_rid()
        request = _extractor_pb2.CreateContainerizedExtractorRequest(workspace_rid=resolved_workspace, name=name)
        if description is not None:
            request.description = description
        response = clients.nominal_hosted_extractors.CreateContainerizedExtractor(request)
        return cls._from_proto(clients, response.extractor)

    @classmethod
    def _get(cls, clients: _Clients, rid: str, *, workspace_rid: str | None) -> Self:
        resolved_workspace = workspace_rid if workspace_rid is not None else clients.resolve_default_workspace_rid()
        response = clients.nominal_hosted_extractors.GetContainerizedExtractor(
            _extractor_pb2.GetContainerizedExtractorRequest(rid=rid, workspace_rid=resolved_workspace)
        )
        return cls._from_proto(clients, response.extractor)

    @classmethod
    def _search(
        cls,
        clients: _Clients,
        *,
        include_archived: bool,
        file_extension: str | None,
        workspace_rid: str | None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> Sequence[Self]:
        resolved_workspace = workspace_rid if workspace_rid is not None else clients.resolve_default_workspace_rid()
        extractors: list[Self] = []
        next_page_token = ""
        while True:
            request = _extractor_pb2.SearchContainerizedExtractorsRequest(
                workspace_rid=resolved_workspace,
                include_archived=include_archived,
                page_size=page_size,
            )
            if file_extension is not None:
                request.file_extension = file_extension
            if next_page_token:
                request.next_page_token = next_page_token
            response = clients.nominal_hosted_extractors.SearchContainerizedExtractors(request)
            extractors.extend(cls._from_proto(clients, extractor) for extractor in response.extractors)
            if not response.next_page_token:
                return extractors
            next_page_token = response.next_page_token

    @classmethod
    def _from_proto(cls, clients: _Clients, extractor: _extractor_pb2.ContainerizedExtractor) -> Self:
        active_image_rid = (
            extractor.active_container_image.rid if extractor.HasField("active_container_image") else None
        )
        return cls(
            rid=extractor.rid,
            name=extractor.name,
            description=extractor.description if extractor.HasField("description") else None,
            is_archived=extractor.is_archived,
            active_container_image_rid=active_image_rid,
            _workspace_rid=extractor.workspace_rid,
            _clients=clients,
        )
