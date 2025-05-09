from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence

from nominal_api import ingest_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid, update_dataclass
from nominal.ts import _ConjureTimestampType


@dataclass(frozen=True)
class UserPassAuth:
    """User/password based authentication credentials for pulling a docker image."""

    # Username for the docker registry containing the docker image
    username: str

    # rid of the Secret containing the password for authentication with the docker registry
    password_rid: str

    @classmethod
    def _from_conjure(cls, auth_input: ingest_api.UserAndPasswordAuthentication) -> Self:
        return cls(
            username=auth_input.username,
            password_rid=auth_input.password_secret_rid,
        )

    def _to_conjure(self) -> ingest_api.Authentication:
        return ingest_api.Authentication(
            user_and_password=ingest_api.UserAndPasswordAuthentication(
                password_secret_rid=self.password_rid,
                username=self.username,
            )
        )


@dataclass(frozen=True)
class FileExtractionInput:
    """Configuration for a file extraction input in a containerized extractor."""

    # Name of the file input
    name: str

    # Displayed description of the file input
    description: str | None

    # Environment variable to populate with the input file path within the extractor
    environment_variable: str

    # File suffixes to filter input files when selecting from the local filesystem
    file_suffixes: Sequence[str]

    # Whether or not this input is required to perform extraction
    required: bool | None

    @classmethod
    def _from_conjure(cls, file_input: ingest_api.FileExtractionInput) -> Self:
        file_suffixes = []
        for file_filter in file_input.file_filters:
            if file_filter.suffix is not None:
                file_suffixes.append(file_filter.suffix)
            else:
                raise ValueError(f"Unknown file filter type: {file_filter.type}")

        return cls(
            name=file_input.name,
            description=file_input.description,
            environment_variable=file_input.environment_variable,
            file_suffixes=file_suffixes,
            required=file_input.required,
        )

    def _to_conjure(self) -> ingest_api.FileExtractionInput:
        return ingest_api.FileExtractionInput(
            environment_variable=self.environment_variable,
            file_filters=[ingest_api.FileFilter(suffix=file_suffix) for file_suffix in self.file_suffixes],
            name=self.name,
            description=self.description,
            required=self.required,
        )


@dataclass(frozen=True)
class TagDetails:
    """Details about docker image tags to register for a custom extractor."""

    # All available tags of a docker image to use for a custom extractor
    tags: Sequence[str]

    # Default docker image tag to use with the custom extractor
    default_tag: str

    @classmethod
    def _from_conjure(cls, details: ingest_api.TagDetails) -> Self:
        return cls(
            tags=details.tags,
            default_tag=details.default_tag,
        )

    def _to_conjure(self) -> ingest_api.TagDetails:
        return ingest_api.TagDetails(default_tag=self.default_tag, tags=list(self.tags))


@dataclass(frozen=True)
class DockerImageSource:
    """Details about docker images and their associated registry to register as custom extractors."""

    # Base docker registry name, e.g. `nvidia` for nvidia/cuda https://hub.docker.com/r/nvidia/cuda
    registry: str

    # Docker image name, e.g. `cuda` for nvidia/cuda https://hub.docker.com/r/nvidia/cuda
    repository: str

    # Details on which tag(s) to register as extractors
    tag_details: TagDetails

    # Authentication details for the provided docker registry, or None if no authentication is required
    authentication: UserPassAuth | None

    # Command to run inside the container to start extraction.
    # If None, then the default CMD from the dockerfile is used instead.
    command: str | None

    @classmethod
    def _from_conjure(cls, raw_source: ingest_api.DockerImageSource) -> Self:
        auth = None
        if raw_source.authentication.public is not None:
            auth = None
        elif raw_source.authentication.user_and_password is not None:
            auth = UserPassAuth._from_conjure(raw_source.authentication.user_and_password)
        else:
            raise ValueError(f"Unexpected type for authentication: {raw_source.authentication.type}")

        return cls(
            registry=raw_source.registry,
            repository=raw_source.repository,
            tag_details=TagDetails._from_conjure(raw_source.tag_details),
            authentication=auth,
            command=raw_source.command,
        )

    def _to_conjure(self) -> ingest_api.DockerImageSource:
        if self.authentication is None:
            authentication = ingest_api.Authentication(public=ingest_api.PublicAuthentication())
        else:
            authentication = self.authentication._to_conjure()

        return ingest_api.DockerImageSource(
            authentication=authentication,
            registry=self.registry,
            repository=self.repository,
            tag_details=self.tag_details._to_conjure(),
        )


@dataclass(frozen=True)
class TimestampMetadata:
    """Metadata about the shared timestamp column provided by the output `.parquet.tar` file from the extractor."""

    # Name of the channel containing timestamp metadata
    series_name: str

    # Type of timestamp used by the channel containing timestamp metadata
    timestamp_type: _ConjureTimestampType

    @classmethod
    def _from_conjure(cls, raw_metadata: ingest_api.TimestampMetadata) -> Self:
        return cls(
            series_name=raw_metadata.series_name,
            timestamp_type=_ConjureTimestampType._from_conjure(raw_metadata.timestamp_type),
        )

    def _to_conjure(self) -> ingest_api.TimestampMetadata:
        return ingest_api.TimestampMetadata(
            series_name=self.series_name,
            timestamp_type=self.timestamp_type._to_conjure_ingest_api(),
        )


@dataclass(frozen=True)
class ContainerizedExtractor(HasRid):
    """Containerized extractor which can be used to parse custom data formats into Nominal using docker images."""

    # Unique identifier for the extractor
    rid: str

    # Human readable name for the extractor
    name: str

    # Optional human readable description for the extractor
    description: str | None

    # Details about the docker image to use for the extractor
    image: DockerImageSource

    # Details about file inputs to the extractor
    inputs: Sequence[FileExtractionInput]

    # Human readable properties to apply to the extractor
    properties: Mapping[str, str]

    # Human readable labels to apply to the extractor
    labels: Sequence[str]

    # Details about the channel containing timestamp data for outputs from the extractor
    timestamp_metadata: TimestampMetadata

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def containerized_extractors(self) -> ingest_api.ContainerizedExtractorService: ...

    def update(
        self,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        timestamp_metadata: TimestampMetadata | None = None,
        tags: Sequence[str] | None = None,
        default_tag: str | None = None,
    ) -> Self:
        """Update metadata for the custom extractor.

        Args:
            name: Update the name of the extractor, if provided.
            description: Update the description of the extractor, if provided.
            properties: Update the properties of the extractor, if provided.
            labels: Update the labels of the extractor, if provided.
            timestamp_metadata: Update the timestamp channel metadata of the extractor, if provided.
            tags: Update the tags of the extractor, if provided.
            default_tag: Update the default tag used for the extractor, if provided.

        Returns:
            Updated version of this instance containing newly changed fields and their values.
        """
        request = ingest_api.UpdateContainerizedExtractorRequest(
            name=name,
            description=description,
            properties=None if properties is None else {**properties},
            labels=None if labels is None else list(labels),
            timestamp_metadata=None if timestamp_metadata is None else timestamp_metadata._to_conjure(),
            tags=None if tags is None else list(tags),
            default_tag=default_tag,
        )

        raw_extractor = self._clients.containerized_extractors.update_containerized_extractor(
            self._clients.auth_header,
            self.rid,
            request,
        )
        updated_extractor = self._from_conjure(self._clients, raw_extractor)
        update_dataclass(self, updated_extractor, self.__dataclass_fields__)
        return self

    def archive(self) -> None:
        """Archive the extractor, preventing it from being shown to users when uploading data."""
        self._clients.containerized_extractors.archive_containerized_extractor(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive the extractor, allowing it to be shown to users when uploading data."""
        self._clients.containerized_extractors.unarchive_containerized_extractor(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, raw_extractor: ingest_api.ContainerizedExtractor) -> Self:
        return cls(
            rid=raw_extractor.rid,
            name=raw_extractor.name,
            description=raw_extractor.description,
            image=DockerImageSource._from_conjure(raw_extractor.image),
            inputs=[FileExtractionInput._from_conjure(raw_input) for raw_input in raw_extractor.inputs],
            properties=raw_extractor.properties,
            labels=raw_extractor.labels,
            timestamp_metadata=TimestampMetadata._from_conjure(raw_extractor.timestamp_metadata),
            _clients=clients,
        )
