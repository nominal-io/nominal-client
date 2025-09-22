from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Protocol, Sequence

from nominal_api import ingest_api
from typing_extensions import Self

from nominal._utils.dataclass_tools import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid
from nominal.ts import _ConjureTimestampType


@dataclass(frozen=True)
class UserPassAuth:
    """User/password based authentication credentials for pulling a docker image.

    Args:
        username: Username for the docker registry containing the docker image.
        password_rid: Resource ID of the Secret containing the password for authentication
            with the docker registry.
    """

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
    """Configuration for a file extraction input in a containerized extractor.

    Args:
        name: Human-readable name for this input configuration.
        description: Optional detailed description of what this input represents.
        environment_variable: Environment variable name that will be set in the container
            to specify the input file path.
        file_suffixes: List of file extensions that this input accepts (e.g., ['.csv', '.txt']).
        required: Whether this input is mandatory for the extractor to run. Defaults to False.
    """

    name: str
    description: str | None
    environment_variable: str
    file_suffixes: Sequence[str]
    required: bool

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
            required=file_input.required if file_input.required is not None else False,
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
    """Details about docker image tags to register for a custom extractor.

    Args:
        tags: Available image tags that can be used for this extractor.
        default_tag: The tag that will be used by default when running the extractor.
    """

    tags: Sequence[str]
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
    registry: str
    repository: str
    tag_details: TagDetails
    authentication: UserPassAuth | None
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


class FileOutputFormat(Enum):
    PARQUET_TAR = "PARQUET_TAR"
    CSV = "CSV"
    PARQUET = "PARQUET"

    @classmethod
    def _from_conjure(cls, raw_source: ingest_api.FileOutputFormat) -> Self:
        return cls(raw_source.value)

    def _to_conjure(self) -> ingest_api.FileOutputFormat:
        return ingest_api.FileOutputFormat(self.value)


@dataclass(frozen=True)
class TimestampMetadata:
    """Metadata about the timestamp output from the extractor.

    Args:
        series_name: Name of the column containing timestamp data in the output files.
        timestamp_type: Type information specifying how timestamps should be interpreted.
    """

    series_name: str
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

    rid: str
    name: str
    description: str | None
    image: DockerImageSource
    inputs: Sequence[FileExtractionInput]
    properties: Mapping[str, str]
    labels: Sequence[str]
    timestamp_metadata: TimestampMetadata
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def containerized_extractors(self) -> ingest_api.ContainerizedExtractorService: ...

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        timestamp_metadata: TimestampMetadata | None = None,
        tags: Sequence[str] | None = None,
        default_tag: str | None = None,
    ) -> Self:
        """Returns:
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
        self._clients.containerized_extractors.archive_containerized_extractor(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
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
