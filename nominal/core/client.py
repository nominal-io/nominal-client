from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from io import TextIOBase
from pathlib import Path
from typing import BinaryIO, Iterable, Mapping, Sequence, Union

import certifi
import conjure_python_client
from conjure_python_client import ServiceConfiguration, SslConfiguration
from nominal_api import (
    api,
    attachments_api,
    authentication_api,
    event,
    ingest_api,
    scout_asset_api,
    scout_catalog,
    scout_checks_api,
    scout_datasource_connection_api,
    scout_notebook_api,
    scout_run_api,
    scout_template_api,
    scout_video_api,
    secrets_api,
    storage_datasource_api,
)
from typing_extensions import Self, deprecated

from nominal import _config, ts
from nominal._utils.deprecation_tools import warn_on_deprecated_argument
from nominal.config import NominalConfig
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._constants import DEFAULT_API_BASE_URL
from nominal.core._utils.api_tools import (
    construct_user_agent_string,
    rid_from_instance_or_string,
)
from nominal.core._utils.multipart import (
    path_upload_name,
    upload_multipart_io,
)
from nominal.core._utils.pagination_tools import (
    list_streaming_checklists_for_asset_paginated,
    list_streaming_checklists_paginated,
    search_assets_paginated,
    search_checklists_paginated,
    search_data_reviews_paginated,
    search_datasets_paginated,
    search_events_paginated,
    search_runs_by_asset_paginated,
    search_runs_paginated,
    search_secrets_paginated,
    search_users_paginated,
    search_workbook_templates_paginated,
    search_workbooks_paginated,
)
from nominal.core._utils.query_tools import (
    create_search_assets_query,
    create_search_checklists_query,
    create_search_containerized_extractors_query,
    create_search_datasets_query,
    create_search_events_query,
    create_search_runs_query,
    create_search_secrets_query,
    create_search_users_query,
    create_search_workbook_templates_query,
    create_search_workbooks_query,
)
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.checklist import Checklist
from nominal.core.connection import Connection, StreamingConnection
from nominal.core.containerized_extractors import (
    ContainerizedExtractor,
    DockerImageSource,
    FileExtractionInput,
    FileOutputFormat,
)
from nominal.core.data_review import DataReview, DataReviewBuilder
from nominal.core.dataset import (
    Dataset,
    _create_dataset,
    _get_dataset,
    _get_datasets,
)
from nominal.core.event import Event, EventType
from nominal.core.filetype import FileType, FileTypes
from nominal.core.run import Run
from nominal.core.secret import Secret
from nominal.core.unit import Unit, _available_units
from nominal.core.user import User
from nominal.core.video import Video
from nominal.core.workbook import Workbook
from nominal.core.workbook_template import WorkbookTemplate
from nominal.core.workspace import Workspace
from nominal.exceptions import NominalConfigError, NominalError, NominalIngestError
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    _SecondsNanos,
    _to_api_duration,
    _to_typed_timestamp_type,
)

logger = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT = timedelta(seconds=30)


class WorkspaceSearchType(enum.Enum):
    ALL = "ALL"
    DEFAULT = "DEFAULT"


WorkspaceSearchT = Union[WorkspaceSearchType, Workspace, str]


@dataclass(frozen=True)
class NominalClient:
    _clients: ClientsBunch = field(repr=False)
    _profile: str | None = None

    @classmethod
    def from_profile(
        cls,
        profile: str,
        *,
        trust_store_path: str | None = None,
        connect_timeout: timedelta | float = DEFAULT_CONNECT_TIMEOUT,
    ) -> Self:
        """Create a connection to the Nominal platform from a named profile in the Nominal config.

        Args:
            profile: profile name in the Nominal config.
            trust_store_path: path to a trust store certificate chain to initiate SSL connections. If not provided,
                certifi's trust store is used.
            connect_timeout: Request connection timeout.
        """
        config = NominalConfig.from_yaml()
        prof = config.get_profile(profile)
        client = cls.from_token(
            prof.token,
            prof.base_url,
            workspace_rid=prof.workspace_rid,
            trust_store_path=trust_store_path,
            connect_timeout=connect_timeout,
            _profile=profile,
        )
        return client

    @classmethod
    def from_token(
        cls,
        token: str,
        base_url: str = DEFAULT_API_BASE_URL,
        *,
        workspace_rid: str | None = None,
        trust_store_path: str | None = None,
        connect_timeout: timedelta | float = DEFAULT_CONNECT_TIMEOUT,
        _profile: str | None = None,
    ) -> Self:
        """Create a connection to the Nominal platform from a token.

        Args:
            token: Authentication token to use for the connection.
            base_url: The URL of the Nominal API platform.
            workspace_rid: The workspace RID to use for all API calls that require it. If not provided, the default
                workspace will be used (if one is configured for the tenant).
            trust_store_path: path to a trust store certificate chain to initiate SSL connections. If not provided,
                certifi's trust store is used.
            connect_timeout: Request connection timeout.
        """
        trust_store_path = certifi.where() if trust_store_path is None else trust_store_path
        timeout_seconds = connect_timeout.total_seconds() if isinstance(connect_timeout, timedelta) else connect_timeout
        cfg = ServiceConfiguration(
            uris=[base_url],
            security=SslConfiguration(trust_store_path=trust_store_path),
            connect_timeout=timeout_seconds,
        )
        agent = construct_user_agent_string()
        return cls(_clients=ClientsBunch.from_config(cfg, base_url, agent, token, workspace_rid), _profile=_profile)

    @classmethod
    def create(
        cls,
        base_url: str,
        token: str | None,
        trust_store_path: str | None = None,
        connect_timeout: timedelta | float = DEFAULT_CONNECT_TIMEOUT,
        *,
        workspace_rid: str | None = None,
    ) -> Self:
        """Create a connection to the Nominal platform.

        base_url: The URL of the Nominal API platform, e.g. "https://api.gov.nominal.io/api".
        token: An API token to authenticate with. If None, the token will be looked up in ~/.nominal.yml.
        trust_store_path: path to a trust store CA root file to initiate SSL connections. If not provided,
            certifi's trust store is used.
        connect_timeout: Timeout for any single request to the Nominal API.
        workspace_rid: The workspace RID to use for all API calls that require it. If not provided, the default
            workspace will be used (if one is configured for the tenant).
        """
        if token is None:
            token = _config.get_token(base_url)
        return cls.from_token(
            token,
            base_url,
            trust_store_path=trust_store_path,
            connect_timeout=connect_timeout,
            workspace_rid=workspace_rid,
        )

    def __repr__(self) -> str:
        """Repr for the class that shows profile name, if available"""
        out = "<NominalClient"
        if self._profile:
            out += f' profile="{self._profile}"'
        out += ">"
        return out

    def _workspace_rid_for_search(self, workspace: WorkspaceSearchT) -> str | None:
        """Provide the correct workspace rid to use when searching (potentially using a provided workspace)

        Args:
            workspace: Workspace (or None) that user wants to search with

        Returns:
            If no workspace is provided, then return the default workspace (or none if none is configured).
            If a workspace is provided, then return it if authenticated, otherwise, return None.
        """
        search_rid = None
        if isinstance(workspace, Workspace):
            search_rid = workspace.rid
        elif isinstance(workspace, str):
            search_rid = workspace
        elif workspace is WorkspaceSearchType.ALL:
            return None
        elif workspace is WorkspaceSearchType.DEFAULT:
            search_rid = None
        else:
            raise ValueError(f"Unexpected workspace: {workspace}")

        try:
            # NOTE: raises a conjure exception if the given rid is not visible to the user (or doesn't exist period)
            resolved_workspace = self.get_workspace(search_rid)
            return resolved_workspace.rid
        except NominalConfigError:
            # re-raising with a more specific exception message
            raise NominalConfigError(
                "WorkspaceSearchType.DEFAULT provided for workspace rid, but no default configured. "
                "Specify a workspace RID within your config profile (see `nom config profile --help`), "
                "specify a workspace RID manually, or contact your Nominal representative to set a default "
                "workspace for your tenant."
            )

    def get_workspace(self, workspace_rid: str | None = None) -> Workspace:
        """Get workspace via given RID, or the default workspace if no RID is provided.

        Args:
            workspace_rid: If provided, the RID of the workspace to retrieve. If None, retrieves the
                default workspace (deferring first to any workspace rid stored in the Nominal config, and attempting
                to fall back to the tenant-wide default workspace).

        Returns:
            Returns details about the requested workspace.

        Raises:
            NominalConfigError: Raises a NominalConfigError if no workspace provided and there is no configured
                default workspace for the user.
            conjure_python_client.ConjureHTTPError: Requested workspace is unavailable to the user.
        """
        if workspace_rid is None:
            raw_workspace = self._clients.workspace.get_default_workspace(self._clients.auth_header)
            if raw_workspace is None:
                raise NominalConfigError(
                    "Could not retrieve default workspace! "
                    "Either the user is not authorized to access or there is no default workspace."
                )

            return Workspace._from_conjure(raw_workspace)
        else:
            raw_workspace = self._clients.workspace.get_workspace(self._clients.auth_header, workspace_rid)
            return Workspace._from_conjure(raw_workspace)

    def list_workspaces(self) -> Sequence[Workspace]:
        """Return all workspaces visible to the current user"""
        return [
            Workspace._from_conjure(raw_workspace)
            for raw_workspace in self._clients.workspace.get_workspaces(self._clients.auth_header)
        ]

    def get_user(self, user_rid: str | None = None) -> User:
        """Retrieve the specified user.

        Args:
            user_rid: Rid of the user to retrieve

        Returns:
            Details on the requested user, or the current user if no user rid is provided.
        """
        if user_rid is None:
            raw_user = self._clients.authentication.get_my_profile(self._clients.auth_header)
        else:
            raw_user = self._clients.authentication.get_user(self._clients.auth_header, user_rid)

        return User._from_conjure(raw_user)

    def _iter_search_users(self, query: authentication_api.SearchUsersQuery) -> Iterable[User]:
        for raw_user in search_users_paginated(self._clients.authentication, self._clients.auth_header, query):
            yield User._from_conjure(raw_user)

    def search_users(self, exact_match: str | None = None, search_text: str | None = None) -> Sequence[User]:
        """Search for users meeting the specified filters.
        Filters are ANDed together, e.g., if exact_match and search_text are both provided, then both must match.

        Args:
            exact_match: Searches for an exact substring across display name and email
            search_text: Searches for a (case-insensitive) substring across display name and email

        Returns:
            All users which match all of the provided conditions
        """
        query = create_search_users_query(exact_match=exact_match, search_text=search_text)
        return list(self._iter_search_users(query))

    def _iter_search_datasets(self, query: scout_catalog.SearchDatasetsQuery) -> Iterable[Dataset]:
        for raw_dataset in search_datasets_paginated(self._clients.catalog, self._clients.auth_header, query):
            yield Dataset._from_conjure(self._clients, raw_dataset)

    @warn_on_deprecated_argument(
        "workspace_rid",
        "`workspace_rid` has been deprecated and will be removed in a future version. Use `workspace` instead.",
    )
    def search_datasets(
        self,
        *,
        exact_match: str | None = None,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        before: str | datetime | IntegralNanosecondsUTC | None = None,
        after: str | datetime | IntegralNanosecondsUTC | None = None,
        workspace_rid: Workspace | str | None = None,
        workspace: WorkspaceSearchT = WorkspaceSearchType.ALL,
        archived: bool | None = None,
    ) -> Sequence[Dataset]:
        """Search for datasets the specified filters.
        Filters are ANDed together, e.g. `(secret.label == label) AND (secret.property == property)`

        Args:
            exact_match: Searches for an exact substring of dataset name
            search_text: Searches for a (case-insensitive) substring across all text fields.
            labels: A sequence of labels that must ALL be present on a secret to be included.
            properties: A mapping of key-value pairs that must ALL be present on a secret to be included.
            before: Searches for datasets created before some time (inclusive).
            after: Searches for datasets created before after time (inclusive).
            workspace_rid: deprecated. use `workspace` instead.
            workspace: Filters search to given workspace.
            archived: Filters results to either archived or unarchived datasets.

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.

        Returns:
            All datasets which match all of the provided conditions
        """
        if workspace is not None and workspace_rid is not None:
            raise ValueError("Both `workspace` and `workspace_rid` provided-- must use one or the other.")

        if workspace_rid is not None:
            workspace = workspace_rid

        query = create_search_datasets_query(
            exact_match=exact_match,
            search_text=search_text,
            labels=labels,
            properties=properties,
            ingested_before_inclusive=before,
            ingested_after_inclusive=after,
            archived=archived,
            workspace_rid=self._workspace_rid_for_search(workspace),
        )
        return list(self._iter_search_datasets(query))

    def create_secret(
        self,
        name: str,
        decrypted_value: str,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Secret:
        """Create a secret for the current user

        Args:
            name: Name of the secret
            decrypted_value: Plain text value of the secret
            description: Description of the secret
            labels: Labels for the secret
            properties: Properties for the secret
        """
        secret_request = secrets_api.CreateSecretRequest(
            name=name,
            description=description or "",
            decrypted_value=decrypted_value,
            workspace=self._clients.workspace_rid,
            labels=list(labels),
            properties={} if properties is None else dict(properties),
        )
        resp = self._clients.secrets.create(self._clients.auth_header, secret_request)
        return Secret._from_conjure(self._clients, resp)

    def get_secret(self, rid: str) -> Secret:
        """Retrieve a secret by RID."""
        resp = self._clients.secrets.get(self._clients.auth_header, rid)
        return Secret._from_conjure(self._clients, resp)

    def _iter_search_secrets(self, query: secrets_api.SearchSecretsQuery) -> Iterable[Secret]:
        for secret in search_secrets_paginated(self._clients.secrets, self._clients.auth_header, query):
            yield Secret._from_conjure(self._clients, secret)

    def search_secrets(
        self,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        workspace: WorkspaceSearchT | None = WorkspaceSearchType.ALL,
    ) -> Sequence[Secret]:
        """Search for secrets meeting the specified filters.
        Filters are ANDed together, e.g. `(secret.label == label) AND (secret.property == property)`

        Args:
            search_text: Searches for a (case-insensitive) substring across all text fields.
            labels: A sequence of labels that must ALL be present on a secret to be included.
            properties: A mapping of key-value pairs that must ALL be present on a secret to be included.
            workspace: Filters search to given workspace.

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.


        Returns:
            All secrets which match all of the provided conditions
        """
        query = create_search_secrets_query(
            search_text=search_text,
            labels=labels,
            properties=properties,
            workspace_rid=self._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
        )
        return list(self._iter_search_secrets(query))

    def create_run(
        self,
        name: str,
        start: datetime | IntegralNanosecondsUTC,
        end: datetime | IntegralNanosecondsUTC | None,
        description: str | None = None,
        *,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
        attachments: Iterable[Attachment] | Iterable[str] = (),
        asset: Asset | str | None = None,
    ) -> Run:
        """Create a run."""
        # TODO(alkasm): support links
        request = scout_run_api.CreateRunRequest(
            attachments=[rid_from_instance_or_string(a) for a in attachments],
            data_sources={},
            description=description or "",
            labels=list(labels),
            links=[],
            properties={} if properties is None else dict(properties),
            start_time=_SecondsNanos.from_flexible(start).to_scout_run_api(),
            title=name,
            end_time=None if end is None else _SecondsNanos.from_flexible(end).to_scout_run_api(),
            assets=[] if asset is None else [rid_from_instance_or_string(asset)],
            workspace=self._clients.workspace_rid,
        )
        response = self._clients.run.create_run(self._clients.auth_header, request)
        return Run._from_conjure(self._clients, response)

    def get_run(self, rid: str) -> Run:
        """Retrieve a run by its RID."""
        response = self._clients.run.get_run(self._clients.auth_header, rid)
        return Run._from_conjure(self._clients, response)

    def _iter_search_runs(
        self,
        start: str | datetime | IntegralNanosecondsUTC | None = None,
        end: str | datetime | IntegralNanosecondsUTC | None = None,
        name_substring: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        exact_match: str | None = None,
        search_text: str | None = None,
        workspace_rid: str | None = None,
    ) -> Iterable[Run]:
        query = create_search_runs_query(
            start=start,
            end=end,
            name_substring=name_substring,
            labels=labels,
            properties=properties,
            exact_match=exact_match,
            search_text=search_text,
            workspace_rid=workspace_rid,
        )
        for run in search_runs_paginated(self._clients.run, self._clients.auth_header, query):
            yield Run._from_conjure(self._clients, run)

    def search_runs(
        self,
        start: str | datetime | IntegralNanosecondsUTC | None = None,
        end: str | datetime | IntegralNanosecondsUTC | None = None,
        name_substring: str | None = None,
        *,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        exact_match: str | None = None,
        search_text: str | None = None,
        workspace: WorkspaceSearchT | None = WorkspaceSearchType.ALL,
    ) -> Sequence[Run]:
        """Search for runs meeting the specified filters.
        Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`

        Args:
            start: Inclusive start time for filtering runs.
            end: Inclusive end time for filtering runs.
            name_substring: Searches for a (case-insensitive) substring in the name.
            labels: A sequence of labels that must ALL be present on a run to be included.
            properties: A mapping of key-value pairs that must ALL be present on a run to be included.
            exact_match: A case-insensitive substring that must be matched exactly.
            search_text: A case-insensitive substring to perform fuzzy-search on all fields with
            workspace: Filters search to given workspace.

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.


        Returns:
            All runs which match all of the provided conditions
        """
        return list(
            self._iter_search_runs(
                start=start,
                end=end,
                name_substring=name_substring,
                labels=labels,
                properties=properties,
                exact_match=exact_match,
                search_text=search_text,
                workspace_rid=self._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
            )
        )

    def search_runs_by_asset(self, asset: Asset | str) -> Sequence[Run]:
        """Search for all runs associated with a given asset:

        Args:
            asset: Asset to search for runs from

        Returns:
            All runs associated with the given asset
        """
        return [
            Run._from_conjure(self._clients, run)
            for run in search_runs_by_asset_paginated(
                self._clients.run, self._clients.auth_header, rid_from_instance_or_string(asset)
            )
        ]

    def create_dataset(
        self,
        name: str,
        *,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create an empty dataset.

        Args:
            name: Name of the dataset to create in Nominal.
            description: Human readable description of the dataset.
            labels: Text labels to apply to the created dataset
            properties: Key-value properties to apply to the cleated dataset
            prefix_tree_delimiter: If present, the delimiter to represent tiers when viewing channels hierarchically.

        Returns:
            Reference to the created dataset in Nominal.
        """
        response = _create_dataset(
            self._clients.auth_header,
            self._clients.catalog,
            name,
            description=description,
            labels=labels,
            properties=properties,
            workspace_rid=self._clients.workspace_rid,
        )
        dataset = Dataset._from_conjure(self._clients, response)

        if prefix_tree_delimiter:
            dataset.set_channel_prefix_tree(prefix_tree_delimiter)

        return dataset

    def create_empty_video(
        self,
        name: str,
        *,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Video:
        """Create an empty video to append video files to.

        Args:
            name: Name of the video to create in Nominal
            description: Description of the video to create in nominal
            labels: Labels to apply to the video in nominal
            properties: Properties to apply to the video in nominal

        Returns:
            Handle to the created video
        """
        request = scout_video_api.CreateVideoRequest(
            title=name,
            labels=list(labels),
            properties={} if properties is None else {**properties},
            description=description,
            workspace=self._clients.workspace_rid,
            marking_rids=[],
        )
        raw_video = self._clients.video.create(self._clients.auth_header, request)
        return Video._from_conjure(self._clients, raw_video)

    def get_video(self, rid: str) -> Video:
        """Retrieve a video by its RID."""
        response = self._clients.video.get(self._clients.auth_header, rid)
        return Video._from_conjure(self._clients, response)

    def _iter_get_videos(self, rids: Iterable[str]) -> Iterable[Video]:
        request = scout_video_api.GetVideosRequest(video_rids=list(rids))
        for response in self._clients.video.batch_get(self._clients.auth_header, request).responses:
            yield Video._from_conjure(self._clients, response)

    def get_videos(self, rids: Iterable[str]) -> Sequence[Video]:
        """Retrieve videos by their RID."""
        return list(self._iter_get_videos(rids))

    def get_dataset(self, rid: str) -> Dataset:
        """Retrieve a dataset by its RID."""
        response = _get_dataset(self._clients.auth_header, self._clients.catalog, rid)
        return Dataset._from_conjure(self._clients, response)

    def _iter_get_datasets(self, rids: Iterable[str]) -> Iterable[Dataset]:
        for ds in _get_datasets(self._clients.auth_header, self._clients.catalog, rids):
            yield Dataset._from_conjure(self._clients, ds)

    def get_datasets(self, rids: Iterable[str]) -> Sequence[Dataset]:
        """Retrieve datasets by their RIDs."""
        return list(self._iter_get_datasets(rids))

    def get_checklist(self, rid: str) -> Checklist:
        response = self._clients.checklist.get(self._clients.auth_header, rid)
        return Checklist._from_conjure(self._clients, response)

    def _iter_search_checklists(self, query: scout_checks_api.ChecklistSearchQuery) -> Iterable[Checklist]:
        for checklist in search_checklists_paginated(self._clients.checklist, self._clients.auth_header, query):
            yield Checklist._from_conjure(self._clients, checklist)

    def search_checklists(
        self,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        author: User | str | None = None,
        assignee: User | str | None = None,
        workspace: WorkspaceSearchT | None = None,
    ) -> Sequence[Checklist]:
        """Search for checklists meeting the specified filters.
        Filters are ANDed together, e.g. `(checklist.label == label) AND (checklist.search_text =~ field)`

        Args:
            search_text: case-insensitive search for any of the keywords in all string fields
            labels: A sequence of labels that must ALL be present on a checklist to be included.
            properties: A mapping of key-value pairs that must ALL be present on a checklist to be included.
            author: Author of checklists to search for
            assignee: Assignee of checklists to search for
            workspace: Filters search to given workspace.

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.


        Returns:
            All checklists which match all of the provided conditions
        """
        query = create_search_checklists_query(
            search_text=search_text,
            labels=labels,
            properties=properties,
            author=rid_from_instance_or_string(author) if author else None,
            assignee=rid_from_instance_or_string(assignee) if assignee else None,
            workspace_rid=self._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
        )
        return list(self._iter_search_checklists(query))

    def create_attachment(
        self,
        attachment_file: Path | str,
        *,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
    ) -> Attachment:
        attachment_path = Path(attachment_file)
        if not attachment_path.exists():
            raise FileNotFoundError(f"No such attachment path: {attachment_path}")

        with attachment_path.open("rb") as f:
            return self.create_attachment_from_io(
                f,
                attachment_path.name,
                FileTypes.BINARY,
                description=description,
                properties=properties,
                labels=labels,
            )

    def create_attachment_from_io(
        self,
        attachment: BinaryIO,
        name: str,
        file_type: tuple[str, str] | FileType = FileTypes.BINARY,
        description: str | None = None,
        *,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
    ) -> Attachment:
        """Upload an attachment.
        The attachment must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.
        """
        if isinstance(attachment, TextIOBase):
            raise TypeError(f"attachment {attachment} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(
            self._clients.auth_header,
            self._clients.workspace_rid,
            attachment,
            name,
            file_type,
            self._clients.upload,
        )
        request = attachments_api.CreateAttachmentRequest(
            description=description or "",
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            s3_path=s3_path,
            title=name,
            workspace=self._clients.workspace_rid,
        )
        response = self._clients.attachment.create(self._clients.auth_header, request)
        return Attachment._from_conjure(self._clients, response)

    def get_attachment(self, rid: str) -> Attachment:
        """Retrieve an attachment by its RID."""
        response = self._clients.attachment.get(self._clients.auth_header, rid)
        return Attachment._from_conjure(self._clients, response)

    def get_attachments(self, rids: Iterable[str]) -> Sequence[Attachment]:
        """Retrive attachments by their RIDs."""
        return [
            Attachment._from_conjure(self._clients, a)
            for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, rids)
        ]

    def get_all_units(self) -> Sequence[Unit]:
        """Retrieve list of metadata for all supported units within Nominal"""
        return _available_units(self._clients.auth_header, self._clients.units)

    def get_unit(self, unit_symbol: str) -> Unit | None:
        """Get details of the given unit symbol, or none if the symbol is not recognized by Nominal.

        Args:
            unit_symbol: Symbol of the unit to get metadata for.
                NOTE: This currently requires that units are formatted as laid out in
                      the latest UCUM standards (see https://ucum.org/ucum)

        Returns:
        -------
            Resolved unit metadata if the symbol is valid and supported by Nominal, or None
            if no such unit symbol matches.

        """
        try:
            api_unit = self._clients.units.get_unit(self._clients.auth_header, unit_symbol)
            return None if api_unit is None else Unit._from_conjure(api_unit)
        except conjure_python_client.ConjureHTTPError as ex:
            logger.debug("Error getting unit '%s': '%s'", unit_symbol, ex)
            return None

    def get_commensurable_units(self, unit_symbol: str) -> Sequence[Unit]:
        """Get the list of units that are commensurable (convertible to/from) the given unit symbol."""
        return [
            Unit._from_conjure(unit)
            for unit in self._clients.units.get_commensurable_units(self._clients.auth_header, unit_symbol)
        ]

    def get_connection(self, rid: str) -> Connection:
        """Retrieve a connection by its RID."""
        response = self._clients.connection.get_connection(self._clients.auth_header, rid)
        return Connection._from_conjure(self._clients, response)

    def create_video_from_mcap(
        self,
        path: Path | str,
        topic: str,
        name: str | None = None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Video:
        """Create a video from an MCAP file containing H264 or H265 video data.

        If name is None, the name of the file will be used.

        See `create_video_from_mcap_io` for more details.
        """
        path = Path(path)
        if name is None:
            name = path.name

        with path.open("rb") as data_file:
            return self.create_video_from_mcap_io(
                data_file,
                name=name,
                topic=topic,
                file_type=FileTypes.MCAP,
                description=description,
                labels=labels,
                properties=properties,
                file_name=path_upload_name(path, FileTypes.MCAP),
            )

    def create_video_from_mcap_io(
        self,
        mcap: BinaryIO,
        topic: str,
        name: str,
        description: str | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MCAP,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        file_name: str | None = None,
    ) -> Video:
        """Create video from topic in a mcap file.

        Mcap must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.

        If name is None, the name of the file will be used.
        """
        if isinstance(mcap, TextIOBase):
            raise TypeError(f"dataset {mcap} must be open in binary mode, rather than text mode")

        if file_name is None:
            file_name = name

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(
            self._clients.auth_header, self._clients.workspace_rid, mcap, file_name, file_type, self._clients.upload
        )
        request = ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                video=ingest_api.VideoOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)),
                    target=ingest_api.VideoIngestTarget(
                        new=ingest_api.NewVideoIngestDestination(
                            title=name,
                            description=description,
                            properties={} if properties is None else dict(properties),
                            labels=list(labels),
                            workspace=self._clients.workspace_rid,
                            marking_rids=[],
                        )
                    ),
                    timestamp_manifest=scout_video_api.VideoFileTimestampManifest(
                        mcap=scout_video_api.McapTimestampManifest(api.McapChannelLocator(topic=topic))
                    ),
                )
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.video is None:
            raise NominalIngestError("error ingesting mcap video: no video created")
        return self.get_video(response.details.video.video_rid)

    def create_streaming_connection(
        self,
        datasource_id: str,
        connection_name: str,
        datasource_description: str | None = None,
        *,
        required_tag_names: list[str] | None = None,
    ) -> StreamingConnection:
        datasource_response = self._clients.storage.create(
            self._clients.auth_header,
            storage_datasource_api.CreateNominalDataSourceRequest(
                id=datasource_id,
                description=datasource_description,
                workspace=self._clients.workspace_rid,
            ),
        )
        connection_response = self._clients.connection.create_connection(
            self._clients.auth_header,
            scout_datasource_connection_api.CreateConnection(
                name=connection_name,
                connection_details=scout_datasource_connection_api.ConnectionDetails(
                    nominal=scout_datasource_connection_api.NominalConnectionDetails(
                        nominal_data_source_rid=datasource_response.rid,
                    ),
                ),
                metadata={},
                scraping=scout_datasource_connection_api.ScrapingConfig(
                    nominal=scout_datasource_connection_api.NominalScrapingConfig(
                        channel_name_components=[
                            scout_datasource_connection_api.NominalChannelNameComponent(channel=api.Empty())
                        ],
                        separator=".",
                    )
                ),
                required_tag_names=required_tag_names or [],
                available_tag_values={},
                should_scrape=True,
                workspace=self._clients.workspace_rid,
                marking_rids=[],
            ),
        )
        conn = Connection._from_conjure(self._clients, connection_response)
        if isinstance(conn, StreamingConnection):
            return conn
        raise NominalError(f"Expected StreamingConnection but got {type(conn).__name__}")

    def create_asset(
        self,
        name: str,
        description: str | None = None,
        *,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
    ) -> Asset:
        """Create an asset."""
        request = scout_asset_api.CreateAssetRequest(
            description=description,
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            title=name,
            attachments=[],
            data_scopes=[],
            links=[],
            workspace=self._clients.workspace_rid,
        )
        response = self._clients.assets.create_asset(self._clients.auth_header, request)
        return Asset._from_conjure(self._clients, response)

    def get_asset(self, rid: str) -> Asset:
        """Retrieve an asset by its RID."""
        response = self._clients.assets.get_assets(self._clients.auth_header, [rid])
        if len(response) == 0 or rid not in response:
            raise ValueError(f"no asset found with RID {rid!r}: {response!r}")
        if len(response) > 1:
            raise ValueError(f"multiple assets found with RID {rid!r}: {response!r}")
        return Asset._from_conjure(self._clients, response[rid])

    def _iter_search_assets(self, query: scout_asset_api.SearchAssetsQuery) -> Iterable[Asset]:
        for asset in search_assets_paginated(self._clients.assets, self._clients.auth_header, query):
            yield Asset._from_conjure(self._clients, asset)

    def search_assets(
        self,
        search_text: str | None = None,
        *,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        exact_substring: str | None = None,
        workspace: WorkspaceSearchT | None = WorkspaceSearchType.ALL,
    ) -> Sequence[Asset]:
        """Search for assets meeting the specified filters.
        Filters are ANDed together, e.g. `(asset.label == label) AND (asset.search_text =~ field)`

        Args:
            search_text: case-insensitive search for any of the keywords in all string fields
            labels: A sequence of labels that must ALL be present on a asset to be included.
            properties: A mapping of key-value pairs that must ALL be present on a asset to be included.
            exact_substring: case-insensitive search for exact string match in all string fields
            workspace: Filters search to given workspace.

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.


        Returns:
            All assets which match all of the provided conditions
        """
        query = create_search_assets_query(
            search_text=search_text,
            labels=labels,
            properties=properties,
            exact_substring=exact_substring,
            workspace_rid=self._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
        )
        return list(self._iter_search_assets(query))

    def _iter_list_streaming_checklists(self, asset: str | None) -> Iterable[str]:
        if asset is None:
            return list_streaming_checklists_paginated(self._clients.checklist_execution, self._clients.auth_header)
        return list_streaming_checklists_for_asset_paginated(
            self._clients.checklist_execution, self._clients.auth_header, asset
        )

    def list_streaming_checklists(self, asset: Asset | str | None = None) -> Iterable[str]:
        """List all Streaming Checklists.

        Args:
            asset: if provided, only return checklists associated with the given asset.
        """
        asset = None if asset is None else rid_from_instance_or_string(asset)
        return list(self._iter_list_streaming_checklists(asset))

    def data_review_builder(self) -> DataReviewBuilder:
        return DataReviewBuilder([], [], [], _clients=self._clients)

    def get_data_review(self, rid: str) -> DataReview:
        response = self._clients.datareview.get(self._clients.auth_header, rid)
        return DataReview._from_conjure(self._clients, response)

    def create_event(
        self,
        name: str,
        type: EventType,
        start: datetime | IntegralNanosecondsUTC,
        duration: timedelta | IntegralNanosecondsDuration = timedelta(),
        *,
        description: str | None = None,
        assets: Iterable[Asset | str] = (),
        properties: Mapping[str, str] | None = None,
        labels: Iterable[str] = (),
    ) -> Event:
        request = event.CreateEvent(
            name=name,
            description=description,
            asset_rids=[rid_from_instance_or_string(asset) for asset in assets],
            timestamp=_SecondsNanos.from_flexible(start).to_api(),
            duration=_to_api_duration(duration),
            origins=[],
            properties=dict(properties) if properties else {},
            labels=list(labels),
            type=type._to_api_event_type(),
        )
        response = self._clients.event.create_event(self._clients.auth_header, request)
        return Event._from_conjure(self._clients, response)

    def get_event(self, rid: str) -> Event:
        events = self.get_events([rid])
        if len(events) != 1:
            raise RuntimeError(f"Expected to receive exactly one event, received {len(events)}")

        return events[0]

    def get_events(self, rids: Sequence[str]) -> Sequence[Event]:
        responses = self._clients.event.batch_get_events(self._clients.auth_header, list(rids))
        return [Event._from_conjure(self._clients, response) for response in responses]

    def _iter_search_data_reviews(
        self,
        assets: Sequence[Asset | str] | None = None,
        runs: Sequence[Run | str] | None = None,
    ) -> Iterable[DataReview]:
        for review in search_data_reviews_paginated(
            self._clients.datareview,
            self._clients.auth_header,
            assets=[rid_from_instance_or_string(asset) for asset in assets] if assets else None,
            runs=[rid_from_instance_or_string(run) for run in runs] if runs else None,
        ):
            yield DataReview._from_conjure(self._clients, review)

    def search_data_reviews(
        self,
        assets: Sequence[Asset | str] | None = None,
        runs: Sequence[Run | str] | None = None,
    ) -> Sequence[DataReview]:
        """Search for any data reviews present within a collection of runs and assets."""
        # TODO (drake-nominal): Expose checklist_refs to users
        return list(self._iter_search_data_reviews(assets, runs))

    def _iter_search_events(self, query: event.SearchQuery) -> Iterable[Event]:
        for e in search_events_paginated(self._clients.event, self._clients.auth_header, query):
            yield Event._from_conjure(self._clients, e)

    def search_events(
        self,
        *,
        search_text: str | None = None,
        after: datetime | IntegralNanosecondsUTC | None = None,
        before: datetime | IntegralNanosecondsUTC | None = None,
        assets: Iterable[Asset | str] | None = None,
        labels: Iterable[str] | None = None,
        properties: Mapping[str, str] | None = None,
        created_by: User | str | None = None,
        workbook: Workbook | str | None = None,
        data_review: DataReview | str | None = None,
        assignee: User | str | None = None,
        event_type: EventType | None = None,
        workspace: WorkspaceSearchT | None = WorkspaceSearchType.ALL,
    ) -> Sequence[Event]:
        """Search for events meeting the specified filters.
        Filters are ANDed together, e.g. `(event.label == label) AND (event.start > before)`

        Args:
            search_text: Searches for a string in the event's metadata.
            after: Filters to end times after this time, exclusive.
            before: Filters to start times before this time, exclusive.
            assets: List of assets that must ALL be present on an event to be included.
            labels: A list of labels that must ALL be present on an event to be included.
            properties: A mapping of key-value pairs that must ALL be present on an event to be included.
            created_by: A User (or rid) of the author that must be present on an event to be included.
            workbook: Workbook to search for events on
            data_review: Search for events from the given data review
            assignee: Search for events with the given assignee
            event_type: Search for events based on level
            workspace: Filters search to given workspace.

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.


        Returns:
            All events which match all of the provided conditions
        """
        query = create_search_events_query(
            search_text=search_text,
            after=after,
            before=before,
            assets=None if assets is None else [rid_from_instance_or_string(asset) for asset in assets],
            labels=labels,
            properties=properties,
            created_by=rid_from_instance_or_string(created_by) if created_by else None,
            workbook=rid_from_instance_or_string(workbook) if workbook else None,
            data_review=rid_from_instance_or_string(data_review) if data_review else None,
            assignee=rid_from_instance_or_string(assignee) if assignee else None,
            event_type=event_type,
            workspace_rid=self._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
        )
        return list(self._iter_search_events(query))

    def get_containerized_extractor(self, rid: str) -> ContainerizedExtractor:
        return ContainerizedExtractor._from_conjure(
            self._clients,
            self._clients.containerized_extractors.get_containerized_extractor(self._clients.auth_header, rid),
        )

    def create_containerized_extractor(
        self,
        name: str,
        *,
        docker_image: DockerImageSource,
        timestamp_column: str,
        timestamp_type: ts._AnyTimestampType,
        inputs: Sequence[FileExtractionInput] = (),
        file_output_format: FileOutputFormat | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        description: str | None = None,
    ) -> ContainerizedExtractor:
        workspace_rid = self._clients.workspace_rid
        if workspace_rid is None:  # TODO: Remove this once workspace_rid is required on the client
            workspace_rid = self.get_workspace().rid

        req = ingest_api.RegisterContainerizedExtractorRequest(
            image=docker_image._to_conjure(),
            inputs=[file_input._to_conjure() for file_input in inputs],
            labels=list(labels),
            name=name,
            properties={} if properties is None else {**properties},
            timestamp_metadata=ingest_api.TimestampMetadata(
                series_name=timestamp_column,
                timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
            ),
            workspace=workspace_rid,
            description=description,
            output_file_format=file_output_format._to_conjure() if file_output_format is not None else None,
        )
        resp = self._clients.containerized_extractors.register_containerized_extractor(self._clients.auth_header, req)
        return self.get_containerized_extractor(resp.extractor_rid)

    def search_containerized_extractors(
        self,
        *,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        workspace: WorkspaceSearchT | None = WorkspaceSearchType.ALL,
    ) -> Sequence[ContainerizedExtractor]:
        """Search for containerized extractors meeting the specified filters.
        Filters are ANDed together, e.g., `(extractor.label == label) AND (extractor.workspace == workspace)`

        Args:
            search_text: Fuzzy-searches for a string in the extractor's metadata.
            labels: A list of labels that must ALL be present on an extractor to be included.
            properties: A mapping of key-value pairs that must ALL be present on an extractor te be included.
            workspace: Filters search to given workspace.

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.


        Returns:
            All extractors which match all of the provided coditions
        """
        query = create_search_containerized_extractors_query(
            search_text=search_text,
            labels=labels,
            properties=properties,
            workspace_rid=self._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
        )
        resp = self._clients.containerized_extractors.search_containerized_extractors(
            self._clients.auth_header, request=ingest_api.SearchContainerizedExtractorsRequest(query=query)
        )
        return [ContainerizedExtractor._from_conjure(self._clients, extractor) for extractor in resp]

    def get_workbook(self, rid: str) -> Workbook:
        """Gets the given workbook by rid."""
        raw_workbook = self._clients.notebook.get(self._clients.auth_header, rid)
        return Workbook._from_conjure(self._clients, raw_workbook)

    def _iter_search_workbooks(
        self, query: scout_notebook_api.SearchNotebooksQuery, include_archived: bool
    ) -> Iterable[Workbook]:
        for raw_workbook in search_workbooks_paginated(
            self._clients.notebook, self._clients.auth_header, query, include_archived
        ):
            try:
                yield Workbook._from_notebook_metadata(self._clients, raw_workbook)
            except ValueError:
                logger.exception(
                    "Failed to deserialize workbook metadata with rid %s: %s", raw_workbook.rid, raw_workbook
                )

    def search_workbooks(
        self,
        *,
        include_archived: bool = False,
        exact_match: str | None = None,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        asset: Asset | str | None = None,
        exact_assets: Sequence[Asset | str] | None = None,
        created_by: User | str | None = None,
        run: Run | str | None = None,
        workspace: WorkspaceSearchT | None = WorkspaceSearchType.ALL,
        archived: bool | None = None,
    ) -> Sequence[Workbook]:
        """Search for workbooks meeting the specified filters.
        Filters are ANDed together, e.g. `(workbook.label == label) AND (workbook.created_by == "rid")`

        Args:
            include_archived: If true, include archived workbooks in results
            exact_match: Searches for a string to match exactly in the workbook's metadata
            search_text: Fuzzy-searches for a string in the workbook's metadata
            labels: A list of labels that must ALL be present on an workbook to be included.
            properties: A mapping of key-value pairs that must ALL be present on an workbook to be included.
            asset: Searches for workbooks that include the given asset
            exact_assets: Searches for workbooks that have the exact given assets
            created_by: Searches for workbooks with the given author
            run: Searches for workbooks with the given run
            workspace: Filters search to given workspace.
            archived: Return workbooks that are either archived or not

        NOTE: If WorkspaceSearchType.ALL is given for `workspace`(default), searches within all workspaces the user can
            access. If WorkspaceSearchType.DEFAULT, searches within the default workspace if configured, or raises
            a NominalConfigError if one is not configured. If a Workspace or a workspace rid is given, searches will
            be constrained to that workspace if the user has access to the workspace.


        Returns:
            All workbooks which match all of the provided conditions
        """
        query = create_search_workbooks_query(
            exact_match=exact_match,
            search_text=search_text,
            labels=labels,
            properties=properties,
            asset_rid=None if asset is None else rid_from_instance_or_string(asset),
            exact_asset_rids=None
            if exact_assets is None
            else [rid_from_instance_or_string(asset) for asset in exact_assets],
            author_rid=None if created_by is None else rid_from_instance_or_string(created_by),
            run_rid=None if run is None else rid_from_instance_or_string(run),
            workspace_rid=self._workspace_rid_for_search(workspace or WorkspaceSearchType.ALL),
            archived=archived,
        )
        return list(self._iter_search_workbooks(query, include_archived))

    def get_workbook_template(self, rid: str) -> WorkbookTemplate:
        """Gets the given workbook template by rid."""
        raw_template = self._clients.template.get(self._clients.auth_header, rid)
        return WorkbookTemplate._from_conjure(self._clients, raw_template)

    def _iter_search_workbook_templates(
        self, query: scout_template_api.SearchTemplatesQuery
    ) -> Iterable[WorkbookTemplate]:
        for raw_template in search_workbook_templates_paginated(
            self._clients.template, self._clients.auth_header, query
        ):
            yield WorkbookTemplate._from_template_summary(self._clients, raw_template)

    def search_workbook_templates(
        self,
        *,
        exact_match: str | None = None,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        created_by: User | str | None = None,
        archived: bool | None = None,
        published: bool | None = None,
    ) -> Sequence[WorkbookTemplate]:
        """Search for workbook templates meeting the specified filters.
        Filters are ANDed together, e.g. `(workbook.label == label) AND (workbook.author_rid == "rid")`

        Args:
            exact_match: Searches for a string to match exactly in the template's metadata
            search_text: Fuzzy-searches for a string in the template's metadata
            labels: A list of labels that must ALL be present on an workbook to be included.
            properties: A mapping of key-value pairs that must ALL be present on an workbook to be included.
            created_by: Searches for workbook templates with the given creator's rid
            archived: Searches for workbook templates that are archived if true
            published: Searches f8or workbook templates that have been published if true

        Returns:
            All workbook templates which match all of the provided conditions
        """
        query = create_search_workbook_templates_query(
            exact_match=exact_match,
            search_text=search_text,
            labels=labels,
            properties=properties,
            created_by=None if created_by is None else rid_from_instance_or_string(created_by),
            archived=archived,
            published=published,
        )
        return list(self._iter_search_workbook_templates(query))

    @deprecated(
        "Calling `NominalClient.create_workbook_from_template` is deprecated and will be removed "
        "in a future release. Use `Template.create_workbook` instead"
    )
    def create_workbook_from_template(
        self,
        template_rid: str,
        run_rid: str,
        title: str | None = None,
        description: str | None = None,
        is_draft: bool = False,
    ) -> Workbook:
        """Creates a workbook from a workbook template.

        NOTE: is_draft is intentionally unused and will be removed in a future release.
        """
        template = self.get_workbook_template(template_rid)
        return template.create_workbook(
            title=title,
            description=description,
            run=run_rid,
        )
