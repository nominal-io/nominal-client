from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from io import TextIOBase
from pathlib import Path
from typing import BinaryIO, Iterable, Mapping, Sequence

import certifi
import conjure_python_client
from conjure_python_client import ServiceConfiguration, SslConfiguration
from nominal_api import (
    api,
    attachments_api,
    authentication_api,
    datasource_logset_api,
    event,
    ingest_api,
    scout_asset_api,
    scout_checks_api,
    scout_datasource_connection_api,
    scout_notebook_api,
    scout_run_api,
    scout_video_api,
    secrets_api,
    storage_datasource_api,
    timeseries_logicalseries_api,
)
from typing_extensions import Self, deprecated

from nominal import _config
from nominal.config import NominalConfig
from nominal.core import _conjure_utils
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._constants import DEFAULT_API_BASE_URL
from nominal.core._multipart import path_upload_name, upload_multipart_file, upload_multipart_io
from nominal.core._utils import construct_user_agent_string, rid_from_instance_or_string
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.channel import Channel
from nominal.core.checklist import Checklist
from nominal.core.connection import Connection, StreamingConnection
from nominal.core.data_review import DataReview, DataReviewBuilder
from nominal.core.dataset import (
    Dataset,
    _build_channel_config,
    _construct_new_ingest_options,
    _create_dataflash_ingest_request,
    _create_dataset,
    _create_mcap_channels,
    _create_mcap_ingest_request,
    _get_dataset,
    _get_datasets,
)
from nominal.core.event import Event, EventType
from nominal.core.filetype import FileType, FileTypes
from nominal.core.log import Log, LogSet, _get_log_set, _log_timestamp_type_to_conjure, _logs_to_conjure
from nominal.core.run import Run
from nominal.core.secret import Secret
from nominal.core.unit import Unit, UnitMapping, _available_units, _build_unit_update
from nominal.core.user import User
from nominal.core.video import Video, _build_video_file_timestamp_manifest
from nominal.core.workbook import Workbook
from nominal.core.workspace import Workspace
from nominal.exceptions import NominalError, NominalIngestError
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    LogTimestampType,
    _AnyTimestampType,
    _SecondsNanos,
    _to_api_duration,
)

logger = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT = timedelta(seconds=30)


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
        for raw_user in _conjure_utils.search_users_paginated(
            self._clients.authentication, self._clients.auth_header, query
        ):
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
        query = _conjure_utils.create_search_users_query(exact_match, search_text)
        return list(self._iter_search_users(query))

    def get_workspace(self, workspace_rid: str | None = None) -> Workspace:
        """Get workspace via given RID, or the default workspace if no RID is provided.

        Args:
            workspace_rid: If provided, the RID of the workspace to retrieve. If None, retrieves the default workspace.

        Returns:
            Returns details about the requested workspace.

        Raises:
            RuntimeError: Raises a RuntimeError if a workspace is not provided, but there is no configured default
                workspace for the current user.
        """
        if workspace_rid is None:
            raw_workspace = self._clients.workspace.get_default_workspace(self._clients.auth_header)
            if raw_workspace is None:
                raise RuntimeError(
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
        for secret in _conjure_utils.search_secrets_paginated(self._clients.secrets, self._clients.auth_header, query):
            yield Secret._from_conjure(self._clients, secret)

    def search_secrets(
        self,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Sequence[Secret]:
        """Search for secrets meeting the specified filters.
        Filters are ANDed together, e.g. `(secret.label == label) AND (secret.property == property)`

        Args:
            search_text: Searches for a (case-insensitive) substring across all text fields.
            labels: A sequence of labels that must ALL be present on a secret to be included.
            properties: A mapping of key-value pairs that must ALL be present on a secret to be included.

        Returns:
            All secrets which match all of the provided conditions
        """
        query = _conjure_utils.create_search_secrets_query(search_text, labels, properties)
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
    ) -> Iterable[Run]:
        query = _conjure_utils.create_search_runs_query(start, end, name_substring, labels, properties)
        for run in _conjure_utils.search_runs_paginated(self._clients.run, self._clients.auth_header, query):
            yield Run._from_conjure(self._clients, run)

    def search_runs(
        self,
        start: str | datetime | IntegralNanosecondsUTC | None = None,
        end: str | datetime | IntegralNanosecondsUTC | None = None,
        name_substring: str | None = None,
        *,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Sequence[Run]:
        """Search for runs meeting the specified filters.
        Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`

        Args:
            start: Inclusive start time for filtering runs.
            end: Inclusive end time for filtering runs.
            name_substring: Searches for a (case-insensitive) substring in the name.
            labels: A sequence of labels that must ALL be present on a run to be included.
            properties: A mapping of key-value pairs that must ALL be present on a run to be included.

        Returns:
            All runs which match all of the provided conditions
        """
        return list(self._iter_search_runs(start, end, name_substring, labels, properties))

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

    @deprecated(
        "Creating a dataset from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_dataset`, `get_dataset`, or `search_datasets` and add data to an existing dataset instead."
    )
    def create_csv_dataset(
        self,
        path: Path | str,
        name: str | None,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        channel_prefix: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Create a dataset from a CSV file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        return self.create_tabular_dataset(
            path,
            name,
            timestamp_column,
            timestamp_type,
            description,
            labels=labels,
            properties=properties,
            prefix_tree_delimiter=prefix_tree_delimiter,
            channel_prefix=channel_prefix,
            tags=tags,
        )

    @deprecated(
        "Creating a dataset from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_dataset`, `get_dataset`, or `search_datasets` and add data to an existing dataset instead."
    )
    def create_ardupilot_dataflash_dataset(
        self,
        path: Path | str,
        name: str | None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create a dataset from an ArduPilot DataFlash log file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path = Path(path)
        file_type = FileTypes.DATAFLASH
        if name is None:
            name = path.name

        s3_path = upload_multipart_file(
            self._clients.auth_header, self._clients.workspace_rid, path, self._clients.upload, file_type
        )
        target = ingest_api.DatasetIngestTarget(
            new=ingest_api.NewDatasetIngestDestination(
                labels=list(labels),
                properties={} if properties is None else dict(properties),
                dataset_description=description,
                dataset_name=name,
                channel_config=_build_channel_config(prefix_tree_delimiter),
                workspace=self._clients.workspace_rid,
            )
        )
        request = _create_dataflash_ingest_request(s3_path, target)
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.dataset is None:
            raise NominalIngestError("error ingesting dataflash: no dataset created")
        return self.get_dataset(response.details.dataset.dataset_rid)

    @deprecated(
        "Creating a dataset from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_dataset`, `get_dataset`, or `search_datasets` and add data to an existing dataset instead."
    )
    def create_tabular_dataset(
        self,
        path: Path | str,
        name: str | None,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        channel_prefix: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Create a dataset from a table-like file.

        Currently, the supported filetypes are:
            - .csv / .csv.gz
            - .parquet / .parquet.gz
            - .parquet.tar / .parquet.tar.gz / .parquet.zip

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path = Path(path)
        file_type = FileType.from_tabular(path)
        if name is None:
            name = path.name

        with path.open("rb") as data_file:
            return self.create_dataset_from_io(
                data_file,
                name=name,
                timestamp_column=timestamp_column,
                timestamp_type=timestamp_type,
                file_type=file_type,
                description=description,
                labels=labels,
                properties=properties,
                prefix_tree_delimiter=prefix_tree_delimiter,
                channel_prefix=channel_prefix,
                tags=tags,
            )

    @deprecated(
        "Creating a dataset from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_dataset`, `get_dataset`, or `search_datasets` and add data to an existing dataset instead."
    )
    def create_journal_json_dataset(
        self,
        path: Path | str,
        name: str | None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create a dataset from a journal log file with json output format.

        Intended to be used with the recorded output of `journalctl --output json ...`.
        The path extension is expected to be `.jsonl` or `.jsonl.gz` if gzipped.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path = Path(path)
        file_type = FileType.from_path_journal_json(path)

        if name is None:
            name = path.name

        s3_path = upload_multipart_file(
            self._clients.auth_header, self._clients.workspace_rid, path, self._clients.upload, file_type
        )
        request = ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                journal_json=ingest_api.JournalJsonOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                    target=ingest_api.DatasetIngestTarget(
                        new=ingest_api.NewDatasetIngestDestination(
                            labels=list(labels),
                            properties={} if properties is None else dict(properties),
                            dataset_description=description,
                            dataset_name=name,
                            channel_config=_build_channel_config(prefix_tree_delimiter),
                            workspace=self._clients.workspace_rid,
                        )
                    ),
                )
            ),
        )

        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.dataset is None:
            raise NominalIngestError("error ingesting journal json: no dataset created")
        return self.get_dataset(response.details.dataset.dataset_rid)

    @deprecated(
        "Creating a dataset from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_dataset`, `get_dataset`, or `search_datasets` and add data to an existing dataset instead."
    )
    def create_dataset_from_io(
        self,
        dataset: BinaryIO,
        name: str,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        file_type: tuple[str, str] | FileType = FileTypes.CSV,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        channel_prefix: str | None = None,
        file_name: str | None = None,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Create a dataset from a file-like object.
        The dataset must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.

        Timestamp column types must be a `CustomTimestampFormat` or one of the following literals:
            "iso_8601": ISO 8601 formatted strings,
            "epoch_{unit}": epoch timestamps in UTC (floats or ints),
            "relative_{unit}": relative timestamps (floats or ints),
            where {unit} is one of: nanoseconds | microseconds | milliseconds | seconds | minutes | hours | days

        Args:
            dataset: Binary file-like tabular data stream
            name: Name of the dataset to create
            timestamp_column: Column of data containing timestamp information for all other columns
            timestamp_type: Type of timestamps contained within timestamp_column
            file_type: Type of file being ingested (e.g. CSV, parquet, etc.). Used for naming the file uploaded
                to cloud storage as part of ingestion.
            description: Human-readable description of the dataset to create
            labels: Text labels to apply to the created dataset
            properties: Key-value properties to apply to the cleated dataset
            prefix_tree_delimiter: If present, the delimiter to represent tiers when viewing channels hierarchically.
            channel_prefix: Prefix to apply to newly created channels
            file_name: Name of the file (without extension) to create when uploading.
            tag_columns: a dictionary mapping tag keys to column names.
            tags: a dictionary of key-value tags to apply to all data within the file

        Returns:
            Reference to the constructed dataset object.
        """
        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)

        # Prevent breaking changes from customers using create_dataset_from_io directly
        if file_name is None:
            file_name = name

        s3_path = upload_multipart_io(
            self._clients.auth_header, self._clients.workspace_rid, dataset, file_name, file_type, self._clients.upload
        )

        request = ingest_api.IngestRequest(
            options=_construct_new_ingest_options(
                name=name,
                timestamp_column=timestamp_column,
                timestamp_type=timestamp_type,
                file_type=file_type,
                description=description,
                labels=labels,
                properties={} if properties is None else properties,
                prefix_tree_delimiter=prefix_tree_delimiter,
                channel_prefix=channel_prefix,
                tag_columns=tag_columns,
                s3_path=s3_path,
                workspace_rid=self._clients.workspace_rid,
                tags=tags,
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if not response.details.dataset:
            raise NominalIngestError("error ingesting dataset: no dataset created")
        return self.get_dataset(response.details.dataset.dataset_rid)

    @deprecated(
        "Creating a dataset from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_dataset`, `get_dataset`, or `search_datasets` and add data to an existing dataset instead."
    )
    def create_mcap_dataset(
        self,
        path: Path | str,
        name: str | None,
        description: str | None = None,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create a dataset from an MCAP file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_mcap_io` for more details on the other arguments.
        """
        mcap_path = Path(path)
        if name is None:
            name = mcap_path.name

        with mcap_path.open("rb") as mcap_file:
            return self.create_dataset_from_mcap_io(
                mcap_file,
                name=name,
                description=description,
                include_topics=include_topics,
                exclude_topics=exclude_topics,
                labels=labels,
                properties=properties,
                prefix_tree_delimiter=prefix_tree_delimiter,
                file_name=path_upload_name(mcap_path, FileTypes.MCAP),
            )

    @deprecated(
        "Creating a dataset from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_dataset`, `get_dataset`, or `search_datasets` and add data to an existing dataset instead."
    )
    def create_dataset_from_mcap_io(
        self,
        dataset: BinaryIO,
        name: str,
        description: str | None = None,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        file_name: str | None = None,
    ) -> Dataset:
        """Create a dataset from an mcap file-like object.

        The dataset must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.

        Args:
            dataset: Binary file-like MCAP stream
            name: Name of the dataset to create
            description: Human-readable description of the dataset to create
            include_topics: If present, list of topics to restrict ingestion to.
                If not present, defaults to all protobuf-encoded topics present in the MCAP.
            exclude_topics: If present, list of topics to not ingest from the MCAP.
            labels: Text labels to apply to the created dataset
            properties: Key-value properties to apply to the cleated dataset
            prefix_tree_delimiter: If present, the delimiter to represent tiers when viewing channels hierarchically.
            file_name: If present, name (without extension) to use when uploading file. Otherwise, defaults to name.

        Returns:
            Reference to the constructed dataset object.
        """
        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset} must be open in binary mode, rather than text mode")

        if file_name is None:
            file_name = name

        s3_path = upload_multipart_io(
            self._clients.auth_header,
            self._clients.workspace_rid,
            dataset,
            file_name,
            file_type=FileTypes.MCAP,
            upload_client=self._clients.upload,
        )
        channels = _create_mcap_channels(include_topics, exclude_topics)
        target = ingest_api.DatasetIngestTarget(
            new=ingest_api.NewDatasetIngestDestination(
                dataset_name=name,
                dataset_description=description,
                properties={} if properties is None else dict(properties),
                labels=list(labels),
                channel_config=_build_channel_config(prefix_tree_delimiter),
                workspace=self._clients.workspace_rid,
            )
        )
        request = _create_mcap_ingest_request(s3_path, channels, target)
        resp = self._clients.ingest.ingest(self._clients.auth_header, request)
        if resp.details.dataset is not None:
            dataset_rid = resp.details.dataset.dataset_rid
            if dataset_rid is not None:
                return self.get_dataset(dataset_rid)
            raise NominalIngestError("error ingesting mcap: no dataset rid")
        raise NominalIngestError("error ingesting mcap: no dataset created")

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
        )
        raw_video = self._clients.video.create(self._clients.auth_header, request)
        return Video._from_conjure(self._clients, raw_video)

    @deprecated(
        "Creating a video from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_empty_video` or `get_video` and add video files to an existing video instead."
    )
    def create_video(
        self,
        path: Path | str,
        name: str | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Video:
        """Create a video from an h264/h265 encoded video file (mp4, mkv, ts, etc.).

        If name is None, the name of the file will be used.

        See `create_video_from_io` for more details.
        """
        path = Path(path)
        file_type = FileType.from_video(path)
        if name is None:
            name = path.name

        with path.open("rb") as data_file:
            return self.create_video_from_io(
                data_file,
                name=name,
                start=start,
                frame_timestamps=frame_timestamps,
                file_type=file_type,
                description=description,
                labels=labels,
                properties=properties,
                file_name=path_upload_name(path, file_type),
            )

    @deprecated(
        "Creating a video from a file via the client is deprecated and will be removed in a future version. "
        "Use `create_empty_video` or `get_video` and add video files to an existing video instead."
    )
    def create_video_from_io(
        self,
        video: BinaryIO,
        name: str,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        description: str | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MP4,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        file_name: str | None = None,
    ) -> Video:
        """Create a video from a file-like object.
        The video must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.

        Args:
        ----
            video: file-like object to read video data from
            name: Name of the video to create in Nominal
            start: Starting timestamp of the video
            frame_timestamps: Per-frame timestamps (in nanoseconds since unix epoch) for every frame of the video
            description: Description of the video to create in nominal
            file_type: Type of data being uploaded, used for naming the file uploaded to cloud storage as part
                of ingestion.
            labels: Labels to apply to the video in nominal
            properties: Properties to apply to the video in nominal
            file_name: Name (without extension) to use when uploading the video file. Defaults to video name.

        Returns:
        -------
            Handle to the created video

        Note:
        ----
            Exactly one of 'start' and 'frame_timestamps' **must** be provided. Most users will
            want to provide a starting timestamp: frame_timestamps is primarily useful when the scale
            of the video data is not 1:1 with the playback speed or non-uniform over the course of the video,
            for example, 200fps video artificially slowed to 30 fps without dropping frames. This will result
            in the playhead on charts within the product playing at the rate of the underlying data rather than
            time elapsed in the video playback.

        """
        if isinstance(video, TextIOBase):
            raise TypeError(f"video {video} must be open in binary mode, rather than text mode")

        timestamp_manifest = _build_video_file_timestamp_manifest(
            self._clients.auth_header, self._clients.workspace_rid, self._clients.upload, start, frame_timestamps
        )

        if file_name is None:
            file_name = name

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(
            self._clients.auth_header, self._clients.workspace_rid, video, file_name, file_type, self._clients.upload
        )
        request = ingest_api.IngestRequest(
            ingest_api.IngestOptions(
                video=ingest_api.VideoOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)),
                    target=ingest_api.VideoIngestTarget(
                        new=ingest_api.NewVideoIngestDestination(
                            title=name,
                            description=description,
                            properties={} if properties is None else dict(properties),
                            labels=list(labels),
                            workspace=self._clients.workspace_rid,
                        )
                    ),
                    timestamp_manifest=timestamp_manifest,
                )
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.video is None:
            raise NominalIngestError("error ingesting video: no video created")
        return self.get_video(response.details.video.video_rid)

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Add logs to an existing dataset with dataset.write_logs instead."
    )
    def create_log_set(
        self,
        name: str,
        logs: Iterable[Log] | Iterable[tuple[datetime | IntegralNanosecondsUTC, str]],
        timestamp_type: LogTimestampType = "absolute",
        description: str | None = None,
    ) -> LogSet:
        """Create an immutable log set with the given logs.

        The logs are attached during creation and cannot be modified afterwards. Logs can either be of type `Log`
        or a tuple of a timestamp and a string. Timestamp type must be either 'absolute' or 'relative'.
        """
        request = datasource_logset_api.CreateLogSetRequest(
            name=name,
            description=description,
            origin_metadata={},
            timestamp_type=_log_timestamp_type_to_conjure(timestamp_type),
            workspace=self._clients.workspace_rid,
        )
        response = self._clients.logset.create(self._clients.auth_header, request)
        return self._attach_logs_and_finalize(response.rid, _logs_to_conjure(logs))

    def _attach_logs_and_finalize(self, rid: str, logs: Iterable[datasource_logset_api.Log]) -> LogSet:
        request = datasource_logset_api.AttachLogsAndFinalizeRequest(logs=list(logs))
        response = self._clients.logset.attach_logs_and_finalize(
            auth_header=self._clients.auth_header, log_set_rid=rid, request=request
        )
        return LogSet._from_conjure(self._clients, response)

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

    @deprecated("LogSets are deprecated and will be removed in a future version.")
    def get_log_set(self, log_set_rid: str) -> LogSet:
        """Retrieve a log set along with its metadata given its RID."""
        response = _get_log_set(self._clients, log_set_rid)
        return LogSet._from_conjure(self._clients, response)

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
        for checklist in _conjure_utils.search_checklists_paginated(
            self._clients.checklist, self._clients.auth_header, query
        ):
            yield Checklist._from_conjure(self._clients, checklist)

    def search_checklists(
        self,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Sequence[Checklist]:
        """Search for checklists meeting the specified filters.
        Filters are ANDed together, e.g. `(checklist.label == label) AND (checklist.search_text =~ field)`

        Args:
            search_text: case-insensitive search for any of the keywords in all string fields
            labels: A sequence of labels that must ALL be present on a checklist to be included.
            properties: A mapping of key-value pairs that must ALL be present on a checklist to be included.

        Returns:
            All checklists which match all of the provided conditions
        """
        query = _conjure_utils.create_search_checklists_query(search_text, labels, properties)
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

    def get_channel(self, rid: str) -> Channel:
        """Get metadata for a given channel by looking up its rid
        Args:
            rid: Identifier for the channel to look up
        Returns:
            Resolved metadata for the requested channel
        Raises:
            conjure_python_client.ConjureHTTPError: An error occurred while looking up the channel.
                This typically occurs when there is no such channel for the given RID.
        """
        warnings.warn(
            "get_channel is deprecated. Use dataset.get_channel() or connection.get_channel() instead.",
            UserWarning,
        )
        return Channel._from_conjure_logicalseries_api(
            self._clients, self._clients.logical_series.get_logical_series(self._clients.auth_header, rid)
        )

    def set_channel_units(self, rids_to_types: UnitMapping) -> Iterable[Channel]:
        """Sets the units for a set of channels based on user-provided unit symbols
        Args:
            rids_to_types: Mapping of channel RIDs -> unit symbols (e.g. 'm/s').
                NOTE: Providing `None` as the unit symbol clears any existing units for the channels.

        Returns:
        -------
            A sequence of metadata for all updated channels
        Raises:
            conjure_python_client.ConjureHTTPError: An error occurred while setting metadata on the channel.
                This typically occurs when either the units are invalid, or there are no
                channels with the given RIDs present.

        """
        warnings.warn(
            "set_channel_units is deprecated. Use dataset.set_channel_units() or connection.set_channel_units()",
            UserWarning,
        )

        series_updates = []
        for rid, series_type in rids_to_types.items():
            series_updates.append(
                timeseries_logicalseries_api.UpdateLogicalSeries(
                    logical_series_rid=rid,
                    unit_update=_build_unit_update(series_type),
                )
            )

        request = timeseries_logicalseries_api.BatchUpdateLogicalSeriesRequest(series_updates)
        response = self._clients.logical_series.batch_update_logical_series(self._clients.auth_header, request)
        return [Channel._from_conjure_logicalseries_api(self._clients, resp) for resp in response.responses]

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
            ),
        )
        conn = Connection._from_conjure(self._clients, connection_response)
        if isinstance(conn, StreamingConnection):
            return conn
        raise NominalError(f"Expected StreamingConnection but got {type(conn).__name__}")

    def create_workbook_from_template(
        self,
        template_rid: str,
        run_rid: str,
        title: str | None = None,
        description: str | None = None,
        is_draft: bool = False,
    ) -> Workbook:
        template = self._clients.template.get(self._clients.auth_header, template_rid)
        request = scout_notebook_api.CreateNotebookRequest(
            title=title if title is not None else f"Workbook from {template.metadata.title}",
            description=description or "",
            notebook_type=None,
            is_draft=is_draft,
            state_as_json="{}",
            charts=None,
            run_rid=run_rid,
            data_scope=None,
            layout=template.layout,
            content=template.content,
            content_v2=None,
            check_alert_refs=[],
            event_refs=[],
            workspace=self._clients.workspace_rid,
        )
        notebook = self._clients.notebook.create(self._clients.auth_header, request)

        return Workbook._from_conjure(self._clients, notebook)

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
        for asset in _conjure_utils.search_assets_paginated(self._clients.assets, self._clients.auth_header, query):
            yield Asset._from_conjure(self._clients, asset)

    def search_assets(
        self,
        search_text: str | None = None,
        *,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Sequence[Asset]:
        """Search for assets meeting the specified filters.
        Filters are ANDed together, e.g. `(asset.label == label) AND (asset.search_text =~ field)`

        Args:
            search_text: case-insensitive search for any of the keywords in all string fields
            labels: A sequence of labels that must ALL be present on a asset to be included.
            properties: A mapping of key-value pairs that must ALL be present on a asset to be included.

        Returns:
            All assets which match all of the provided conditions
        """
        query = _conjure_utils.create_search_assets_query(search_text, labels, properties)
        return list(self._iter_search_assets(query))

    def _iter_list_streaming_checklists(self, asset: str | None) -> Iterable[str]:
        if asset is None:
            return _conjure_utils.list_streaming_checklists_paginated(
                self._clients.checklist_execution, self._clients.auth_header
            )
        return _conjure_utils.list_streaming_checklists_for_asset_paginated(
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
        return DataReviewBuilder([], [], self._clients)

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
        assets: Iterable[Asset | str] = (),
        properties: Mapping[str, str] | None = None,
        labels: Iterable[str] = (),
    ) -> Event:
        request = event.CreateEvent(
            name=name,
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

    def get_events(self, uuids: Sequence[str]) -> Sequence[Event]:
        responses = self._clients.event.get_events(self._clients.auth_header, event.GetEvents(list(uuids)))
        return [Event._from_conjure(self._clients, response) for response in responses]

    def _iter_search_data_reviews(
        self,
        assets: Sequence[Asset | str] | None = None,
        runs: Sequence[Run | str] | None = None,
    ) -> Iterable[DataReview]:
        for review in _conjure_utils.search_data_reviews_paginated(
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
        for e in _conjure_utils.search_events_paginated(self._clients.event, self._clients.auth_header, query):
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

        Returns:
            All events which match all of the provided conditions
        """
        query = _conjure_utils.create_search_events_query(
            search_text=search_text,
            after=after,
            before=before,
            assets=None if assets is None else [rid_from_instance_or_string(asset) for asset in assets],
            labels=labels,
            properties=properties,
            created_by=None if created_by is None else rid_from_instance_or_string(created_by),
        )
        return list(self._iter_search_events(query))
