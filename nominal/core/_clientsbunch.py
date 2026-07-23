from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Protocol, TypeVar

from conjure_python_client import Service, ServiceConfiguration
from nominal_api import (
    attachments_api,
    authentication_api,
    event,
    ingest_api,
    scout,
    scout_assets,
    scout_catalog,
    scout_checklistexecution_api,
    scout_checks_api,
    scout_compute_api,
    scout_dataexport_api,
    scout_datareview_api,
    scout_datasource,
    scout_datasource_connection,
    scout_video,
    storage_datasource_api,
    storage_writer_api,
    timeseries_channelmetadata,
    timeseries_metadata,
    upload_api,
)
from typing_extensions import Self

from nominal._utils.dataclass_tools import LazyField
from nominal.core._utils.grpc_tools import GRPCStub, create_grpc_channel, translate_grpc_errors
from nominal.core._utils.networking import (
    HeaderProvider,
    create_conjure_client_factory,
)
from nominal.core.exceptions import NominalConfigError
from nominal.protos.authorization.roles.v1 import roles_pb2_grpc
from nominal.protos.comments.v1 import comments_pb2_grpc
from nominal.protos.ingest.v2 import containerized_extractor_pb2_grpc, ingest_service_pb2_grpc
from nominal.protos.registry.v2 import registry_pb2_grpc
from nominal.protos.sandbox.v1 import sandbox_workspace_pb2_grpc
from nominal.protos.secrets.v1 import secrets_pb2_grpc
from nominal.protos.units.v1 import units_pb2_grpc
from nominal.protos.workspaces.v1 import workspaces_pb2, workspaces_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

ON_BEHALF_OF_USER_RID_HEADER = "X-Nominal-On-Behalf-Of-User"
TService = TypeVar("TService", bound=Service)
TStub = TypeVar("TStub")


@dataclass(frozen=True)
class RequestMetrics:
    largest_latency_before_request: float
    """
    delta between current time and oldest timestamp before request (seconds)
    """
    smallest_latency_before_request: float
    """
    delta between current time and newest timestamp before request (seconds)
    """
    request_rtt: float
    """
    delta between before and after request (seconds)
    """
    largest_latency_after_request: float
    """
    delta between current time and oldest timestamp after request (seconds)
    """
    smallest_latency_after_request: float
    """
    delta between current time and newest timestamp after request (seconds)
    """


class ProtoWriteService(Service):
    def write_nominal_batches(self, auth_header: str, data_source_rid: str, request: bytes) -> None:
        _headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-protobuf",
            "Authorization": auth_header,
        }
        _path = f"/storage/writer/v1/nominal/{data_source_rid}"
        self._request("POST", self._uri + _path, params={}, headers=_headers, data=request)

    def write_nominal_batches_with_metrics(
        self,
        auth_header: str,
        data_source_rid: str,
        request: bytes,
        oldest_timestamp: IntegralNanosecondsUTC,
        newest_timestamp: IntegralNanosecondsUTC,
    ) -> RequestMetrics:
        _headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-protobuf",
            "Authorization": auth_header,
        }
        _path = f"/storage/writer/v1/nominal/{data_source_rid}"
        before_req = time.time_ns()

        self._request("POST", self._uri + _path, params={}, headers=_headers, data=request)

        after_req = time.time_ns()

        return RequestMetrics(
            largest_latency_before_request=(before_req - oldest_timestamp) / 1e9,
            smallest_latency_before_request=(before_req - newest_timestamp) / 1e9,
            request_rtt=(after_req - before_req) / 1e9,
            largest_latency_after_request=(after_req - oldest_timestamp) / 1e9,
            smallest_latency_after_request=(after_req - newest_timestamp) / 1e9,
        )

    def write_prometheus_batches(self, auth_header: str, data_source_rid: str, request: bytes) -> None:
        _headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-protobuf",
            "Authorization": auth_header,
        }
        _path = f"/storage/writer/v1/prometheus/{data_source_rid}"
        self._request("POST", self._uri + _path, params={}, headers=_headers, data=request)


@dataclass(frozen=True)
class ClientsBunch:
    auth_header: str
    workspace_rid: str | None
    app_base_url: str
    header_provider: HeaderProvider | None
    _api_base_url: str = field(repr=False)
    _user_agent: str = field(repr=False)
    _token: str = field(repr=False)
    _service_config: ServiceConfiguration = field(repr=False)

    _default_workspace: LazyField[workspaces_pb2.Workspace] = field(
        default_factory=LazyField,
        init=False,
        repr=False,
        compare=False,
    )

    # Conjure services
    assets: scout_assets.AssetService
    attachment: attachments_api.AttachmentService
    authentication: authentication_api.AuthenticationServiceV2
    catalog: scout_catalog.CatalogService
    channel_metadata: timeseries_channelmetadata.ChannelMetadataService
    checklist_execution: scout_checklistexecution_api.ChecklistExecutionService
    checklist: scout_checks_api.ChecklistService
    compute: scout_compute_api.ComputeService
    connection: scout_datasource_connection.ConnectionService
    dataexport: scout_dataexport_api.DataExportService
    datareview: scout_datareview_api.DataReviewService
    datasource: scout_datasource.DataSourceService
    event: event.EventService
    ingest_jobs: ingest_api.IngestJobService
    ingest: ingest_api.IngestService
    notebook: scout.NotebookService
    proto_write: ProtoWriteService
    run: scout.RunService
    series_metadata: timeseries_metadata.SeriesMetadataService
    storage_writer: storage_writer_api.NominalChannelWriterService
    storage: storage_datasource_api.NominalDataSourceService
    template: scout.TemplateService
    upload: upload_api.UploadService
    video_file: scout_video.VideoFileService
    video: scout_video.VideoService

    # GRPC services
    comments: comments_pb2_grpc.CommentsServiceStub
    containerized_extractor: containerized_extractor_pb2_grpc.ContainerizedExtractorServiceStub
    ingest_v2: ingest_service_pb2_grpc.IngestServiceStub
    registry: registry_pb2_grpc.RegistryServiceStub
    roles: roles_pb2_grpc.RoleServiceStub
    sandbox_workspace: sandbox_workspace_pb2_grpc.SandboxWorkspaceServiceStub
    secrets: secrets_pb2_grpc.SecretServiceStub
    units: units_pb2_grpc.UnitsServiceStub
    workspace: workspaces_pb2_grpc.WorkspaceServiceStub

    def _get_workspace_by_rid(self, workspace_rid: str) -> workspaces_pb2.Workspace:
        """Fetch a single workspace by its RID via the gRPC workspace service.

        Centralizes the single call site for fetching a workspace by RID (the RPC plus gRPC error translation).
        """
        with translate_grpc_errors():
            return self.workspace.GetWorkspace(
                workspaces_pb2.GetWorkspaceRequest(workspace_rid=workspace_rid)
            ).workspace

    def _fetch_default_workspace(self) -> workspaces_pb2.Workspace:
        """Fetch the workspace object this client should treat as its default.

        Pinned clients resolve their configured workspace RID as the default. Unpinned clients fall back to the
        tenant-wide default workspace endpoint.
        """
        # User has explicitly configured a default workspace in the config profile -> retrieve that workspace
        if self.workspace_rid is not None:
            return self._get_workspace_by_rid(self.workspace_rid)

        # User has not explicitly configured a default workspace in the config profile -> get tenant-wide default
        with translate_grpc_errors():
            response = self.workspace.GetDefaultWorkspace(workspaces_pb2.GetDefaultWorkspaceRequest())
        if response.HasField("workspace"):
            return response.workspace

        raise NominalConfigError(
            "Could not retrieve default workspace! "
            "Either the user is not authorized to access or there is no default workspace."
        )

    def resolve_default_workspace_rid(self) -> str:
        """Resolve the default workspace RID for this client bundle.

        Resolution flow:
        1. Lazily resolve and cache the workspace object this client treats as its default.
        2. Return the resolved workspace RID.

        Note:
            Pinned clients validate the configured `workspace_rid` by fetching that workspace on first use. Unpinned
            clients resolve through the tenant default workspace endpoint. In both cases, the cached workspace object
            is reused by later calls to `resolve_workspace()`.

        Returns:
            The resolved default workspace RID.

        Raises:
            NominalConfigError: If no default workspace can be resolved.
        """
        return self._default_workspace.get_or_init(self._fetch_default_workspace).rid

    def resolve_workspace(self, workspace_rid: str | None = None) -> workspaces_pb2.Workspace:
        """Resolve an optionally provided workspace rid to the correct RID to use in requests.

        Args:
            workspace_rid: The workspace RID to fetch and validate. If None, resolves the client's default
                workspace by preferring an explicitly configured `workspace_rid` and otherwise falling back to the
                tenant default workspace.

        Returns:
            The resolved workspace object.

        Note:
            If the default workspace was already resolved on this client and `workspace_rid` matches that RID, the
            cached workspace object is returned without another workspace-service request.

        Raises:
            NominalConfigError: If `workspace_rid` is None and no default workspace can be resolved.
            NominalNotFoundError: If an explicit workspace RID does not exist or is not accessible to the user (the
                backend does not distinguish the two).
        """
        if workspace_rid is None:
            # `_default_workspace` caches the single workspace object this client resolves as "default", whether that
            # came from a configured workspace RID or the tenant default endpoint.
            return self._default_workspace.get_or_init(self._fetch_default_workspace)

        # If the default workspace has been initialized, and we are explicitly fetching that workspace,
        # short-circuit and return the cached workspace object
        if self._default_workspace.is_initialized():
            raw_workspace = self._default_workspace.get()
            if raw_workspace.rid == workspace_rid:
                return raw_workspace

        # Retrieve the workspace by rid
        return self._get_workspace_by_rid(workspace_rid)

    @classmethod
    def from_config(
        cls,
        cfg: ServiceConfiguration,
        base_url: str,
        agent: str,
        token: str,
        workspace_rid: str | None,
        *,
        header_provider: HeaderProvider | None = None,
    ) -> Self:
        app_base_url = api_base_url_to_app_base_url(base_url)

        def client_factory(service_class: type[TService]) -> TService:
            return create_conjure_client_factory(
                user_agent=agent,
                service_config=cfg,
                header_provider=header_provider,
            )(service_class)

        grpc_channel = create_grpc_channel(
            api_base_url=base_url,
            service_config=cfg,
            user_agent=agent,
            auth_header=f"Bearer {token}",
            header_provider=header_provider,
        )

        def grpc_factory(stub_class: GRPCStub[TStub]) -> TStub:
            return stub_class(grpc_channel)

        return cls(
            auth_header=f"Bearer {token}",
            workspace_rid=workspace_rid,
            app_base_url=app_base_url,
            header_provider=header_provider,
            _api_base_url=base_url,
            _user_agent=agent,
            _token=token,
            _service_config=cfg,
            # Conjure Service Stubs
            assets=client_factory(scout_assets.AssetService),
            attachment=client_factory(attachments_api.AttachmentService),
            authentication=client_factory(authentication_api.AuthenticationServiceV2),
            catalog=client_factory(scout_catalog.CatalogService),
            channel_metadata=client_factory(timeseries_channelmetadata.ChannelMetadataService),
            checklist_execution=client_factory(scout_checklistexecution_api.ChecklistExecutionService),
            checklist=client_factory(scout_checks_api.ChecklistService),
            compute=client_factory(scout_compute_api.ComputeService),
            connection=client_factory(scout_datasource_connection.ConnectionService),
            dataexport=client_factory(scout_dataexport_api.DataExportService),
            datareview=client_factory(scout_datareview_api.DataReviewService),
            datasource=client_factory(scout_datasource.DataSourceService),
            event=client_factory(event.EventService),
            ingest_jobs=client_factory(ingest_api.IngestJobService),
            ingest=client_factory(ingest_api.IngestService),
            notebook=client_factory(scout.NotebookService),
            proto_write=client_factory(ProtoWriteService),
            run=client_factory(scout.RunService),
            series_metadata=client_factory(timeseries_metadata.SeriesMetadataService),
            storage_writer=client_factory(storage_writer_api.NominalChannelWriterService),
            storage=client_factory(storage_datasource_api.NominalDataSourceService),
            template=client_factory(scout.TemplateService),
            upload=client_factory(upload_api.UploadService),
            video_file=client_factory(scout_video.VideoFileService),
            video=client_factory(scout_video.VideoService),
            # GRPC Service Stubs
            comments=grpc_factory(comments_pb2_grpc.CommentsServiceStub),
            containerized_extractor=grpc_factory(containerized_extractor_pb2_grpc.ContainerizedExtractorServiceStub),
            ingest_v2=grpc_factory(ingest_service_pb2_grpc.IngestServiceStub),
            registry=grpc_factory(registry_pb2_grpc.RegistryServiceStub),
            roles=grpc_factory(roles_pb2_grpc.RoleServiceStub),
            sandbox_workspace=grpc_factory(sandbox_workspace_pb2_grpc.SandboxWorkspaceServiceStub),
            secrets=grpc_factory(secrets_pb2_grpc.SecretServiceStub),
            units=grpc_factory(units_pb2_grpc.UnitsServiceStub),
            workspace=grpc_factory(workspaces_pb2_grpc.WorkspaceServiceStub),
        )


class HasScoutParams(Protocol):
    @property
    def auth_header(self) -> str: ...
    @property
    def workspace_rid(self) -> str | None: ...
    @property
    def app_base_url(self) -> str: ...
    @property
    def header_provider(self) -> HeaderProvider | None: ...
    def resolve_workspace(self, workspace_rid: str | None = None) -> workspaces_pb2.Workspace: ...
    def resolve_default_workspace_rid(self) -> str: ...


def api_base_url_to_app_base_url(api_base_url: str, fallback: str = "") -> str:
    """Convert from API base URL to APP base URL.

    Rules:
    - https://api$ANYTHING/api -> https://app$ANYTHING
    - https://api$ANYTHING -> https://app$ANYTHING (this is mainly for local dev @ api.nominal.test)

    Examples:
    - https://api.gov.nominal.io/api -> https://app.gov.nominal.io
    - https://api-staging.gov.nominal.io/api -> https://app-staging.gov.nominal.io
    - https://api.nominal.test -> https://app.nominal.test
    """
    api_base_url = api_base_url.rstrip("/")
    match = re.match(r"^(https?://)api([^/]*)(/api)?", api_base_url)
    if match:
        return f"{match.group(1)}app{match.group(2)}"
    return fallback
