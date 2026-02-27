from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import requests
from google.protobuf.json_format import MessageToDict, ParseDict

if TYPE_CHECKING:
    from nominal.ingest_flow.v1 import ingest_flow_pb2

_proto_stubs_initialized = False


def _ensure_proto_stubs() -> None:
    global _proto_stubs_initialized
    if _proto_stubs_initialized:
        return

    spec = importlib.util.find_spec("nominal_api_protos")
    if spec is None or spec.origin is None:
        raise ImportError("nominal_api_protos is not installed — install with: uv pip install nominal-api-protos")

    site_packages = os.path.dirname(os.path.dirname(spec.origin))

    stubs = {
        "buf/validate/validate_pb2.py": ("buf/validate/validate.proto", "buf.validate"),
        "nominal_api_protos/nominal/gen/v1/alias_pb2.py": ("nominal/gen/v1/alias.proto", "nominal.gen.v1"),
        "nominal_api_protos/nominal/gen/v1/error_pb2.py": ("nominal/gen/v1/error.proto", "nominal.gen.v1"),
        "nominal_api_protos/nominal/conjure/v1/compat_pb2.py": ("nominal/conjure/v1/compat.proto", "nominal.conjure.v1"),
    }

    for rel_path, (proto_name, package) in stubs.items():
        file_path = os.path.join(site_packages, rel_path)
        if os.path.exists(file_path):
            continue

        from google.protobuf import descriptor_pb2

        fd = descriptor_pb2.FileDescriptorProto()
        fd.name = proto_name
        fd.package = package
        fd.syntax = "proto3"
        serialized = repr(fd.SerializeToString())

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        current = os.path.dirname(file_path)
        while current != site_packages and current != "/":
            init = os.path.join(current, "__init__.py")
            if not os.path.exists(init):
                open(init, "w").close()
            current = os.path.dirname(current)

        with open(file_path, "w") as f:
            f.write(
                f"from google.protobuf import descriptor as _descriptor\n"
                f"from google.protobuf import descriptor_pool as _descriptor_pool\n"
                f"from google.protobuf import runtime_version as _runtime_version\n"
                f"from google.protobuf import symbol_database as _symbol_database\n"
                f"from google.protobuf.internal import builder as _builder\n"
                f"_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 6, 31, 1, '', '{proto_name}')\n"
                f"_sym_db = _symbol_database.Default()\n"
                f"DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile({serialized})\n"
            )

    _proto_stubs_initialized = True


def _patch_nominal_path() -> None:
    spec = importlib.util.find_spec("nominal_api_protos")
    if spec is None or spec.origin is None:
        return
    protos_nominal = os.path.join(os.path.dirname(spec.origin), "nominal")
    import nominal
    if protos_nominal not in nominal.__path__:
        nominal.__path__.append(protos_nominal)


def _get_pb2():
    _ensure_proto_stubs()
    _patch_nominal_path()
    from nominal.ingest_flow.v1 import ingest_flow_pb2
    return ingest_flow_pb2


_SERVICE = "nominal.ingest_flow.v1.IngestFlowService"


_BASE_URL = "https://hw-tums.api-staging.gov.nominal.io"


@dataclass(frozen=True)
class IngestFlowClient:
    _token: str
    _pb2: object = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_pb2", _get_pb2())

    @property
    def _twirp_base(self) -> str:
        return f"{_BASE_URL}/twirp"

    def _call(self, method: str, request_proto, response_class):
        url = f"{self._twirp_base}/{_SERVICE}/{method}"
        resp = requests.post(
            url,
            json=MessageToDict(request_proto, preserving_proto_field_name=True),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._token}",
            },
        )
        if resp.status_code != 200:
            raise RuntimeError(f"IngestFlowService/{method} failed ({resp.status_code}): {resp.text}")
        return ParseDict(resp.json(), response_class())

    def search(
        self,
        search_text: str = "",
        page_size: int = 100,
        next_page_token: str | None = None,
    ) -> ingest_flow_pb2.SearchIngestFlowsResponse:
        pb2 = self._pb2
        kwargs = {}
        if next_page_token is not None:
            kwargs["next_page_token"] = next_page_token
        return self._call(
            "SearchIngestFlows",
            pb2.SearchIngestFlowsRequest(
                query=pb2.IngestFlowSearchQuery(search_text=search_text),
                page_size=page_size,
                **kwargs,
            ),
            pb2.SearchIngestFlowsResponse,
        )

    def get(self, rid: str) -> ingest_flow_pb2.GetIngestFlowResponse:
        pb2 = self._pb2
        return self._call(
            "GetIngestFlow",
            pb2.GetIngestFlowRequest(rid=rid),
            pb2.GetIngestFlowResponse,
        )

    def create(
        self,
        title: str,
        *,
        description: str | None = None,
        workspace: str | None = None,
        is_published: bool = False,
        state: ingest_flow_pb2.IngestFlowState | None = None,
        commit_message: str = "initial",
        initial_branch_name: str | None = None,
    ) -> ingest_flow_pb2.CreateIngestFlowResponse:
        pb2 = self._pb2
        kwargs = {}
        if description is not None:
            kwargs["description"] = description
        if workspace is not None:
            kwargs["workspace"] = workspace
        if state is not None:
            kwargs["state"] = state
        if initial_branch_name is not None:
            kwargs["initial_branch_name"] = initial_branch_name
        return self._call(
            "CreateIngestFlow",
            pb2.CreateIngestFlowRequest(
                title=title,
                is_published=is_published,
                commit_message=commit_message,
                **kwargs,
            ),
            pb2.CreateIngestFlowResponse,
        )

    def update_metadata(
        self,
        rid: str,
        *,
        title: str | None = None,
        description: str | None = None,
        is_archived: bool | None = None,
        is_published: bool | None = None,
    ) -> ingest_flow_pb2.UpdateIngestFlowMetadataResponse:
        pb2 = self._pb2
        kwargs = {}
        if title is not None:
            kwargs["title"] = title
        if description is not None:
            kwargs["description"] = description
        if is_archived is not None:
            kwargs["is_archived"] = is_archived
        if is_published is not None:
            kwargs["is_published"] = is_published
        return self._call(
            "UpdateIngestFlowMetadata",
            pb2.UpdateIngestFlowMetadataRequest(rid=rid, **kwargs),
            pb2.UpdateIngestFlowMetadataResponse,
        )

    def commit(
        self,
        rid: str,
        message: str,
        state: ingest_flow_pb2.IngestFlowState,
        *,
        branch: str | None = None,
        latest_commit_on_branch: str | None = None,
    ) -> ingest_flow_pb2.CommitResponse:
        pb2 = self._pb2
        kwargs = {}
        if branch is not None:
            kwargs["branch"] = branch
        if latest_commit_on_branch is not None:
            kwargs["latest_commit_on_branch"] = latest_commit_on_branch
        return self._call(
            "Commit",
            pb2.CommitRequest(rid=rid, message=message, state=state, **kwargs),
            pb2.CommitResponse,
        )

    def validate(self, state: ingest_flow_pb2.IngestFlowState) -> ingest_flow_pb2.ValidateIngestFlowResponse:
        pb2 = self._pb2
        return self._call(
            "ValidateIngestFlow",
            pb2.ValidateIngestFlowRequest(state=state),
            pb2.ValidateIngestFlowResponse,
        )

    def archive(self, *rids: str) -> ingest_flow_pb2.ArchiveIngestFlowsResponse:
        pb2 = self._pb2
        return self._call(
            "ArchiveIngestFlows",
            pb2.ArchiveIngestFlowsRequest(ingest_flow_rids=list(rids)),
            pb2.ArchiveIngestFlowsResponse,
        )

    def unarchive(self, *rids: str) -> ingest_flow_pb2.UnarchiveIngestFlowsResponse:
        pb2 = self._pb2
        return self._call(
            "UnarchiveIngestFlows",
            pb2.UnarchiveIngestFlowsRequest(ingest_flow_rids=list(rids)),
            pb2.UnarchiveIngestFlowsResponse,
        )

    def save_working_state(
        self,
        rid: str,
        state: ingest_flow_pb2.IngestFlowState,
        message: str = "",
        *,
        branch: str | None = None,
        latest_commit_on_branch: str | None = None,
    ) -> ingest_flow_pb2.SaveWorkingStateResponse:
        pb2 = self._pb2
        kwargs = {}
        if branch is not None:
            kwargs["branch"] = branch
        if latest_commit_on_branch is not None:
            kwargs["latest_commit_on_branch"] = latest_commit_on_branch
        return self._call(
            "SaveWorkingState",
            pb2.SaveWorkingStateRequest(rid=rid, state=state, message=message, **kwargs),
            pb2.SaveWorkingStateResponse,
        )

    def merge_to_main(
        self,
        rid: str,
        branch: str,
        message: str,
        *,
        latest_commit_on_main: str | None = None,
    ) -> ingest_flow_pb2.MergeToMainResponse:
        pb2 = self._pb2
        kwargs = {}
        if latest_commit_on_main is not None:
            kwargs["latest_commit_on_main"] = latest_commit_on_main
        return self._call(
            "MergeToMain",
            pb2.MergeToMainRequest(rid=rid, branch=branch, message=message, **kwargs),
            pb2.MergeToMainResponse,
        )
