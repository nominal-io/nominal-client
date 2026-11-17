from __future__ import annotations

from collections.abc import Iterable

from nominal.core._utils.pagination_tools import DEFAULT_PAGE_SIZE, paginate_grpc
from nominal.protos.file_store.v1 import drives_pb2, drives_pb2_grpc, file_store_pb2, files_pb2, files_pb2_grpc


def list_drives_paginated(
    drives: drives_pb2_grpc.DrivesServiceStub,
    workspace_rid: str,
    include_archived: bool = False,
) -> Iterable[file_store_pb2.Drive]:
    def factory(page_token: str | None) -> drives_pb2.ListDrivesRequest:
        return drives_pb2.ListDrivesRequest(
            workspace_rid=workspace_rid,
            include_archived=include_archived,
            page_size=DEFAULT_PAGE_SIZE,
            page_token=page_token,
        )

    for response in paginate_grpc(drives.ListDrives, request_factory=factory):
        yield from response.drives


def list_files_paginated(
    drive_files: files_pb2_grpc.FilesServiceStub,
    drive_rid: str,
    parent_path: str,
    include_removed: bool = False,
    recursive: bool = False,
) -> Iterable[file_store_pb2.FileEntry]:
    def factory(page_token: str | None) -> files_pb2.ListFilesRequest:
        return files_pb2.ListFilesRequest(
            drive_rid=drive_rid,
            parent_path=file_store_pb2.LogicalPath(path=parent_path),
            include_removed=include_removed,
            recursive=recursive,
            page_size=DEFAULT_PAGE_SIZE,
            page_token=page_token,
        )

    for response in paginate_grpc(drive_files.ListFiles, request_factory=factory):
        yield from response.entries


def list_file_revisions_paginated(
    drive_files: files_pb2_grpc.FilesServiceStub,
    drive_rid: str,
    file_rid: str,
) -> Iterable[file_store_pb2.ManagedFileRevision]:
    def factory(page_token: str | None) -> files_pb2.ListFileRevisionsRequest:
        return files_pb2.ListFileRevisionsRequest(
            drive_rid=drive_rid,
            file_rid=file_rid,
            page_size=DEFAULT_PAGE_SIZE,
            page_token=page_token,
        )

    for response in paginate_grpc(drive_files.ListFileRevisions, request_factory=factory):
        yield from response.file_revisions
