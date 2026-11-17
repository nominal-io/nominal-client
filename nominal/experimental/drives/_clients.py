from __future__ import annotations

from typing import Protocol

from nominal_api import upload_api

from nominal.core._clientsbunch import HasScoutParams
from nominal.protos.file_store.v1 import drives_pb2_grpc, files_pb2_grpc


class _Clients(HasScoutParams, Protocol):
    """The services drives, files, and revisions use.

    One protocol shared across the subpackage rather than the usual per-class nested
    `_Clients`: a drive hands its clients to every file and revision it returns, and they
    hand them on to the files their changes produce, so one bunch flows through all of them.
    Files and revisions only call `drive_files`; a drive also uses `drives` and `upload`.
    """

    @property
    def drives(self) -> drives_pb2_grpc.DrivesServiceStub: ...
    @property
    def drive_files(self) -> files_pb2_grpc.FilesServiceStub: ...
    @property
    def upload(self) -> upload_api.UploadService: ...
