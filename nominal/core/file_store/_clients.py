from __future__ import annotations

from typing import Protocol

from nominal.core._clientsbunch import HasScoutParams
from nominal.protos.file_store.v1 import drives_pb2_grpc, files_pb2_grpc


class _Clients(HasScoutParams, Protocol):
    """The File Store stubs every drive, file, and revision resource needs.

    One protocol shared across the subpackage rather than the usual per-class nested
    `_Clients`, because all four resource types talk to exactly these two stubs.
    """

    @property
    def drives(self) -> drives_pb2_grpc.DrivesServiceStub: ...
    @property
    def drive_files(self) -> files_pb2_grpc.FilesServiceStub: ...
