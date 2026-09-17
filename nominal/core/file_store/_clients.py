from __future__ import annotations

from typing import Protocol

from nominal_api import upload_api

from nominal.core._clientsbunch import HasScoutParams
from nominal.protos.file_store.v1 import drives_pb2_grpc, file_store_pb2, files_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC


class _Clients(HasScoutParams, Protocol):
    """The File Store stubs every drive, file, and revision resource needs.

    One protocol shared across the subpackage rather than the usual per-class nested
    `_Clients`, because all four resource types talk to exactly these two stubs, plus the
    conjure upload service that `Drive.put_file` uploads through.
    """

    @property
    def drives(self) -> drives_pb2_grpc.DrivesServiceStub: ...
    @property
    def drive_files(self) -> files_pb2_grpc.FilesServiceStub: ...
    @property
    def upload(self) -> upload_api.UploadService: ...


def _attribution(msg: file_store_pb2.Attribution) -> tuple[IntegralNanosecondsUTC | None, str | None]:
    """Split an attribution into (time, user rid), tolerating either being unset."""
    return (
        msg.time.ToNanoseconds() if msg.HasField("time") else None,
        msg.user_rid or None,
    )


def _basename(path: str) -> str:
    """Last segment of a drive-relative path, e.g. the filename portion of `data/run.csv`."""
    return path.rsplit("/", 1)[-1]
