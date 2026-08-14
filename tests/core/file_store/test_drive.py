from __future__ import annotations

from unittest.mock import MagicMock

from nominal.core.file_store.drive import (
    Drive,
    VirtualDrive,
    _create_drive,
    _get_drive,
    _get_drive_by_id,
    _list_drives,
)
from nominal.core.file_store.enums import DriveMutability, DriveSource, DriveState, VirtualDriveState
from nominal.protos.file_store.v1 import drives_pb2, file_store_pb2


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    clients.resolve_workspace.return_value.rid = "ri.workspace.default"
    return clients


def _drive_proto(
    rid: str = "ri.drive.1",
    *,
    id: str = "telemetry",
    source: file_store_pb2.DriveSource.ValueType = file_store_pb2.DRIVE_SOURCE_NOMINAL,
    state: file_store_pb2.DriveState.ValueType = file_store_pb2.DRIVE_STATE_ACTIVE,
    mutability: file_store_pb2.DriveMutability.ValueType = file_store_pb2.DRIVE_MUTABILITY_WRITABLE,
) -> file_store_pb2.Drive:
    drive = file_store_pb2.Drive(
        rid=rid,
        workspace_rid="ri.workspace.default",
        id=id,
        state=state,
        source=source,
        content_mutability=mutability,
    )
    drive.created.time.FromNanoseconds(1_700_000_000_000_000_000)
    drive.created.user_rid = "ri.user.1"
    return drive


def test_managed_source_builds_a_drive_and_virtual_sources_build_a_virtual_drive() -> None:
    """The class is chosen from the drive's source, so callers can act on the type."""
    clients = _clients()

    managed = Drive._from_proto(clients, _drive_proto(source=file_store_pb2.DRIVE_SOURCE_NOMINAL))
    assert type(managed) is Drive

    for source in (
        file_store_pb2.DRIVE_SOURCE_S3,
        file_store_pb2.DRIVE_SOURCE_GOOGLE_DRIVE,
        file_store_pb2.DRIVE_SOURCE_GCS,
    ):
        assert isinstance(Drive._from_proto(clients, _drive_proto(source=source)), VirtualDrive)


def test_drive_exposes_attribution_and_metadata() -> None:
    drive = Drive._from_proto(_clients(), _drive_proto())

    assert drive.rid == "ri.drive.1"
    assert drive.id == "telemetry"
    assert drive.workspace_rid == "ri.workspace.default"
    assert drive.state is DriveState.ACTIVE
    assert drive.source is DriveSource.NOMINAL
    assert drive.content_mutability is DriveMutability.WRITABLE
    assert drive.created_at == 1_700_000_000_000_000_000
    assert drive.created_by_rid == "ri.user.1"


def test_drive_without_attribution_parses() -> None:
    """Attribution may be absent; that must not crash or invent a timestamp."""
    drive = Drive._from_proto(_clients(), file_store_pb2.Drive(rid="ri.drive.1", id="d"))

    assert drive.created_at is None
    assert drive.created_by_rid is None


def test_archive_refreshes_the_same_instance_in_place() -> None:
    clients = _clients()
    drive = Drive._from_proto(clients, _drive_proto())
    clients.drives.ArchiveDrive.return_value = drives_pb2.ArchiveDriveResponse(
        drive=_drive_proto(state=file_store_pb2.DRIVE_STATE_ARCHIVED)
    )

    returned = drive.archive()

    assert returned is drive
    assert drive.state is DriveState.ARCHIVED
    assert clients.drives.ArchiveDrive.call_args.args[0].drive_rid == "ri.drive.1"


def test_rename_sends_the_new_id_and_refreshes() -> None:
    clients = _clients()
    drive = Drive._from_proto(clients, _drive_proto())
    clients.drives.UpdateDriveDetails.return_value = drives_pb2.UpdateDriveDetailsResponse(
        drive=_drive_proto(id="renamed")
    )

    drive.rename("renamed")

    assert drive.id == "renamed"
    request = clients.drives.UpdateDriveDetails.call_args.args[0]
    assert request.drive_rid == "ri.drive.1"
    assert request.id == "renamed"


def test_refresh_of_a_virtual_drive_stays_virtual() -> None:
    """Refreshing must not silently demote a VirtualDrive to the base class."""
    clients = _clients()
    drive = Drive._from_proto(clients, _drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3))
    clients.drives.GetDrive.return_value = drives_pb2.GetDriveResponse(
        drive=_drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3, id="updated")
    )

    drive.refresh()

    assert isinstance(drive, VirtualDrive)
    assert drive.id == "updated"


def test_virtual_drive_reports_provider_status() -> None:
    clients = _clients()
    drive = Drive._from_proto(clients, _drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3))
    assert isinstance(drive, VirtualDrive)
    status_proto = file_store_pb2.VirtualDriveStatus(
        state=file_store_pb2.VIRTUAL_DRIVE_STATE_AUTH_ERROR, message="credentials rejected"
    )
    status_proto.last_successful_check_time.FromNanoseconds(1_700_000_000_000_000_000)
    clients.drives.GetVirtualDriveStatus.return_value = drives_pb2.GetVirtualDriveStatusResponse(status=status_proto)

    status = drive.status()

    assert status.state is VirtualDriveState.AUTH_ERROR
    assert status.message == "credentials rejected"
    assert status.last_successful_check_at == 1_700_000_000_000_000_000


def test_create_drive_uses_the_resolved_workspace() -> None:
    clients = _clients()
    clients.drives.CreateDrive.return_value = drives_pb2.CreateDriveResponse(drive=_drive_proto())

    drive = _create_drive(clients, "telemetry")

    assert drive.id == "telemetry"
    request = clients.drives.CreateDrive.call_args.args[0]
    assert request.id == "telemetry"
    assert request.workspace_rid == "ri.workspace.default"


def test_get_drive_by_rid_does_not_resolve_a_workspace() -> None:
    """Get-by-RID is workspace-independent; resolving one would be a wasted request."""
    clients = _clients()
    clients.drives.GetDrive.return_value = drives_pb2.GetDriveResponse(drive=_drive_proto())

    _get_drive(clients, "ri.drive.1")

    clients.resolve_workspace.assert_not_called()
    clients.resolve_default_workspace_rid.assert_not_called()


def test_get_drive_by_id_scopes_to_the_workspace() -> None:
    clients = _clients()
    clients.drives.GetDriveById.return_value = drives_pb2.GetDriveByIdResponse(drive=_drive_proto())

    _get_drive_by_id(clients, "telemetry")

    request = clients.drives.GetDriveById.call_args.args[0]
    assert request.id == "telemetry"
    assert request.workspace_rid == "ri.workspace.default"


def test_list_drives_follows_pagination_cursors() -> None:
    clients = _clients()
    clients.drives.ListDrives.side_effect = [
        drives_pb2.ListDrivesResponse(drives=[_drive_proto("ri.drive.1")], next_page_token="page-2"),
        drives_pb2.ListDrivesResponse(drives=[_drive_proto("ri.drive.2")]),
    ]

    drives = _list_drives(clients)

    assert [d.rid for d in drives] == ["ri.drive.1", "ri.drive.2"]
    assert clients.drives.ListDrives.call_args_list[1].args[0].page_token == "page-2"


def test_list_drives_passes_include_archived() -> None:
    clients = _clients()
    clients.drives.ListDrives.side_effect = [drives_pb2.ListDrivesResponse(drives=[])]

    _list_drives(clients, include_archived=True)

    assert clients.drives.ListDrives.call_args.args[0].include_archived is True
