from __future__ import annotations

from nominal.core.fs.drive import (
    Drive,
    VirtualDrive,
    _create_drive,
    _get_drive,
    _get_drive_by_id,
    _list_drives,
)
from nominal.core.fs.enums import DriveMutability, DriveSource, DriveState, VirtualDriveState
from nominal.protos.file_store.v1 import drives_pb2, file_store_pb2
from tests.core.fs._fixtures import drive_proto, mock_clients


def test_managed_source_builds_a_drive_and_virtual_sources_build_a_virtual_drive() -> None:
    """The class is chosen from the drive's source, so callers can act on the type."""
    clients = mock_clients()

    managed = Drive._from_proto(clients, drive_proto(source=file_store_pb2.DRIVE_SOURCE_NOMINAL))
    assert type(managed) is Drive

    for source in (
        file_store_pb2.DRIVE_SOURCE_S3,
        file_store_pb2.DRIVE_SOURCE_GOOGLE_DRIVE,
        file_store_pb2.DRIVE_SOURCE_GCS,
    ):
        assert isinstance(Drive._from_proto(clients, drive_proto(source=source)), VirtualDrive)


def test_drive_exposes_attribution_and_metadata() -> None:
    """Every drive field converts from the response, including attribution in nanoseconds."""
    drive = Drive._from_proto(mock_clients(), drive_proto())

    assert drive.rid == "ri.drive.1"
    assert drive.id == "telemetry"
    assert drive.workspace_rid == "ri.workspace.default"
    assert drive.state is DriveState.ACTIVE
    assert drive.source is DriveSource.NOMINAL
    assert drive.content_mutability is DriveMutability.WRITABLE
    assert drive.created_at == 1_700_000_000_000_000_000
    assert drive.created_by_rid == "ri.user.1"
    assert drive.description is None
    assert Drive._from_proto(mock_clients(), drive_proto(description="raw telemetry")).description == "raw telemetry"


def test_drive_without_attribution_parses() -> None:
    """Attribution may be absent; that must not crash or invent a timestamp."""
    drive = Drive._from_proto(mock_clients(), file_store_pb2.Drive(rid="ri.drive.1", id="d"))

    assert drive.created_at is None
    assert drive.created_by_rid is None


def test_archive_refreshes_the_same_instance_in_place() -> None:
    """Archiving updates the caller's instance from the response instead of returning a copy."""
    clients = mock_clients()
    drive = Drive._from_proto(clients, drive_proto())
    clients.drives.ArchiveDrive.return_value = drives_pb2.ArchiveDriveResponse(
        drive=drive_proto(state=file_store_pb2.DRIVE_STATE_ARCHIVED)
    )

    returned = drive.archive()

    assert returned is drive
    assert drive.state is DriveState.ARCHIVED
    assert clients.drives.ArchiveDrive.call_args.args[0].drive_rid == "ri.drive.1"


def test_update_sends_only_the_fields_given_and_refreshes_in_place() -> None:
    """Omitted fields stay unset on the request so the server leaves them unchanged."""
    clients = mock_clients()
    drive = Drive._from_proto(clients, drive_proto(description="raw telemetry"))
    clients.drives.UpdateDriveDetails.return_value = drives_pb2.UpdateDriveDetailsResponse(
        drive=drive_proto(id="renamed", description="raw telemetry")
    )

    returned = drive.update(id="renamed")

    assert returned is drive
    assert drive.id == "renamed"
    request = clients.drives.UpdateDriveDetails.call_args.args[0]
    assert request.drive_rid == "ri.drive.1"
    assert request.id == "renamed"
    assert not request.HasField("description")


def test_update_with_an_empty_description_clears_it() -> None:
    """An empty description is sent explicitly, which the server treats as clearing it."""
    clients = mock_clients()
    drive = Drive._from_proto(clients, drive_proto(description="raw telemetry"))
    clients.drives.UpdateDriveDetails.return_value = drives_pb2.UpdateDriveDetailsResponse(drive=drive_proto())

    drive.update(description="")

    request = clients.drives.UpdateDriveDetails.call_args.args[0]
    assert request.HasField("description") and request.description == ""
    assert not request.HasField("id")
    assert drive.description is None


def test_refresh_of_a_virtual_drive_stays_virtual() -> None:
    """Refreshing must not silently demote a VirtualDrive to the base class."""
    clients = mock_clients()
    drive = Drive._from_proto(clients, drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3))
    clients.drives.GetDrive.return_value = drives_pb2.GetDriveResponse(
        drive=drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3, id="updated")
    )

    drive.refresh()

    assert isinstance(drive, VirtualDrive)
    assert drive.id == "updated"


def test_virtual_drive_reports_provider_status() -> None:
    """A provider status probe converts its state, message, and last successful check."""
    clients = mock_clients()
    drive = Drive._from_proto(clients, drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3))
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


def test_a_provider_never_reached_has_no_last_successful_check() -> None:
    """An unset check time means the provider has never been reached, not that it was reached at the epoch."""
    clients = mock_clients()
    drive = Drive._from_proto(clients, drive_proto(source=file_store_pb2.DRIVE_SOURCE_S3))
    assert isinstance(drive, VirtualDrive)
    clients.drives.GetVirtualDriveStatus.return_value = drives_pb2.GetVirtualDriveStatusResponse(
        status=file_store_pb2.VirtualDriveStatus(state=file_store_pb2.VIRTUAL_DRIVE_STATE_UNREACHABLE)
    )

    assert drive.status().last_successful_check_at is None


def test_create_drive_uses_the_resolved_workspace() -> None:
    """Creating a drive without a workspace uses the client's resolved default, and omits an absent description."""
    clients = mock_clients()
    clients.drives.CreateDrive.return_value = drives_pb2.CreateDriveResponse(drive=drive_proto())

    drive = _create_drive(clients, "telemetry")

    assert drive.id == "telemetry"
    request = clients.drives.CreateDrive.call_args.args[0]
    assert request.id == "telemetry"
    assert request.workspace_rid == "ri.workspace.default"
    assert not request.HasField("description")


def test_get_drive_by_rid_does_not_resolve_a_workspace() -> None:
    """Get-by-RID is workspace-independent; resolving one would be a wasted request."""
    clients = mock_clients()
    clients.drives.GetDrive.return_value = drives_pb2.GetDriveResponse(drive=drive_proto())

    _get_drive(clients, "ri.drive.1")

    clients.resolve_workspace.assert_not_called()
    clients.resolve_default_workspace_rid.assert_not_called()


def test_get_drive_by_id_scopes_to_the_workspace() -> None:
    """An id is only unique within a workspace, so lookup by id sends the resolved workspace."""
    clients = mock_clients()
    clients.drives.GetDriveById.return_value = drives_pb2.GetDriveByIdResponse(drive=drive_proto())

    _get_drive_by_id(clients, "telemetry")

    request = clients.drives.GetDriveById.call_args.args[0]
    assert request.id == "telemetry"
    assert request.workspace_rid == "ri.workspace.default"


def test_list_drives_follows_pagination_cursors() -> None:
    """Listing drives collects every page by following the returned cursor."""
    clients = mock_clients()
    clients.drives.ListDrives.side_effect = [
        drives_pb2.ListDrivesResponse(drives=[drive_proto("ri.drive.1")], next_page_token="page-2"),
        drives_pb2.ListDrivesResponse(drives=[drive_proto("ri.drive.2")]),
    ]

    drives = _list_drives(clients)

    assert [d.rid for d in drives] == ["ri.drive.1", "ri.drive.2"]
    assert clients.drives.ListDrives.call_args_list[1].args[0].page_token == "page-2"
