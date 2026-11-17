from __future__ import annotations

import itertools
import zipfile
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest
import requests

from nominal.core import ArchiveStatusFilter
from nominal.core.client import NominalClient
from nominal.exceptions import (
    NominalAlreadyExistsError,
    NominalConflictError,
    NominalError,
    NominalNotFoundError,
    NominalPermissionDeniedError,
)
from nominal.experimental import analyst
from nominal.experimental.analyst import Skill
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2
from nominal.protos.skills.v1 import skills_pb2

_RID = "ri.file-store.main.skill.0b8f6c1e-0d3c-4c43-9a55-6f0d3c1e2b7a"
_WORKSPACE = "ri.workspace.main.workspace.1"
_DRIVE = "ri.file-store.main.drive.1"
_FOLDER = "skills/post-test-review"
_SKILL_MD = "---\nname: post-test-review\ndescription: Review a test run.\nlicense: MIT\n---\n\nRead the run.\n"
_RENAMED_MD = "---\nname: flight-review\ndescription: Review a flight test.\n---\n\nRead the flight.\n"
_UNDESCRIBED_MD = "---\nname: post-test-review\n---\n\nRead the run.\n"


def _identity(name: str) -> file_store_pb2.LogicalFileIdentity:
    return file_store_pb2.LogicalFileIdentity(
        managed=file_store_pb2.ManagedFileIdentity(file_rid=f"ri.file-store.main.file.{name}")
    )


def _ref(revision: str) -> file_store_pb2.FileRevisionRef:
    return file_store_pb2.FileRevisionRef(managed=file_store_pb2.ManagedFileRevisionRef(file_revision_rid=revision))


def _file(path: str, revision: str, size_bytes: int = 1, identity: str | None = None) -> file_store_pb2.LogicalFile:
    return file_store_pb2.LogicalFile(
        identity=_identity(revision if identity is None else identity),
        path=file_store_pb2.LogicalPath(path=path),
        size_bytes=size_bytes,
        current_revision=_ref(revision),
    )


def _entry(revision: str, **kwargs) -> file_store_pb2.LogicalFile:
    """The skill's SKILL.md: the file its record points at."""
    entry = _file(f"{_FOLDER}/SKILL.md", revision, identity="entry")
    entry.state = file_store_pb2.FILE_STATE_ACTIVE
    entry.MergeFrom(file_store_pb2.LogicalFile(**kwargs))
    return entry


def _saved(path: str, revision: str) -> files_pb2.FileChangeResult:
    return files_pb2.FileChangeResult(
        success=files_pb2.FileChangeSuccess(
            file=_file(path, revision), file_revision=file_store_pb2.ManagedFileRevision(ref=_ref(revision))
        )
    )


def _refused(error: files_pb2.FileChangeError.ValueType) -> files_pb2.FileChangeResult:
    return files_pb2.FileChangeResult(failure=files_pb2.FileChangeFailure(change=error, message="refused"))


_BATCH_FAILURE = _refused(files_pb2.FILE_CHANGE_ERROR_BATCH_FAILURE)


def _listing(*files: file_store_pb2.LogicalFile, next_page_token: str | None = None) -> files_pb2.ListFilesResponse:
    return files_pb2.ListFilesResponse(
        entries=[file_store_pb2.FileEntry(file=file) for file in files], next_page_token=next_page_token
    )


def _list_folders(
    listings: dict[str, files_pb2.ListFilesResponse],
) -> Callable[[files_pb2.ListFilesRequest], files_pb2.ListFilesResponse]:
    """Answers ListFiles with each folder's listing, and an empty one for any other folder."""
    return lambda request: listings.get(request.parent_path.path, _listing())


def _proto_skill(**kwargs) -> skills_pb2.Skill:
    defaults = dict(
        skill_rid=_RID,
        workspace_rid=_WORKSPACE,
        state=skills_pb2.SKILL_STATE_ENABLED,
        version=7,
        last_known=skills_pb2.SkillSnapshot(name="post-test-review", description="Review a test run."),
        backing=skills_pb2.SkillBacking(
            file_store=skills_pb2.FileStoreSkillBacking(
                entry_file_identity=_identity("entry"),
                drive_rid=_DRIVE,
                entry_path=file_store_pb2.LogicalPath(path=f"{_FOLDER}/SKILL.md"),
            )
        ),
        created=file_store_pb2.Attribution(),
        updated=file_store_pb2.Attribution(),
    )
    return skills_pb2.Skill(**{**defaults, **kwargs})


@pytest.fixture
def uploads(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """Stands in for the object-storage upload, recording each upload's bytes by the key it returns."""
    uploaded: dict[str, bytes] = {}
    # Uploads run on several threads, so each key comes from one shared counter.
    counter = itertools.count()

    def upload(auth_header, workspace_rid, data, filename, mimetype, upload_client, **kwargs):
        key = f"key-{next(counter)}-{filename}"
        uploaded[key] = data.read()
        return MagicMock(key=key)

    monkeypatch.setattr("nominal.experimental.analyst._skill._put_multipart_upload_to", upload)
    return uploaded


@pytest.fixture
def downloads(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """Stands in for downloading from a signed URL, serving each URL's bytes from this mapping."""
    contents: dict[str, bytes] = {}
    session = MagicMock()
    session.get.side_effect = lambda url, timeout: MagicMock(content=contents[url])
    monkeypatch.setattr("nominal.experimental.analyst._skill.create_multipart_request_session", lambda **_: session)
    return contents


def _skill_clients() -> MagicMock:
    clients = MagicMock()
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(skill=_proto_skill())
    clients.drive_files.GetFileByIdentity.return_value = files_pb2.GetFileByIdentityResponse(file=_entry("rev-1"))
    clients.drive_files.GetDownloadUrl.side_effect = lambda request: files_pb2.GetDownloadUrlResponse(
        url=f"https://storage/{request.source_ref.managed.file_revision_rid}"
    )
    return clients


def _client() -> tuple[NominalClient, MagicMock]:
    clients = _skill_clients()
    clients.resolve_default_workspace_rid.return_value = _WORKSPACE
    clients.skills.UpdateWorkspaceSkillsDrive.return_value = skills_pb2.UpdateWorkspaceSkillsDriveResponse(
        skills_drive=skills_pb2.WorkspaceSkillsDrive(drive=file_store_pb2.Drive(rid=_DRIVE))
    )
    clients.drive_files.ListFiles.return_value = _listing()
    clients.drive_files.ApplyFileChanges.side_effect = lambda request: files_pb2.ApplyFileChangesResponse(
        results=[_saved(change.put.destination.path.path.path, f"rev-{i}") for i, change in enumerate(request.changes)]
    )
    clients.skills.CreateSkill.return_value = skills_pb2.CreateSkillResponse(
        skill=_proto_skill(state=skills_pb2.SKILL_STATE_DRAFT, version=1)
    )
    clients.skills.UpdateSkill.return_value = skills_pb2.UpdateSkillResponse(skill=_proto_skill(version=2))
    return NominalClient(_clients=clients), clients


def _puts(clients: MagicMock, call: int = 0) -> list[files_pb2.PutFile]:
    request = clients.drive_files.ApplyFileChanges.call_args_list[call].args[0]
    return [change.put for change in request.changes]


def test_skill_without_valid_frontmatter_converts_absent_fields() -> None:
    """A skill whose SKILL.md had no valid frontmatter has no name or description."""
    skill = Skill._from_proto(MagicMock(), skills_pb2.Skill(skill_rid=_RID, state=skills_pb2.SKILL_STATE_DRAFT))

    assert skill.name is None
    assert skill.description is None
    assert not skill.is_enabled and not skill.is_archived


@pytest.mark.parametrize(("enabled", "states"), [(True, [skills_pb2.SKILL_STATE_ENABLED]), (False, [])])
def test_create_saves_the_folder_then_registers_skill_md(uploads, enabled, states) -> None:
    """Create saves each file new under skills/<name>, registers the saved SKILL.md, and turns it on if asked."""
    client, clients = _client()

    skill = analyst.create_skill(
        client, _SKILL_MD, files={"methods/limits.md": "Use the plan's limits.\n"}, enabled=enabled
    )

    puts = _puts(clients)
    assert [put.destination.path.path.path for put in puts] == [f"{_FOLDER}/SKILL.md", f"{_FOLDER}/methods/limits.md"]
    assert [uploads[put.object.object_key] for put in puts] == [_SKILL_MD.encode(), b"Use the plan's limits.\n"]
    assert clients.drive_files.ApplyFileChanges.call_args.args[0].failure_policy == files_pb2.FAILURE_POLICY_ABORT
    registered = clients.skills.CreateSkill.call_args.args[0].skill
    assert registered.workspace_rid == _WORKSPACE
    assert registered.backing.file_store.entry_file_identity == _identity("rev-0")
    updates = [call.args[0] for call in clients.skills.UpdateSkill.call_args_list]
    assert [update.skill.state for update in updates] == states
    assert all(update.expected_version == 1 for update in updates)
    assert skill.version == (2 if enabled else 1)


def test_create_refuses_a_taken_name_before_uploading(uploads) -> None:
    """A folder that already holds files belongs to a skill, so create saves nothing."""
    client, clients = _client()
    clients.drive_files.ListFiles.return_value = _listing(_file(f"{_FOLDER}/notes.md", "rev-x"))

    with pytest.raises(NominalAlreadyExistsError):
        analyst.create_skill(client, _SKILL_MD)

    assert uploads == {}
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_create_refuses_a_name_another_save_takes_first(uploads) -> None:
    """Another create can put files in the folder after it was found empty; File Store then refuses the batch, and the
    name is reported as taken.
    """
    client, clients = _client()
    clients.drive_files.ApplyFileChanges.side_effect = None
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_refused(files_pb2.FILE_CHANGE_ERROR_PATH_ALREADY_EXISTS)]
    )

    with pytest.raises(NominalAlreadyExistsError, match="already exists"):
        analyst.create_skill(client, _SKILL_MD)

    clients.skills.CreateSkill.assert_not_called()


def test_create_saves_nothing_when_a_file_is_refused(uploads) -> None:
    """File Store saves a skill's files all or none, so a refused file leaves nothing to remove or register."""
    client, clients = _client()
    clients.drive_files.ApplyFileChanges.side_effect = None
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_BATCH_FAILURE, _refused(files_pb2.FILE_CHANGE_ERROR_INVALID_LOGICAL_PATH)]
    )

    with pytest.raises(NominalError, match=r"methods/limits\.md.*Nothing was changed"):
        analyst.create_skill(client, _SKILL_MD, files={"methods/limits.md": "text"})

    assert clients.drive_files.ApplyFileChanges.call_count == 1
    clients.skills.CreateSkill.assert_not_called()


def test_create_removes_the_revisions_it_saved_when_the_skill_cant_be_registered(uploads, fake_rpc_error) -> None:
    """When the skills service refuses the new skill, create removes the revisions it saved, all or none, so a file
    another save has changed since is left alone.
    """
    client, clients = _client()
    clients.skills.CreateSkill.side_effect = fake_rpc_error(grpc.StatusCode.INVALID_ARGUMENT)

    with pytest.raises(NominalError) as raised:
        analyst.create_skill(client, _SKILL_MD, files={"methods/limits.md": "text"})

    removal = clients.drive_files.ApplyFileChanges.call_args_list[1].args[0]
    assert [change.remove.revision_rid for change in removal.changes] == ["rev-0", "rev-1"]
    assert (removal.drive_rid, removal.failure_policy) == (_DRIVE, files_pb2.FAILURE_POLICY_ABORT)
    # The revisions come from the save, not from listing the folder again.
    assert clients.drive_files.ListFiles.call_count == 1
    assert not getattr(raised.value, "__notes__", [])


@pytest.mark.parametrize("code", [grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE])
def test_create_keeps_its_files_when_its_unknown_whether_the_skill_was_registered(
    uploads, fake_rpc_error, code
) -> None:
    """After a timeout or outage the skill may be registered, so create keeps its files and says so."""
    client, clients = _client()
    clients.skills.CreateSkill.side_effect = fake_rpc_error(code)

    with pytest.raises(NominalError) as raised:
        analyst.create_skill(client, _SKILL_MD)

    assert clients.drive_files.ApplyFileChanges.call_count == 1
    (note,) = raised.value.__notes__
    assert "unknown whether the skill was created" in note
    assert f"still in {_FOLDER}" in note


def test_create_says_where_its_files_are_when_removing_them_is_refused(uploads, fake_rpc_error) -> None:
    """File Store removes the files all or none, so a refused removal leaves every file, and the error says where."""
    client, clients = _client()
    clients.drive_files.ApplyFileChanges.side_effect = [
        files_pb2.ApplyFileChangesResponse(results=[_saved(f"{_FOLDER}/SKILL.md", "rev-0")]),
        files_pb2.ApplyFileChangesResponse(
            results=[_refused(files_pb2.FILE_CHANGE_ERROR_REVISION_PRECONDITION_FAILED)]
        ),
    ]
    clients.skills.CreateSkill.side_effect = fake_rpc_error(grpc.StatusCode.INVALID_ARGUMENT)

    with pytest.raises(NominalError, match="INVALID_ARGUMENT") as raised:
        analyst.create_skill(client, _SKILL_MD)

    (note,) = raised.value.__notes__
    assert f"still in {_FOLDER} in the skills drive of workspace {_WORKSPACE}" in note
    assert "Could not remove SKILL.md" in note


def test_writes_without_the_edit_permission_save_nothing(uploads, fake_rpc_error) -> None:
    """The skills service reports a user who can't edit the workspace's configuration as a missing workspace."""
    client, clients = _client()
    clients.skills.UpdateWorkspaceSkillsDrive.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)
    clients.drive_files.ListFiles.return_value = _listing(_entry("rev-1"))
    skill = Skill._from_proto(clients, _proto_skill())

    with pytest.raises(NominalPermissionDeniedError):
        analyst.create_skill(client, _SKILL_MD)
    with pytest.raises(NominalPermissionDeniedError):
        skill.write_files({"methods/a.md": "text"})
    with pytest.raises(NominalPermissionDeniedError):
        skill.update(content=_SKILL_MD)
    with pytest.raises(NominalPermissionDeniedError):
        skill.update(enabled=False)
    with pytest.raises(NominalPermissionDeniedError):
        skill.archive()

    assert uploads == {}
    clients.drive_files.ApplyFileChanges.assert_not_called()
    clients.skills.UpdateSkill.assert_not_called()


@pytest.mark.parametrize(
    "content",
    [
        "Read the run.\n",
        "---\nname: Post Test Review\ndescription: Review a test run.\n---\n",
        "---\nname: post--review\ndescription: Review a test run.\n---\n",
        "---\ndescription: [unclosed\n---\n",
    ],
)
def test_create_needs_frontmatter_with_a_valid_name(uploads, content) -> None:
    """The frontmatter name picks the skill's folder, so SKILL.md without a valid one is refused before any call."""
    client, clients = _client()

    with pytest.raises(ValueError):
        analyst.create_skill(client, content)

    clients.skills.UpdateWorkspaceSkillsDrive.assert_not_called()
    assert uploads == {}


@pytest.mark.parametrize(
    ("state", "write"),
    [
        (skills_pb2.SKILL_STATE_ENABLED, lambda client, skill, folder: analyst.create_skill(client, _UNDESCRIBED_MD)),
        (skills_pb2.SKILL_STATE_ENABLED, lambda client, skill, folder: analyst.import_skill(client, folder)),
        (skills_pb2.SKILL_STATE_ENABLED, lambda client, skill, folder: skill.update(content=_UNDESCRIBED_MD)),
        (
            skills_pb2.SKILL_STATE_DISABLED,
            lambda client, skill, folder: skill.update(content=_UNDESCRIBED_MD, enabled=True),
        ),
    ],
    ids=["create", "import", "update an enabled skill", "update turning it on"],
)
def test_turning_on_needs_a_description_before_saving(uploads, tmp_path: Path, state, write) -> None:
    """Analyst can't load a skill without a description, so a write that leaves the skill on refuses SKILL.md
    without one before saving anything.
    """
    client, clients = _client()
    skill = Skill._from_proto(clients, _proto_skill(state=state))
    (tmp_path / "SKILL.md").write_text(_UNDESCRIBED_MD)

    with pytest.raises(ValueError, match="description"):
        write(client, skill, tmp_path)

    clients.skills.UpdateWorkspaceSkillsDrive.assert_not_called()
    assert uploads == {}


@pytest.mark.parametrize("description", ["'   '", "[a, b]", "x" * 1025])
def test_a_description_is_text_of_1_to_1024_characters(uploads, description) -> None:
    """A blank, non-text, or overlong description can't turn a skill on."""
    client, clients = _client()

    with pytest.raises(ValueError, match="description"):
        analyst.create_skill(client, f"---\nname: post-test-review\ndescription: {description}\n---\n")

    assert uploads == {}


@pytest.mark.parametrize(
    ("listing", "write"),
    [
        (_listing(), lambda client, skill: analyst.create_skill(client, _UNDESCRIBED_MD, enabled=False)),
        (
            _listing(_entry("rev-1")),
            lambda client, skill: skill.update(content=_UNDESCRIBED_MD, enabled=False),
        ),
    ],
    ids=["create a draft", "update turning it off"],
)
def test_a_skill_left_off_saves_without_a_description(uploads, listing, write) -> None:
    """A skill that isn't turned on can have SKILL.md without a description."""
    client, clients = _client()
    clients.drive_files.ListFiles.return_value = listing
    skill = Skill._from_proto(clients, _proto_skill())

    write(client, skill)

    (put,) = _puts(clients)
    assert uploads[put.object.object_key] == _UNDESCRIBED_MD.encode()


def test_a_failed_upload_saves_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Files upload at once, and one that fails stops the save before File Store applies any."""
    client, clients = _client()

    def upload(auth_header, workspace_rid, data, filename, mimetype, upload_client, **kwargs):
        if filename == "b.md":
            raise requests.ConnectionError("upload failed")
        return MagicMock(key=f"key-{filename}")

    monkeypatch.setattr("nominal.experimental.analyst._skill._put_multipart_upload_to", upload)

    with pytest.raises(requests.ConnectionError):
        analyst.create_skill(client, _SKILL_MD, files={"methods/a.md": "A", "methods/b.md": "B"})

    clients.drive_files.ApplyFileChanges.assert_not_called()


@pytest.mark.parametrize("path", ["SKILL.md", "../outside.md", "/methods/a.md", "methods//a.md", "methods\\a.md"])
def test_reference_paths_stay_inside_the_folder(uploads, path) -> None:
    """A reference file can't be SKILL.md or leave the skill's folder; nothing is saved."""
    client, clients = _client()
    skill = Skill._from_proto(clients, _proto_skill())

    with pytest.raises(ValueError):
        analyst.create_skill(client, _SKILL_MD, files={path: "text"})
    with pytest.raises(ValueError):
        skill.write_files({path: "text"})

    assert uploads == {}
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_content_update_replaces_skill_md_at_its_current_revision(uploads) -> None:
    """New SKILL.md replaces the revision current now, then the skill records it against this object's version,
    checking the edit permission once.
    """
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(_entry("rev-1"))
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_saved(f"{_FOLDER}/SKILL.md", "rev-2")]
    )
    clients.skills.UpdateSkill.return_value = skills_pb2.UpdateSkillResponse(skill=_proto_skill(version=8))

    skill.update(content=_SKILL_MD, enabled=False)

    (put,) = _puts(clients)
    assert put.destination.file_revision_rid == "rev-1"
    assert uploads[put.object.object_key] == _SKILL_MD.encode()
    update = clients.skills.UpdateSkill.call_args.args[0]
    assert list(update.update_mask.paths) == ["backing", "state"]
    assert update.skill.backing.file_store.entry_file_identity == _identity("entry")
    assert update.skill.state == skills_pb2.SKILL_STATE_DISABLED
    assert update.expected_version == 7
    assert skill.version == 8
    assert clients.skills.UpdateWorkspaceSkillsDrive.call_count == 1


@pytest.mark.parametrize(
    "entry",
    [
        _entry("rev-1", state=file_store_pb2.FILE_STATE_REMOVED),
        _entry("rev-1", path=file_store_pb2.LogicalPath(path="notes/SKILL.md")),
    ],
    ids=["deleted, or deleted and re-created", "moved out of the folder"],
)
def test_content_update_refuses_a_skill_md_the_skill_does_not_point_at(uploads, entry) -> None:
    """The skill's record points at its SKILL.md, so a SKILL.md saved where it was deleted, or over one re-created in
    Drives, would go unrecorded; the update raises before uploading anything.
    """
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.GetFileByIdentity.return_value = files_pb2.GetFileByIdentityResponse(file=entry)

    with pytest.raises(NominalNotFoundError, match="no longer in"):
        skill.update(content=_SKILL_MD)

    assert uploads == {}
    clients.drive_files.ApplyFileChanges.assert_not_called()
    clients.skills.UpdateSkill.assert_not_called()


def test_content_update_reads_skill_md_before_checking_the_skills_version(uploads) -> None:
    """SKILL.md's revision is read before the skill's version is checked, so another update that saves SKILL.md and
    records it in between fails the check, and one that saves after the read makes File Store refuse the stale revision.
    """
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    reads: list[str] = []
    clients.drive_files.GetFileByIdentity.side_effect = lambda request: (
        reads.append("SKILL.md") or files_pb2.GetFileByIdentityResponse(file=_entry("rev-1"))
    )
    versions = iter([7, 8])
    clients.skills.GetSkill.side_effect = lambda request: (
        reads.append("skill") or skills_pb2.GetSkillResponse(skill=_proto_skill(version=next(versions)))
    )

    with pytest.raises(NominalConflictError):
        skill.update(content=_SKILL_MD)

    assert reads == ["skill", "SKILL.md", "skill"]
    assert uploads == {}
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_content_update_says_skill_md_is_saved_when_recording_it_fails(uploads, fake_rpc_error) -> None:
    """A conflict after SKILL.md saved isn't the conflict that saves nothing, so it isn't raised as one."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_saved(f"{_FOLDER}/SKILL.md", "rev-2")]
    )
    clients.skills.UpdateSkill.side_effect = fake_rpc_error(grpc.StatusCode.ABORTED)

    with pytest.raises(NominalError, match="SKILL.md is saved") as raised:
        skill.update(content=_SKILL_MD)

    assert type(raised.value) is NominalError


def test_turning_off_an_archived_skill_restores_it_turned_off() -> None:
    """update(enabled=False) sends the state unarchive() sends, so an archived skill comes back turned off."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill(state=skills_pb2.SKILL_STATE_ARCHIVED))
    clients.skills.UpdateSkill.return_value = skills_pb2.UpdateSkillResponse(
        skill=_proto_skill(state=skills_pb2.SKILL_STATE_DISABLED, version=8)
    )

    skill.update(enabled=False)

    assert clients.skills.UpdateSkill.call_args.args[0].skill.state == skills_pb2.SKILL_STATE_DISABLED
    assert not skill.is_archived


def test_content_update_from_a_stale_object_saves_nothing(uploads) -> None:
    """A skill that changed since this object was fetched raises a conflict before SKILL.md is uploaded."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(skill=_proto_skill(version=8))

    with pytest.raises(NominalConflictError):
        skill.update(content=_SKILL_MD)

    assert uploads == {}
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_rename_moves_the_folder_and_keeps_pointing_the_skill_at_skill_md(uploads) -> None:
    """A new frontmatter name saves SKILL.md and moves the folder to skills/<new name> in one batch. SKILL.md keeps
    its identity through both, so the skill records the identity it has without looking SKILL.md up after the move.
    """
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.side_effect = _list_folders({_FOLDER: _listing(_entry("rev-1"))})
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[
            _saved(f"{_FOLDER}/SKILL.md", "rev-2"),
            files_pb2.FileChangeResult(directory_moved=files_pb2.MoveDirectorySuccess()),
        ]
    )

    skill.update(content=_RENAMED_MD)

    request = clients.drive_files.ApplyFileChanges.call_args.args[0]
    assert request.failure_policy == files_pb2.FAILURE_POLICY_ABORT
    put, move = request.changes[0].put, request.changes[1].move_directory
    assert put.destination.file_revision_rid == "rev-1"
    assert (getattr(move, "from").path, move.to.path) == (_FOLDER, "skills/flight-review")
    clients.drive_files.GetFile.assert_not_called()
    assert clients.drive_files.GetFileByIdentity.call_count == 1
    update = clients.skills.UpdateSkill.call_args.args[0]
    assert update.skill.backing.file_store.entry_file_identity == _identity("entry")


@pytest.mark.parametrize(
    ("error", "raised"),
    [
        (files_pb2.FILE_CHANGE_ERROR_DIRECTORY_MOVE_LIMIT_EXCEEDED, NominalError),
        (files_pb2.FILE_CHANGE_ERROR_PATH_ALREADY_EXISTS, NominalAlreadyExistsError),
    ],
)
def test_refused_move_saves_nothing(uploads, error, raised) -> None:
    """When the folder can't move, the batch saves nothing and the skill isn't updated; a name taken meanwhile is
    reported as taken.
    """
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.side_effect = _list_folders({_FOLDER: _listing(_entry("rev-1"))})
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_BATCH_FAILURE, _refused(error)]
    )

    with pytest.raises(raised):
        skill.update(content=_RENAMED_MD)

    assert clients.drive_files.ApplyFileChanges.call_count == 1
    clients.skills.UpdateSkill.assert_not_called()


def test_turning_on_an_archived_skill_saves_nothing(uploads) -> None:
    """An archived skill must be restored before it is turned on, so SKILL.md isn't saved only to be left unrecorded."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill(state=skills_pb2.SKILL_STATE_ARCHIVED))

    with pytest.raises(NominalError, match="unarchive"):
        skill.update(content=_SKILL_MD, enabled=True)

    assert uploads == {}
    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_rename_to_a_taken_name_saves_nothing(uploads) -> None:
    """Renaming to a name whose folder holds files raises before SKILL.md is uploaded."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.side_effect = _list_folders(
        {
            _FOLDER: _listing(_entry("rev-1")),
            "skills/flight-review": _listing(_file("skills/flight-review/SKILL.md", "rev-x")),
        }
    )

    with pytest.raises(NominalAlreadyExistsError):
        skill.update(content=_RENAMED_MD)

    assert uploads == {}


def test_state_update_checks_the_version() -> None:
    """Turning a skill off is saved against this object's version, and a stale one raises a conflict."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.UpdateSkill.return_value = skills_pb2.UpdateSkillResponse(skill=_proto_skill(version=8))

    skill.update(enabled=False)

    request = clients.skills.UpdateSkill.call_args.args[0]
    assert (list(request.update_mask.paths), request.skill.state) == (["state"], skills_pb2.SKILL_STATE_DISABLED)
    assert request.expected_version == 7
    assert skill.version == 8


def test_stale_record_update_raises_conflict(fake_rpc_error) -> None:
    """The skills service's stale-version error surfaces as NominalConflictError."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.UpdateSkill.side_effect = fake_rpc_error(grpc.StatusCode.ABORTED)

    with pytest.raises(NominalConflictError):
        skill.archive()


def test_write_files_replaces_existing_files_and_creates_new_ones(uploads) -> None:
    """A file already in the folder is replaced at its current revision; a new one is created at its path."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(
        _file(f"{_FOLDER}/SKILL.md", "rev-entry"), _file(f"{_FOLDER}/methods/a.md", "rev-a")
    )
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_saved(f"{_FOLDER}/methods/a.md", "rev-a2"), _saved(f"{_FOLDER}/methods/b.md", "rev-b")]
    )

    skill.write_files({"methods/a.md": "Method A.\n", "methods/b.md": "Method B.\n"})

    replaced, created = _puts(clients)
    assert replaced.destination.file_revision_rid == "rev-a"
    assert created.destination.path.path.path == f"{_FOLDER}/methods/b.md"
    assert [uploads[put.object.object_key] for put in (replaced, created)] == [b"Method A.\n", b"Method B.\n"]


def test_write_files_reports_a_file_changed_meanwhile(uploads) -> None:
    """A file another save changed raises a conflict, and none of the files are saved."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(_file(f"{_FOLDER}/methods/a.md", "rev-a"))
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_refused(files_pb2.FILE_CHANGE_ERROR_REVISION_PRECONDITION_FAILED), _BATCH_FAILURE]
    )

    with pytest.raises(NominalConflictError, match=r"methods/a\.md.*Nothing was changed"):
        skill.write_files({"methods/a.md": "Method A.\n", "methods/b.md": "Method B.\n"})


def test_remove_files_refuses_a_missing_path_before_removing() -> None:
    """Removing a path the folder doesn't have raises before any file is removed."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(_file(f"{_FOLDER}/methods/a.md", "rev-a"))

    with pytest.raises(NominalNotFoundError, match="methods/b.md"):
        skill.remove_files(["methods/a.md", "methods/b.md"])

    clients.drive_files.ApplyFileChanges.assert_not_called()


def test_remove_files_removes_each_file_at_its_current_revision() -> None:
    """Each file is removed by its current revision, so a file changed since is left alone."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(_file(f"{_FOLDER}/methods/a.md", "rev-a"))
    clients.drive_files.ApplyFileChanges.return_value = files_pb2.ApplyFileChangesResponse(
        results=[_saved(f"{_FOLDER}/methods/a.md", "rev-a2")]
    )

    skill.remove_files(["methods/a.md"])

    request = clients.drive_files.ApplyFileChanges.call_args.args[0]
    assert (request.drive_rid, [change.remove.revision_rid for change in request.changes]) == (_DRIVE, ["rev-a"])


def test_list_files_follows_pages_and_gives_paths_inside_the_folder() -> None:
    """Listing reads every page of the folder and gives each path relative to it, in path order."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.side_effect = [
        _listing(_file(f"{_FOLDER}/methods/a.md", "r2", 9), next_page_token="next"),
        _listing(_file(f"{_FOLDER}/SKILL.md", "r1", 120)),
    ]

    files = skill.list_files()

    first, second = (call.args[0] for call in clients.drive_files.ListFiles.call_args_list)
    assert [(file.path, file.size_bytes) for file in files] == [("SKILL.md", 120), ("methods/a.md", 9)]
    assert (first.parent_path.path, first.recursive, second.page_token) == (_FOLDER, True, "next")


def test_get_content_reads_the_skill_md_the_skill_points_at(downloads) -> None:
    """The content is the SKILL.md the skill's record points at, read at its current revision."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.GetFileByIdentity.return_value = files_pb2.GetFileByIdentityResponse(
        file=_file(f"{_FOLDER}/SKILL.md", "rev-1")
    )
    downloads["https://storage/rev-1"] = _SKILL_MD.encode()

    assert skill.get_content() == _SKILL_MD
    assert (
        clients.drive_files.GetFileByIdentity.call_args.args[0].identity
        == _proto_skill().backing.file_store.entry_file_identity
    )


def test_read_file_downloads_the_current_revision_as_text(downloads) -> None:
    """Reading a file downloads its current revision and decodes it as UTF-8."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_file(f"{_FOLDER}/methods/a.md", "rev-a"))
    downloads["https://storage/rev-a"] = "Méthode A.\n".encode()

    assert skill.read_file("methods/a.md") == "Méthode A.\n"
    assert clients.drive_files.GetFile.call_args.args[0].path.path == f"{_FOLDER}/methods/a.md"
    # The download names the revision reference File Store returned, as it is.
    assert clients.drive_files.GetDownloadUrl.call_args.args[0].source_ref == _ref("rev-a")


def test_read_file_refuses_a_file_that_is_not_text(downloads) -> None:
    """The client reads text only; a binary file in the skill's folder raises an error naming it."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.GetFile.return_value = files_pb2.GetFileResponse(file=_file(f"{_FOLDER}/diagram.png", "rev-d"))
    downloads["https://storage/rev-d"] = b"\x89PNG\r\n\x1a\n\xff"

    with pytest.raises(NominalError, match="diagram.png"):
        skill.read_file("diagram.png")


def test_file_operations_need_the_skill_folder() -> None:
    """A skill whose SKILL.md isn't at skills/<name>/SKILL.md has no folder to list."""
    clients = _skill_clients()
    backing = skills_pb2.SkillBacking(
        file_store=skills_pb2.FileStoreSkillBacking(entry_path=file_store_pb2.LogicalPath(path="notes/SKILL.md"))
    )
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(skill=_proto_skill(backing=backing))
    skill = Skill._from_proto(clients, _proto_skill())

    with pytest.raises(NominalError, match="notes/SKILL.md"):
        skill.list_files()
    clients.drive_files.ListFiles.assert_not_called()


def test_export_writes_every_file_under_the_skill_name(downloads, tmp_path: Path) -> None:
    """Export writes the current files, SKILL.md included, into a folder named after the skill."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(
        _file(f"{_FOLDER}/SKILL.md", "rev-entry"), _file(f"{_FOLDER}/m/a.md", "rev-a")
    )
    downloads.update({"https://storage/rev-entry": _SKILL_MD.encode(), "https://storage/rev-a": b"Method A.\r\n"})

    folder = skill.export(tmp_path)

    assert folder == tmp_path / "post-test-review"
    assert (folder / "SKILL.md").read_text() == _SKILL_MD
    assert (folder / "m" / "a.md").read_bytes() == b"Method A.\r\n"
    with pytest.raises(FileExistsError):
        skill.export(tmp_path)


def test_export_refuses_a_path_that_could_leave_the_folder(downloads, tmp_path: Path) -> None:
    """File Store accepts backslashes, which Windows reads as separators, so export writes nothing for such a path."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(_file(f"{_FOLDER}/..\\..\\evil.md", "rev-x"))

    with pytest.raises(ValueError):
        skill.export(tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_export_on_windows_refuses_a_skill_name_with_a_drive(downloads, tmp_path: Path, monkeypatch) -> None:
    """The skill's folder name is checked as its files' paths are, so a name such as D:review writes nothing."""
    clients = _skill_clients()
    backing = _proto_skill().backing
    backing.file_store.entry_path.path = "skills/D:review/SKILL.md"
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(skill=_proto_skill(backing=backing))
    monkeypatch.setattr("nominal.experimental.analyst._skill._ON_WINDOWS", True)

    with pytest.raises(ValueError):
        skill.export(tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_export_refuses_two_files_that_land_on_one(downloads, tmp_path: Path) -> None:
    """Where case is ignored, Notes.md and notes.md are one file, so export raises instead of keeping only one."""
    (tmp_path / "probe").write_text("")
    if not (tmp_path / "PROBE").exists():
        pytest.skip("this file system tells names apart by case")
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(
        _file(f"{_FOLDER}/Notes.md", "rev-a"), _file(f"{_FOLDER}/notes.md", "rev-b")
    )
    downloads.update({"https://storage/rev-a": b"A", "https://storage/rev-b": b"B"})

    with pytest.raises(FileExistsError):
        skill.export(tmp_path / "out")


@pytest.mark.parametrize("path", ["C:x.md", "C:/Users/x.md", "notes:hidden.md"])
def test_export_on_windows_refuses_a_drive_or_a_stream(downloads, tmp_path: Path, monkeypatch, path) -> None:
    """Windows reads a colon as a drive or a file's hidden stream, so export writes nothing for such a path there."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.drive_files.ListFiles.return_value = _listing(_file(f"{_FOLDER}/{path}", "rev-x"))
    monkeypatch.setattr("nominal.experimental.analyst._skill._ON_WINDOWS", True)

    with pytest.raises(ValueError):
        skill.export(tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_unarchive_checks_the_current_state_before_restoring() -> None:
    """Restoring reads the skill's current state, so a stale snapshot neither blocks a restore nor turns a skill off."""
    clients = _skill_clients()
    skill = Skill._from_proto(clients, _proto_skill())
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(
        skill=_proto_skill(state=skills_pb2.SKILL_STATE_ARCHIVED, version=9)
    )
    clients.skills.UpdateSkill.return_value = skills_pb2.UpdateSkillResponse(
        skill=_proto_skill(state=skills_pb2.SKILL_STATE_DISABLED, version=10)
    )

    skill.unarchive()
    clients.skills.GetSkill.return_value = skills_pb2.GetSkillResponse(skill=_proto_skill(version=10))
    skill.unarchive()

    assert clients.skills.UpdateSkill.call_count == 1
    request = clients.skills.UpdateSkill.call_args.args[0]
    assert (request.skill.state, request.expected_version) == (skills_pb2.SKILL_STATE_DISABLED, 9)


@pytest.mark.parametrize(
    ("enabled", "archive_status", "states"),
    [
        (
            None,
            ArchiveStatusFilter.NOT_ARCHIVED,
            [skills_pb2.SKILL_STATE_DRAFT, skills_pb2.SKILL_STATE_ENABLED, skills_pb2.SKILL_STATE_DISABLED],
        ),
        (True, ArchiveStatusFilter.ANY, [skills_pb2.SKILL_STATE_ENABLED]),
        (
            False,
            ArchiveStatusFilter.ANY,
            [skills_pb2.SKILL_STATE_DRAFT, skills_pb2.SKILL_STATE_DISABLED, skills_pb2.SKILL_STATE_ARCHIVED],
        ),
        (None, ArchiveStatusFilter.ARCHIVED, [skills_pb2.SKILL_STATE_ARCHIVED]),
    ],
)
def test_search_selects_states_from_filters(enabled, archive_status, states) -> None:
    """The enabled and archive filters select the skill states the service searches."""
    client, clients = _client()
    clients.skills.SearchSkills.return_value = skills_pb2.SearchSkillsResponse()

    analyst.search_skills(client, enabled=enabled, archive_status=archive_status)

    assert list(clients.skills.SearchSkills.call_args.args[0].filter.states) == states


def test_search_with_no_selectable_state_makes_no_call() -> None:
    """Asking for enabled skills among archived ones matches nothing, so no request is sent."""
    client, clients = _client()

    assert analyst.search_skills(client, enabled=True, archive_status=ArchiveStatusFilter.ARCHIVED) == []
    clients.skills.SearchSkills.assert_not_called()


def test_search_follows_pages_in_name_order() -> None:
    """Search returns every page of matches, in name order, from the chosen workspace."""
    client, clients = _client()
    clients.skills.SearchSkills.side_effect = [
        skills_pb2.SearchSkillsResponse(skills=[_proto_skill()], next_page_token="next"),
        skills_pb2.SearchSkillsResponse(skills=[_proto_skill(skill_rid="ri.file-store.main.skill.b")]),
    ]

    results = analyst.search_skills(client, "review", workspace="ri.workspace.main.workspace.2")

    first, second = (call.args[0] for call in clients.skills.SearchSkills.call_args_list)
    assert [skill.rid for skill in results] == [_RID, "ri.file-store.main.skill.b"]
    assert (first.filter.text, first.sort) == ("review", skills_pb2.SKILL_LIBRARY_SORT_NAME_ASC)
    assert first.workspace_rid == "ri.workspace.main.workspace.2"
    assert second.page_token == "next"


def _write_skill_folder(folder: Path) -> None:
    (folder / "methods").mkdir(parents=True)
    (folder / ".git").mkdir()
    (folder / ".git" / "HEAD").write_bytes(b"\xff\xfe")
    (folder / "SKILL.md").write_text(_SKILL_MD)
    (folder / "methods" / "limits.md").write_text("Use the plan's limits.\n")


def _zip_skill_folder(tmp_path: Path) -> Path:
    archive_path = tmp_path / "skill.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for file in (tmp_path / "post-test-review").rglob("*"):
            if file.is_file():
                archive.write(file, file.relative_to(tmp_path).as_posix())
    return archive_path


def test_import_saves_skill_md_unchanged_and_skips_hidden_paths(uploads, tmp_path: Path) -> None:
    """Importing a folder saves SKILL.md as written and the other text files, skipping paths that start with '.'."""
    client, clients = _client()
    _write_skill_folder(tmp_path / "post-test-review")

    analyst.import_skill(client, tmp_path / "post-test-review")

    puts = _puts(clients)
    assert [put.destination.path.path.path for put in puts] == [f"{_FOLDER}/SKILL.md", f"{_FOLDER}/methods/limits.md"]
    assert uploads[puts[0].object.object_key] == _SKILL_MD.encode()


def test_import_skips_links_that_can_lead_outside_the_folder(uploads, tmp_path: Path) -> None:
    """A link in a folder can point anywhere, such as a key in the home folder, so links and what they reach are
    skipped.
    """
    client, clients = _client()
    folder = tmp_path / "post-test-review"
    _write_skill_folder(folder)
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "key.md").write_text("secret")
    (folder / "methods" / "key.md").symlink_to(secrets / "key.md")
    (folder / "linked").symlink_to(secrets, target_is_directory=True)

    analyst.import_skill(client, folder)

    puts = _puts(clients)
    assert [put.destination.path.path.path for put in puts] == [f"{_FOLDER}/SKILL.md", f"{_FOLDER}/methods/limits.md"]
    assert b"secret" not in uploads.values()


def test_import_reads_the_folder_holding_a_zips_skill_md(uploads, tmp_path: Path) -> None:
    """A .zip imports the folder that holds its SKILL.md."""
    client, clients = _client()
    _write_skill_folder(tmp_path / "post-test-review")

    analyst.import_skill(client, _zip_skill_folder(tmp_path))

    puts = _puts(clients)
    assert [put.destination.path.path.path for put in puts] == [f"{_FOLDER}/SKILL.md", f"{_FOLDER}/methods/limits.md"]
    assert uploads[puts[0].object.object_key] == _SKILL_MD.encode()


def test_import_takes_a_zips_shallowest_skill_md_as_the_skill(uploads, tmp_path: Path) -> None:
    """A SKILL.md nested inside a zipped skill folder is a reference file, as it is when importing the folder."""
    client, clients = _client()
    _write_skill_folder(tmp_path / "post-test-review")
    (tmp_path / "post-test-review" / "examples").mkdir()
    (tmp_path / "post-test-review" / "examples" / "SKILL.md").write_text("Example.\n")

    analyst.import_skill(client, _zip_skill_folder(tmp_path))

    puts = _puts(clients)
    assert uploads[puts[0].object.object_key] == _SKILL_MD.encode()
    assert sorted(put.destination.path.path.path for put in puts[1:]) == [
        f"{_FOLDER}/examples/SKILL.md",
        f"{_FOLDER}/methods/limits.md",
    ]


def test_import_refuses_binary_files_before_saving(uploads, tmp_path: Path) -> None:
    """A file that isn't UTF-8 text stops the import before anything is uploaded."""
    client, clients = _client()
    _write_skill_folder(tmp_path)
    (tmp_path / "diagram.png").write_bytes(b"\x89PNG\r\n\x1a\n\xff")

    with pytest.raises(ValueError, match="diagram.png"):
        analyst.import_skill(client, tmp_path)
    assert uploads == {}
    clients.skills.UpdateWorkspaceSkillsDrive.assert_not_called()


@pytest.mark.parametrize("content", [b"hello", b"\xff"])
def test_read_closes_download_session(monkeypatch, content) -> None:
    """Downloads release their session even when UTF-8 decoding fails."""
    client, clients = _client()
    session = MagicMock()
    session.get.return_value.content = content
    monkeypatch.setattr("nominal.experimental.analyst._skill.create_multipart_request_session", lambda **_: session)
    skill = analyst.get_skill(client, _RID)
    if content == b"hello":
        assert skill.get_content() == "hello"
    else:
        with pytest.raises(NominalError, match="Not UTF-8"):
            skill.get_content()
    session.close.assert_called_once_with()
