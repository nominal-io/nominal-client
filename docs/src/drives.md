# Nominal Drives

Nominal Drives give a workspace a place to keep arbitrary files, organized into **drives**.
A drive is a namespace with its own path hierarchy — think of it as a single bucket or shared
folder that files live in. The SDK exposes them from `nominal.core` (and `nominal.core.fs`).

Drives come in two flavors:

- A **managed** drive stores its files in Nominal, and you can write to it through this SDK:
  upload files, move them, remove them, and restore old revisions.
- A **virtual** drive mirrors an external provider (for example an S3 bucket or a Google Drive
  folder). It is read-only through Nominal — every write method on it raises immediately rather
  than sending a request that would fail anyway.

Both kinds are represented by `Drive`; a virtual one comes back as its subclass `VirtualDrive`,
so `isinstance` checks let you branch on which one you have.

## Creating a drive and putting a file into it

```python
from nominal.core import NominalClient

client = NominalClient.from_token(token)

drive = client.create_drive("telemetry")

uploaded = drive.put_file("local/readings.csv", "raw/2026-08/readings.csv")
print(uploaded.rid, uploaded.path, uploaded.size_bytes)
```

To upload bytes that aren't in a local file, pass a seekable binary stream to `put_file_obj`.
It uploads from the stream's current position to its end:

```python
import io

drive.put_file_obj(io.BytesIO(csv_bytes), "raw/2026-08/generated.csv")
```

Both methods name and type the stored file after `destination_path`, and both always create a
new file there, which must be free — if something already exists at that path, the call raises
`NominalFileStoreError` instead of overwriting it. To replace what's at a path, upload the new
content to a free path and then move it into place — see
[Destinations and their preconditions](#destinations-and-their-preconditions) below.

An existing drive can be retrieved instead of created:

```python
drive = client.get_drive_by_id("telemetry")
# or, if you have its RID:
drive = client.get_drive(drive_rid)

drives = client.list_drives()
```

## Listing a drive, and narrowing entries

`Drive.list_files` lists the immediate children of a path — it is not recursive:

```python
for entry in drive.list_files("raw/2026-08"):
    print(entry.path)
```

Each entry is a `DriveEntry`, which is either a `DriveDirectory` (implied by the paths beneath it,
and carrying no metadata of its own) or a `DriveFile`. Narrow with `isinstance` to get at
file-specific fields and methods:

```python
from nominal.core import DriveDirectory, ManagedDriveFile, VirtualDriveFile

for entry in drive.list_files("raw/2026-08"):
    if isinstance(entry, DriveDirectory):
        print("directory:", entry.path)
    elif isinstance(entry, ManagedDriveFile):
        print("managed file:", entry.path, entry.size_bytes, entry.rid)
    elif isinstance(entry, VirtualDriveFile):
        print("mirrored file:", entry.path, entry.size_bytes, entry.provider)
```

`ManagedDriveFile` and `VirtualDriveFile` are both `DriveFile`, because the backend models them
differently: a managed file has a stable RID and a linear revision history, while a file mirrored
from a provider is identified by a provider-specific reference and pinned by content rather than
by RID. Operations a virtual file can't support raise `NominalFileStoreError` — see
[Virtual drives](#virtual-drives) below.

You can also fetch a single file directly, without listing its parent directory:

```python
file = drive.get_file("raw/2026-08/readings.csv")
```

## Downloading a file

`download` writes a file's current content into an existing directory, named after the file's
path in the drive:

```python
local_path = file.download("downloads/")
```

A `DriveFileRevision` downloads the same way, with the content and name of that revision.

## Path rules

Every path in a drive is drive-relative:

- no leading or trailing `/`;
- no `.` or `..` segments;
- the empty string denotes the drive's root, so `drive.list_files()` (equivalently
  `drive.list_files("")`) lists everything at the top level.

## Destinations and their preconditions

Moving a file and restoring a revision need to say *where* a file should end up. That's
expressed as a `FileDestination`, which is one of three things:

- a `str`: a drive-relative path, and the operation expects **nothing** to already be there;
- a `ManagedDriveFile`: replace this file, at its current revision;
- a `DriveFileRevision`: replace exactly this revision (useful when you want the operation to fail
  if the file has moved on since you last looked at it).

```python
readings = drive.get_file("raw/2026-08/readings.csv")
assert isinstance(readings, ManagedDriveFile)

# Move it to a free path:
readings.move_to("archive/2026-08/readings.csv")

# Replace what's now at "archive/2026-08/readings.csv" with a different file's content:
corrected = drive.get_file("incoming/readings-corrected.csv")
assert isinstance(corrected, ManagedDriveFile)
corrected.move_to(readings)
```

If the destination path is occupied and you passed a `str`, or the file/revision you passed as a
destination is no longer current, the call raises `NominalFileStoreError`.

## Soft removal and restoring a past revision

Removing a file is soft — its revisions are kept, and any of them can be restored later:

```python
removed = uploaded.remove()
```

Every content change and every removal is recorded as a `DriveFileRevision` in the file's linear
history:

```python
for revision in uploaded.revisions():
    print(revision.rid, revision.state, revision.path)
```

A revision's `state` (`DriveFileState.ACTIVE` or `DriveFileState.REMOVED`) is what distinguishes a
removal marker from a content revision — a `REMOVED` revision records that the file was removed at
that point, rather than holding new content. To bring content back, restore an `ACTIVE` revision to
a destination:

```python
from nominal.core import DriveFileState

last_content_revision = next(r for r in uploaded.revisions() if r.state is DriveFileState.ACTIVE)
restored = last_content_revision.restore("raw/2026-08/readings.csv")
```

If the file the revision belongs to is still active, the destination must replace that file (pass
the `ManagedDriveFile` or its current revision); if the file was removed, a free `str` path works,
since removal frees the path it occupied.

## Virtual drives

A `VirtualDrive` mirrors an external provider and is fully readable: `list_files`, `get_file`,
reading fields off the files it returns, and downloading them all work as they do on a managed
drive.

Use `status()` to check on the provider it mirrors — useful for diagnosing why reads are failing or
stale:

```python
status = virtual_drive.status()
print(status.state, status.message, status.last_successful_check_at)
```

`status.state` is a `VirtualDriveState` — `ACTIVE`, `AUTH_ERROR`, `UNREACHABLE`, or
`INVALID_CONFIGURATION` — and `last_successful_check_at` is `None` if the provider has never been
reached successfully.

`VirtualDriveFile.resolve()` pins the file's content as observed when you retrieved it and returns
the RID of that pinned revision, as a durable reference — resolving the same observed content
always returns the same RID, even after the file changes further upstream. Downloading a virtual
file resolves it first and downloads that pinned revision.

Everything that would modify a virtual drive or a file mirrored into it raises
`NominalFileStoreError` instead of sending a request:

- `put_file` and `put_file_obj` on a `VirtualDrive`;
- `move_to` and `remove` on a `VirtualDriveFile`;
- `revisions()` on a `VirtualDriveFile` — a mirrored file has no history to list.

## Renaming, archiving, and unarchiving a drive

```python
drive.rename("new-id")
drive.archive()
drive.unarchive()
```

These three require organization-admin permissions. Archiving hides a drive from listings that
exclude archived drives (`list_drives()` excludes them by default; pass `include_archived=True` to
see them); unarchiving restores it.

## Errors

Writes that the backend rejects for a single file, and checks the SDK makes before sending a
request, raise `NominalFileStoreError`, which carries a `code` (a `FileStoreErrorCode`) and a
`message`:

```python
from nominal.core import FileStoreErrorCode, NominalFileStoreError

try:
    drive.put_file("local/readings.csv", "raw/2026-08/readings.csv")
except NominalFileStoreError as e:
    if e.code is FileStoreErrorCode.PATH_ALREADY_EXISTS:
        print("something is already there:", e.message)
    else:
        raise
```

`FileStoreErrorCode.UNKNOWN` covers an unset code or one a newer server sends that this version of
the SDK doesn't yet model.

Other failures raise the SDK's general errors: looking up a path that doesn't exist with `get_file`
raises `NominalNotFoundError`, as does downloading a revision whose bytes have been reclaimed by
retention or an administrator.
