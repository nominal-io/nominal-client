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

drive = client.create_drive("telemetry", description="Raw telemetry from test stands")

readings = drive.put_file("local/readings.csv", "raw/2026-08/readings.csv")
print(readings.rid, readings.path, readings.size_bytes)
```

Putting to a path creates a new file there, and the path must be free — if something already
exists there, the call raises `NominalFileStoreError` instead of overwriting it. To store new
content for an existing file, put it onto the file itself. The file keeps its RID and gains a
revision:

```python
readings = drive.put_file("local/readings-corrected.csv", readings)
```

To upload bytes that aren't in a local file, pass a seekable binary stream to `put_file_obj`.
It uploads from the stream's current position to its end:

```python
import io

generated = drive.put_file_obj(io.BytesIO(csv_bytes), "raw/2026-08/generated.csv")
```

Both methods name and type the stored file after its destination in the drive. The service checks
the destination once the bytes have arrived, so a conflict is reported after the upload finishes.

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
by RID. Both can be downloaded, but only a `ManagedDriveFile` has `revisions()`, `move_to()`, and
`remove()` — see [Virtual drives](#virtual-drives) below.

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

A `DriveFileRevision` downloads the same way, with the content and name of that revision. The
directory must exist, and nothing may already be at the file's name inside it.

## Path rules

Every path in a drive is drive-relative:

- no leading or trailing `/`;
- no `.` or `..` segments;
- the empty string denotes the drive's root, so `drive.list_files()` (equivalently
  `drive.list_files("")`) lists everything at the top level.

## Destinations and their preconditions

Putting, moving, and restoring all say *where* content should end up. That's expressed as a
`FileDestination`, which is one of three things:

- a `str`: a drive-relative path, and the operation expects **nothing** to already be there;
- a `ManagedDriveFile`: that file, at its current revision;
- a `DriveFileRevision`: exactly that revision (useful when you want the operation to fail if the
  file has moved on since you last looked at it).

```python
# Move a file to a free path:
readings.move_to("archive/2026-08/readings.csv")

# Move another file onto it:
generated.move_to(readings)
```

Putting onto a file adds a revision to it, but moving onto a file replaces it: the destination file
is removed, ending its history, and the moved file takes over its path. If the destination path is
occupied and you passed a `str`, or the file or revision you passed is no longer current, the call
raises `NominalFileStoreError`.

## Soft removal and restoring a past revision

Removing a file is soft — its revisions are kept, and any of them can be restored later:

```python
generated.remove()
```

Every content change and every removal is recorded as a `DriveFileRevision` in the file's linear
history:

```python
for revision in generated.revisions():
    print(revision.rid, revision.state, revision.path)
```

A revision's `state` (`DriveFileState.ACTIVE` or `DriveFileState.REMOVED`) is what distinguishes a
removal marker from a content revision — a `REMOVED` revision records that the file was removed at
that point, rather than holding new content. To bring content back, restore an `ACTIVE` revision to
a destination:

```python
from nominal.core import DriveFileState

last_content_revision = next(r for r in generated.revisions() if r.state is DriveFileState.ACTIVE)
restored = last_content_revision.restore("raw/2026-08/generated.csv")
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
file resolves it first and downloads that pinned revision, which Nominal may fetch from the
provider as you download it; if the provider can't serve it, the download raises
`requests.HTTPError`.

A virtual drive is read-only through Nominal: `put_file` and `put_file_obj` on a `VirtualDrive`
raise `NominalFileStoreError` before uploading anything. A `VirtualDriveFile` has no history and
can't be moved or removed, so it has no `revisions()`, `move_to()`, or `remove()` methods.

## Updating, archiving, and unarchiving a drive

```python
drive.update(id="stand-telemetry", description="Raw and processed test-stand telemetry")
drive.update(description="")  # clears the description
drive.archive()
drive.unarchive()
```

`update` changes only the fields you pass. These three require organization-admin permissions.
Archiving hides a drive from listings that exclude archived drives (`list_drives()` excludes them
by default; pass `include_archived=True` to see them); unarchiving restores it.

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
