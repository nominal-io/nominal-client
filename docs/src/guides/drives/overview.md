---
myst:
  html_meta:
    description: "Store, version and download files in Nominal Drives with Python"
---

# Nominal Drives

{.lead}
Store, version and download files in workspace-scoped drives with `nominal.experimental.drives`.
This API is experimental and may change between releases.

A managed drive stores files in Nominal. A virtual drive mirrors an external provider and is
read-only through Nominal. Both expose files through drive-relative paths.

```{include} ../_snippets/connect-the-client.md
```

## Create or retrieve a drive

Use {py:func}`~nominal.experimental.drives.create_drive` to create a managed drive in the client's
workspace:

```python
from nominal.experimental import drives

drive = drives.create_drive(client, "telemetry", description="Raw telemetry from test stands")
```

Retrieve an existing drive by its workspace-unique ID with
{py:func}`~nominal.experimental.drives.get_drive_by_id`, or by RID with
{py:func}`~nominal.experimental.drives.get_drive`:

```python
drive = drives.get_drive_by_id(client, "telemetry")
drive = drives.get_drive(client, drive_rid)
available_drives = drives.list_drives(client)
```

Creation, lookup by ID and listing accept `workspace_rid` to select a workspace explicitly.
A drive owned by another resource may have no ID; use its RID to retrieve it.

## Upload files and new versions

{py:meth}`~nominal.experimental.drives.Drive.put_file` uploads a local file. A string destination
creates a new file and must be unoccupied:

```python
readings = drive.put_file("local/readings.csv", "raw/readings.csv")
print(readings.rid, readings.path, readings.size_bytes)
```

To add a version to an existing file, pass the file as the destination. Its RID stays the same:

```python
readings = drive.put_file("local/readings-corrected.csv", readings)
```

{py:meth}`~nominal.experimental.drives.Drive.put_file_obj` accepts a seekable binary stream and
uploads from its current position to its end:

```python
import io

generated = drive.put_file_obj(io.BytesIO(b"time,value\n0,1\n"), "raw/generated.csv")
```

The destination determines the stored filename and content type. The service checks the
destination after the upload completes. If the path is occupied or the destination's revision
has changed, the call raises {py:class}`~nominal.experimental.drives.NominalFileStoreError`.
The passed destination snapshot is unchanged; use the returned file.

## List and retrieve files

{py:meth}`~nominal.experimental.drives.Drive.list_files` lists immediate children by default:

```python
for entry in drive.list_files("raw"):
    if isinstance(entry, drives.DriveDirectory):
        print("directory:", entry.path)
    elif isinstance(entry, drives.ManagedDriveFile):
        print("file:", entry.path, entry.size_bytes, entry.rid)
    elif isinstance(entry, drives.VirtualDriveFile):
        print("provider file:", entry.path, entry.size_bytes, entry.provider)
```

For a managed drive, pass `recursive=True` to collect active files at every depth, without
directory entries:

```python
for file in drive.list_files("raw", recursive=True):
    print(file.path)
```

Recursive listing cannot include removed files. For an immediate-child listing,
`include_removed=True` includes them. All listing modes collect every page.

Use {py:meth}`~nominal.experimental.drives.Drive.get_file` when you know the path:

```python
file = drive.get_file("raw/readings.csv")
```

Paths are drive-relative, with no leading or trailing slash and no `.` or `..` segments.
The empty path denotes the root: `drive.list_files()` lists its immediate children.

## Download content

{py:meth}`~nominal.experimental.drives.DriveFile.download` writes the snapshot's content into an
existing directory, using the basename of its drive path. It refuses to overwrite an existing file:

```python
local_path = file.download("downloads/")
```

{py:class}`~nominal.experimental.drives.DriveFileRevision` downloads the same way, using that
revision's content and filename. Files and revisions expose
{py:class}`~nominal.experimental.drives.DownloadAvailability`: `AVAILABLE`, `BYTES_RECLAIMED`,
`NO_CONTENT`, or `UNKNOWN`. Availability is a snapshot; a later download can still fail if
content is reclaimed or the provider becomes unavailable.

## Move files and directories

{py:meth}`~nominal.experimental.drives.ManagedDriveFile.move_to` refreshes the file in place:

```python
readings.move_to("archive/readings.csv")
```

A string destination must be free. Passing a managed file or revision as the destination
requires that revision to still be current. Moving onto another file removes that destination
file and puts the moved file at its path.

{py:meth}`~nominal.experimental.drives.Drive.move_directory` moves all active files beneath a directory:

```python
drive.move_directory("raw", "processed")
```

The destination must be empty and cannot contain or sit inside the source. The service can
reject a move that exceeds its limits. Existing file snapshots keep their old paths until
`refresh()` is called; the directory move returns no per-file details.

## Remove and restore files

{py:meth}`~nominal.experimental.drives.ManagedDriveFile.remove` removes a file while keeping its history:

```python
generated.refresh()  # Pick up its revision after the directory move.
generated.remove()
```

{py:meth}`~nominal.experimental.drives.ManagedDriveFile.revisions` returns revisions newest first,
including removal markers. Restore a content revision with
{py:meth}`~nominal.experimental.drives.DriveFileRevision.restore`:

```python
revision = next(r for r in generated.revisions() if r.state is drives.DriveFileState.ACTIVE)
restored = revision.restore("raw/generated.csv")
```

If the owning file is still active, pass that file or its current revision as the destination.
If it was removed, a free path can be used. Restoration requires the revision's content to
remain available.

## Apply changes in a batch

Use {py:meth}`~nominal.experimental.drives.Drive.batch` to put, move, remove,
restore, or move directories in one request. Results follow the input order. By default,
each change succeeds or fails independently and later changes still run after a failure:

```python
pending = drive.get_file("incoming/report.csv")
assert isinstance(pending, drives.ManagedDriveFile)
batch = drive.batch()
batch.move(pending, "reports/report.csv")
batch.move_directory("incoming/calibration", "reports/calibration")
results = batch.submit()

for result in results:
    if isinstance(result, drives.FileChangeFailure):
        print(result.code, result.message)
    elif isinstance(result, drives.FileChangeSuccess):
        print(result.file.path, result.revision.rid)
    else:
        print("Directory moved")
```

Use `batch.remove(file)` to remove a file or
`batch.restore(revision, destination)` to restore a revision. Inputs must belong to
this drive. Batch submission leaves your input snapshots unchanged; use returned files
or refresh existing snapshots. A batch is single-use: calling `submit` consumes it even
if submission fails, and subsequent additions or submissions raise `RuntimeError`.
Reconcile an uncertain outcome before creating a replacement batch.

To add uploads, prepare each put before submitting the batch. Preparation uploads bytes
and pins the destination's revision precondition, without creating or changing a drive file:

```python
prepared = drive.prepare_put_file("local/summary.csv", "reports/summary.csv")
batch = drive.batch()
batch.put(prepared)
batch.move_directory("staging", "published")
results = batch.submit(failure_policy=drives.FailurePolicy.ABORT)
```

`ABORT` applies every change or none. The first failed change reports its error; the
other results report `BATCH_FAILURE`. Uploads happen before the transaction, so an
aborted batch does not undo uploaded bytes. Use `prepare_put_file_obj` for a seekable
binary stream.

`CONTINUE`, the default, accepts up to 1,000 changes. `ABORT` accepts up to 100. The client
does not split batches. Earlier changes are visible to later changes, but all revision
preconditions come from the snapshots you submit. Changing the same file twice can therefore
make the second change fail its revision precondition. Request-level errors raise Nominal
exceptions; see [Recover after a transport error](#drive-change-retries) before retrying.

## Read virtual drives

{py:class}`~nominal.experimental.drives.VirtualDrive` exposes provider-backed files through
`get_file` and nonrecursive `list_files`. Check provider connectivity with
{py:meth}`~nominal.experimental.drives.VirtualDrive.status`:

```python
status = virtual_drive.status()
print(status.state, status.message, status.last_successful_check_at)
```

A {py:class}`~nominal.experimental.drives.VirtualDriveFile` keeps the revision reference observed
when it was retrieved. Downloads use that reference directly; Nominal resolves it and may
stream content from the provider. Use {py:meth}`~nominal.experimental.drives.VirtualDriveFile.resolve`
when you need a durable revision RID. Resolving does not copy or guarantee retention of the bytes.

Azure Blob files can be listed and identified. Resolving and downloading their content depends
on server support; the current service reports those operations as unimplemented.

## Handle rejected changes

A rejected single-file operation raises {py:class}`~nominal.experimental.drives.NominalFileStoreError` with a
{py:class}`~nominal.experimental.drives.FileStoreErrorCode`:

```python
try:
    drive.put_file("local/readings.csv", "raw/readings.csv")
except drives.NominalFileStoreError as error:
    if error.code is drives.FileStoreErrorCode.PATH_ALREADY_EXISTS:
        print("Choose a new path or retrieve the file to add a version.")
    else:
        raise
```

Failures reported as gRPC statuses use the client's general exceptions, such as
{py:class}`~nominal.exceptions.NominalNotFoundError`. Revision conflicts are surfaced to the
caller rather than retried with a newer revision.


(drive-change-retries)=
## Recover after a transport error

The client does not automatically replay file changes after a transport error such as
`UNAVAILABLE` or `RESOURCE_EXHAUSTED`. This applies to `DriveBatch.submit` and the single-operation
methods `put_file`, `put_file_obj`, `move_to`, `remove`, `restore`, and `move_directory`.
Reads and multipart byte uploads keep their existing retry behavior. For a put, the upload
and the final change that adds a file revision are separate requests; only the latter has
this no-retry policy.

A transport error can arrive **after the server has applied the changes**. For example,
a directory move can succeed but its response can be lost. Replaying it would then fail
because the source directory no longer exists, hiding the original success.

A transport exception is different from a returned `FileChangeFailure`:

- With `CONTINUE`, some or all changes may already have applied when the response is lost.
- With `ABORT`, the batch still applies all or none, but a transport exception does not tell
  you which happened. It is not evidence of rollback. A received batch response containing
  failures, including `BATCH_FAILURE`, does establish that the atomic batch applied nothing.

Before submitting changes again:

1. Refresh managed files you already hold. `file.refresh()` looks up the stable file RID,
   so it follows moves and returns the removed state after a removal. Inspect revision
   history with `file.revisions()` when the current state alone is insufficient.
2. For new files or directory moves, inspect the expected destination and source paths with
   `get_file` or `list_files`. Another writer may have changed them, so path existence alone
   is not proof that your request succeeded. Compare identities, revisions, and expected state.
3. Submit only changes you have established are still needed. Preserve the original revision
   preconditions unless you deliberately decide to act on newer state. If the outcome remains
   unclear, stop and investigate instead of replaying the batch.

Do not wrap these writes in an unconditional retry loop. Refreshing an input and retrying
without inspecting it can replace the original precondition with a newer revision and apply
a change you did not intend. The same caution applies to prepared puts: their destination
precondition remains pinned to preparation time, and an aborted batch does not undo the upload.
