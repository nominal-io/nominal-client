# VideoDatasetFile — Python SDK Design

- **Status:** approved design, pending user review of this doc
- **Scope:** public Python SDK surface only (`nominal.core`)
- **Branch / worktree:** `feat/video-dataset-files` (`.claude/worktrees/video-dataset-files`), branched from `origin/main`
- **Source spec:** "VideoDatasetFile Python API Specification" (proposed initial API), reviewed for feasibility against the codebase on 2026-07-23.

## Summary

Expose video ingest into **datasets** as a first-class `DatasetFile` subtype, so a video is uploaded and managed the same way tabular/MCAP data is:

```python
video_file = dataset.add_video("front-camera.mp4", channel="camera/front", start=now)
video_files = dataset.list_video_files()
```

A video file is a `VideoDatasetFile(DatasetFile)`. `Dataset` remains the handle for upload, retrieval, and listing. This replaces the legacy `Video` / `VideoFile` workflow for dataset-backed video, without removing the legacy classes.

## Decisions taken (this design)

These were settled with the API owner and shape everything below:

1. **v1 scope = read + upload; `update()` is deferred.** We build subtype specialization, the four upload methods, `list_video_files`, and `get_video_file` now. `VideoDatasetFile.update()` is **not shipped in v1** — it is designed here but gated on a backend file-addressed timing endpoint (see [Deferred: update()](#deferred-update-backend-gated)).
2. **Keep `_timestamp_manifest`** as a private, `repr=False`, `compare=False` field carrying immutable ingest provenance. No public timestamp-manifest model is introduced.
3. **Do not export `IngestStatus`** from `nominal.core` in this feature. Users read `file.ingest_status` but cannot import the enum type yet. (The `TODO(drake)` at `nominal/core/dataset_file.py:302` remains open, to be resolved separately.)

## Feasibility ground-truth (why this is buildable)

Confirmed against the code on the branch:

- **Runtime subtype dispatch has a precedent:** `Connection._from_conjure -> Connection | StreamingConnection` (`nominal/core/connection.py:19-32`). We use a *cleaner variant* (see below): each `_from_conjure` stays class-specific and a free function does the dispatch, so the base class never imports its subclass.
- **The data is available; `metadata` is just discarded today:** `DatasetFile._from_conjure` (`nominal/core/dataset_file.py:253-299`) drops `dataset_file.metadata`. The conjure row carries it: `scout_catalog.DatasetFile.metadata` is a union with a single `video` arm → `datasource.VideoFileMetadata(timestamp_manifest, segment_metadata)`.
- **Subtype survives refresh:** `RefreshableConjureMixin` rebuilds via `type(self)._from_conjure(...)` (`nominal/core/_utils/api_tools.py:64-65`). Because `VideoDatasetFile` has its own class-specific `_from_conjure`, `refresh()` / polling preserve the runtime type — with no dispatch logic on the base class at all.
- **Aggregates map 1:1:** `datasource.VideoSegmentsMetadata` exposes exactly `num_frames`, `num_segments`, `scale_factor`, `media_duration_seconds`, `media_frame_rate` (conjure `_impl.py:5604-5656`). `segment_metadata` is optional on the row → aggregates are `None` until segmentation exists.
- **`bounds` needs no network I/O:** it is already an inherited *field* on `DatasetFile` (`nominal/core/dataset_file.py:47`), not a property.
- **Upload primitive exists:** `IngestOptions(video_v2=VideoOptsV2(...))` — `VideoOptsV2` carries `source`, `target: DatasetIngestTarget`, `timestamp_manifest`, `channel`, `tags`, `over_write_segments` (conjure `_impl.py:18849`). It has **no consumers today**; we would be first.
- **Job-based file lookup exists:** `IngestionJob.dataset_files()` → `catalog.get_dataset_files_for_job` (`nominal/core/ingestion_job.py:168-180`) — backs the ingest-response fallback.

Architectural note: this rides the **legacy conjure `ingest.ingest(...)` path**, *not* the experimental v2 gRPC `IngestBuilder` on the sibling branch. That builder intentionally omits video ("the v2 endpoint rejects them today", `nominal/experimental/ingest/_ingest_builder.py:10-11`). Convergence with the builder is future work, not part of this design.

## Public API surface (v1)

### `VideoDatasetFile`

```python
@dataclass(frozen=True, kw_only=True)
class VideoDatasetFile(DatasetFile):
    _timestamp_manifest: scout_video_api.VideoFileTimestampManifest = field(
        repr=False, compare=False,
    )
    num_frames: int | None = None
    num_segments: int | None = None
    media_duration_seconds: float | None = None
    media_frame_rate: float | None = None
    scale_factor: float | None = None
```

- Publicly importable: `from nominal.core import VideoDatasetFile` (added to `nominal/core/__init__.py` `__all__`).
- **`kw_only=True` is deliberate and scoped to this subclass.** `DatasetFile` is not `kw_only`, but its last field (`_ingest_error_message`) has a default, so a *required* `_timestamp_manifest` cannot follow it in a positional dataclass. `kw_only=True` on the leaf resolves the ordering while keeping `_timestamp_manifest` required and honestly typed. The object is only ever constructed via `_from_conjure`, never positionally, so there is no ergonomic cost. This is the one intentional divergence from the house `@dataclass(frozen=True)` style, documented here so it is not "fixed" later by mistake.
- Inherits and supports unchanged: `refresh()`, `download()`, `download_original_files()`, `delete()`, `get_ingest_error()`, `poll_until_ingestion_completed()`, `get_file_size()`, and all identity/timestamp/tag/status/`bounds` fields. Valid anywhere a `DatasetFile` is accepted.
- The five aggregates are read-only and populated from `segment_metadata`; `None` for queued/ingesting/failed files (no segment metadata yet).
- `_timestamp_manifest` is private, unsupported as public API, hidden from `repr`, excluded from equality, immutable provenance.

### Uploading — four methods on `Dataset`

All four upload the source, submit a `VideoOptsV2` ingest, and immediately return an **ingesting** `VideoDatasetFile`. `tags` and `overwrite_overlapping` are common to all.

```python
@overload
def add_video(self, path: PathLike, *, channel: str,
              start: datetime | IntegralNanosecondsUTC,
              tags: Mapping[str, str] | None = None,
              overwrite_overlapping: bool = False) -> VideoDatasetFile: ...
@overload
def add_video(self, path: PathLike, *, channel: str,
              frame_timestamps: Sequence[IntegralNanosecondsUTC],
              tags: Mapping[str, str] | None = None,
              overwrite_overlapping: bool = False) -> VideoDatasetFile: ...

def add_video_from_io(self, video: BinaryIO, name: str, *, channel: str,
                      start=..., frame_timestamps=...,
                      file_type: tuple[str, str] | FileType = FileTypes.MP4,
                      tags=None, overwrite_overlapping=False) -> VideoDatasetFile: ...

def add_mcap_video(self, path: PathLike, *, channel: str, topic: str,
                   tags=None, overwrite_overlapping=False) -> VideoDatasetFile: ...

def add_mcap_video_from_io(self, mcap: BinaryIO, name: str, *, channel: str, topic: str,
                           file_type: tuple[str, str] | FileType = FileTypes.MCAP,
                           tags=None, overwrite_overlapping=False) -> VideoDatasetFile: ...
```

Rules:

- **Timestamp-mode exclusivity** for `add_video` / `add_video_from_io`: exactly one of `start` or `frame_timestamps` is required; both-missing and both-present each raise `ValueError`. Messages match legacy `Video.add_from_io` (`nominal/core/video.py:248-252`).
- **Change from spec:** `add_video_from_io` and `add_mcap_video_from_io` are also declared with `@overload`s for the timestamp modes (parity with `add_video` and legacy `Video.add_from_io`), giving call-site type safety over a runtime-only check.
- Binary streams only; a text stream raises `TypeError` (matches existing `add_from_io` guard).
- MCAP methods are separate from `add_video` (not an overload) and coexist with the existing tabular `Dataset.add_mcap`. MCAP timestamps come from `topic`.
- `overwrite_overlapping` maps to `VideoOptsV2.over_write_segments` (passed as `... or None`, matching legacy).

### Listing and retrieval

```python
def list_video_files(self, *, successful_only: bool = True) -> Iterable[VideoDatasetFile]: ...
def get_video_file(self, dataset_file_id: str) -> VideoDatasetFile: ...
```

- `list_video_files` filters the dataset's files to those whose runtime type is `VideoDatasetFile` (i.e. rows with `metadata.video`). `successful_only=False` also yields queued/ingesting/failed/deletion-state video files. Does not depend on indexed channel segments.
- `get_video_file` fetches via `get_dataset_file`; raises `FileNotFoundError` if absent, `TypeError` if the id resolves to a non-video dataset file. (Kept as specced; revisit if a Nominal-domain error is preferred later.)

### Runtime subtype specialization (generic paths)

These keep their existing static return annotations but return `VideoDatasetFile` at runtime when the row has `metadata.video`:

- `Dataset.list_files() -> Iterable[DatasetFile]`
- `Dataset.get_dataset_file(...) -> DatasetFile`
- `Dataset.search_files(...) -> Sequence[DatasetFile]`
- `IngestionJob.dataset_files() -> Sequence[DatasetFile]`

All of these build files through the free dispatch function (see [Internal mechanics](#internal-mechanics)), not by calling a class `_from_conjure` directly. Callers narrow with `isinstance(file, VideoDatasetFile)`. `refresh()` and batch-ingestion polling preserve the subtype via `type(self)._from_conjure`.

## Internal mechanics

- **Dispatch (class-specific factories + one free function):**
  - `DatasetFile._from_conjure` stays as-is in responsibility: builds a plain `DatasetFile` from a row's common fields. It gains nothing video-specific and does **not** import `VideoDatasetFile`.
  - `VideoDatasetFile._from_conjure` builds a `VideoDatasetFile`: common fields + `_timestamp_manifest` from `metadata.video.timestamp_manifest` + aggregates from `metadata.video.segment_metadata`. If `metadata.video` is unexpectedly absent it raises a clear internal error (a video file's row always carries it).
  - A module-level free function `_dataset_file_from_conjure(clients, row) -> DatasetFile` inspects `row.metadata`; if the `video` arm is present it delegates to `VideoDatasetFile._from_conjure`, else to `DatasetFile._from_conjure`. This is the only place that knows both types.
  - **All generic construction sites call the free function**, not the class methods: `Dataset._list_files`/`list_files`, `Dataset.get_dataset_file`, `Dataset.search_files`, `IngestionJob._iter_dataset_files`, and the batch helpers in `dataset_file.py` (`_batch_refresh_files`, `batch_get_dataset_files`, `wait_for_files_to_ingest`, `as_files_ingested`).
  - **`refresh()` is unaffected by the dispatcher:** it calls `type(self)._from_conjure` directly, which is already the correct class-specific factory, so subtype is preserved without the dispatcher and without the base knowing about the subclass.
  - **Module layout (to avoid a circular import):** `VideoDatasetFile` lives in its own module (`nominal/core/video_dataset_file.py`) importing `DatasetFile`. The free dispatcher must be reachable from `dataset_file.py`'s batch helpers, so it will live in `dataset_file.py` and obtain `VideoDatasetFile` via a function-local (lazy) import, keeping module-load dependencies acyclic (`video_dataset_file` → `dataset_file`, never the reverse at import time). Final placement confirmed during implementation.
- **Aggregate extraction:** if `segment_metadata is None`, all five aggregates are `None`; else copy field-for-field.
- **Ingest:** build `IngestOptions(video_v2=VideoOptsV2(source=S3IngestSource(path=s3_path), target=DatasetIngestTarget(existing=ExistingDatasetIngestDestination(dataset_rid=self.rid)), timestamp_manifest=..., channel=channel, tags=tags, over_write_segments=overwrite_overlapping or None))`, then `self._clients.ingest.ingest(auth_header, request)`. Upload via the existing `upload_multipart_io` / `upload_multipart_file` helpers (`nominal/core/_utils/multipart.py`).
- **Timestamp manifest construction** (reuse the legacy shapes in `nominal/core/video.py`):
  - `start` → `VideoFileTimestampManifest(no_manifest=NoTimestampManifest(starting_timestamp=...))`
  - `frame_timestamps` → upload JSON sidecar to S3 (as `Video._upload_frame_timestamps`), `VideoFileTimestampManifest(s3path=...)`
  - MCAP `topic` → `VideoFileTimestampManifest(mcap=McapTimestampManifest(McapChannelLocator(topic=topic)))`
- **Ingest-response fallback** (internal, not public contract): from the `IngestResponse`, prefer `details.dataset.dataset_file_id`; if absent, use `ingest_job_rid` → `IngestionJob.dataset_files()` filtered to `VideoDatasetFile`; require exactly one, else raise `NominalIngestError`.

## Deferred: `update()` (backend-gated)

Designed but **not built in v1**. Target signature (mirrors legacy `VideoFile.update`, minus `description`):

```python
def update(self, *, name=None, starting_timestamp=None,
           ending_timestamp=None, true_frame_rate=None, scale_factor=None) -> Self: ...
```

- At most one of `{ending_timestamp, true_frame_rate, scale_factor}`; `starting_timestamp` allowed alongside any one; invalid combos raise `ValueError` (logic copyable from `nominal/core/video_file.py:74-90`).
- `name` changeable independently or alongside timing. Timing changes require a successfully indexed file. Overlap/authorization errors propagate from the backend.
- Successful call refreshes and returns the same instance.
- **Blocker:** the client has no video-series/`VideoFileRid` — only `dataset_rid` + `dataset_file_id`. Legacy `update()` calls `video_file.update(...)` keyed by `VideoFileRid`. `update()` cannot ship until the backend provides a dataset-file-addressed timing endpoint (or surfaces the owning series RID in `metadata.video`). See gate #1 below.

## Backend dependencies (release gates)

1. **[HIGH — blocks `update()`]** A dataset-file-addressed timing-update API (or owning series RID in `metadata.video`), so callers never resupply channel identity.
2. **[HIGH — blocks listing correctness]** `metadata.video` written to Catalog *before* async segmentation, so queued/ingesting/failed video files are recognizable.
3. **[MED — blocks `update()` correctness]** Timing mutations persist effective `bounds` to Catalog; derived segment metadata refreshed after scaling; dataset-level bounds updated/recomputed; refresh returns synchronized state.
4. **[LOW — SDK-handled]** `VideoOptsV2` may return a dataset RID without a dataset-file id → covered by the fallback above.

Gates 2 and 4 are the only ones needed for the v1 (read/upload) surface. Gates 1 and 3 gate the deferred `update()`.

## Non-goals (initial)

Public channel identity on `VideoDatasetFile`; changing a file's channel/tags; public timestamp-manifest types or mutation; file descriptions; archive/unarchive; asset-backed video mutation; Python batch-update methods; playback/playlist/segment-query APIs; network I/O from `bounds`; removal of legacy `Video`/`VideoFile`.

## Testing strategy

- Unit: `_from_conjure` dispatch (video row → `VideoDatasetFile`; non-video → `DatasetFile`); aggregate extraction incl. `segment_metadata=None`; `_timestamp_manifest` excluded from `repr`/eq; timestamp-mode exclusivity `ValueError`s; ingest-response fallback (direct id, job-lookup single, zero/multiple → `NominalIngestError`); `get_video_file` `FileNotFoundError` / `TypeError`.
- Subtype preservation across `refresh()` and batch polling.
- Follow existing test patterns in `tests/core/` (e.g. `tests/core/test_multipart_uploader.py` style / existing dataset-file tests).

## Acceptance criteria (v1)

- `VideoDatasetFile` is a `DatasetFile` subtype; `_timestamp_manifest` absent from `repr`/equality.
- Aggregates populated when `segment_metadata` exists, else `None`.
- All generic dataset-file creation paths specialize video rows at runtime; `refresh()`/polling preserve the subtype.
- `list_video_files(successful_only=False)` includes failed and ingesting video files.
- All four upload methods return an ingesting `VideoDatasetFile` and validate timestamp-mode exclusivity.
- Ingest-job fallback handles the missing dataset-file id.
- No public operation requires callers to construct or understand video-channel/series internals.
- (`update()` acceptance criteria tracked with the deferred phase.)
