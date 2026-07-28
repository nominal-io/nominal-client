# VideoDatasetFile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose video ingest into datasets as a first-class `DatasetFile` subtype (`VideoDatasetFile`), with `Dataset.add_video*` upload, `list_video_files`/`get_video_file` retrieval, and runtime subtype specialization across all generic dataset-file paths.

**Architecture:** A new `VideoDatasetFile(DatasetFile)` carries video aggregates + a private timestamp manifest. Each class keeps a class-specific `_from_conjure`; a single free dispatch function `_dataset_file_from_conjure` chooses the subtype from `row.metadata.video` and is called at the four build-from-row sites. Uploads ride the legacy conjure `ingest.ingest(...)` path with `VideoOptsV2`, with an ingest-job fallback when the response omits the dataset-file id. `update()` is out of scope for v1 (backend-gated).

**Tech Stack:** Python ≥3.10, `nominal_api` conjure bindings (`ingest_api`, `scout_catalog`, `scout_video_api`, `api`), `typing_extensions.Self`, pytest + `unittest.mock`.

**Design doc:** `docs/superpowers/specs/2026-07-23-video-dataset-files-design.md`

## Global Constraints

- Python floor `>=3.10,<4`; mypy runs in `strict = true` mode — all new code must type-check.
- Core dataclasses are `@dataclass(frozen=True)`. **Only** `VideoDatasetFile` adds `kw_only=True` (required because `_timestamp_manifest` is a required field following `DatasetFile`'s defaulted `_ingest_error_message`); this is an intentional, documented divergence — do not "fix" it.
- Return-type annotations on generic paths stay `DatasetFile` / `Iterable[DatasetFile]` / `Sequence[DatasetFile]`; only the runtime type specializes.
- **Do NOT export `IngestStatus`** in this work; leave the `TODO(drake)` at `nominal/core/dataset_file.py:302` untouched.
- **Keep** the private `_timestamp_manifest` field (`repr=False, compare=False`); introduce no public timestamp-manifest type.
- **Do NOT implement `update()`** in v1. It is deferred behind a backend file-addressed timing endpoint.
- Tests follow the existing idiom in `tests/core/test_dataset_file.py`: `MagicMock` objects, `patch(...)` on module-level functions, and calling methods on a mock `self` where validation precedes client use.
- Every commit message ends with the trailer:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

## File Structure

- **Create** `nominal/core/video_dataset_file.py` — `VideoDatasetFile` class + its `_from_conjure`.
- **Create** `nominal/core/_video_ingest.py` — `build_video_timestamp_manifest` + `build_video_ingest_options` (dataset-backed video ingest builders).
- **Modify** `nominal/core/dataset_file.py` — extract `_parse_common_file_fields`; add `_dataset_file_from_conjure` dispatcher.
- **Modify** `nominal/core/dataset.py` — wire dispatcher into `list_files`, `get_dataset_file`, `_iter_search_dataset_files`; add `_handle_video_ingest_response`, `add_video`, `add_video_from_io`, `add_mcap_video`, `add_mcap_video_from_io`, `list_video_files`, `get_video_file`.
- **Modify** `nominal/core/ingestion_job.py` — wire dispatcher into `_iter_dataset_files`.
- **Modify** `nominal/core/__init__.py` — export `VideoDatasetFile`.
- **Create** tests: `tests/core/test_video_dataset_file.py`, `tests/core/test_video_ingest.py`, and additions to `tests/core/test_dataset.py`.

---

### Task 1: `VideoDatasetFile` class + shared common-field parsing + export

**Files:**
- Modify: `nominal/core/dataset_file.py:253-299` (extract `_parse_common_file_fields`)
- Create: `nominal/core/video_dataset_file.py`
- Modify: `nominal/core/__init__.py:25,41-96`
- Test: `tests/core/test_video_dataset_file.py`

**Interfaces:**
- Produces: `_parse_common_file_fields(clients: DatasetFile._Clients, dataset_file: scout_catalog.DatasetFile) -> dict[str, Any]` (module fn in `dataset_file.py`); `class VideoDatasetFile(DatasetFile)` with fields `num_frames|num_segments|None`, `media_duration_seconds|media_frame_rate|scale_factor: float|None`, private `_timestamp_manifest`, and classmethod `_from_conjure(cls, clients, dataset_file) -> Self`.

- [ ] **Step 1: Write the failing tests**

Create `tests/core/test_video_dataset_file.py`:

```python
from __future__ import annotations

from unittest.mock import MagicMock, patch

from nominal.core.dataset_file import DatasetFile, IngestStatus
from nominal.core.video_dataset_file import VideoDatasetFile


def _common_kwargs(clients: MagicMock) -> dict:
    return dict(
        id="file-1",
        dataset_rid="ds-1",
        name="front.mp4",
        bounds=None,
        uploaded_at=0,
        ingested_at=None,
        deleted_at=None,
        ingest_status=IngestStatus.INGESTING,
        timestamp_channel=None,
        timestamp_type=None,
        file_tags=None,
        tag_columns=None,
        _clients=clients,
        _ingest_error_message=None,
    )


def _video_row(segment: object | None) -> MagicMock:
    row = MagicMock()
    row.metadata.video.timestamp_manifest = MagicMock(name="manifest")
    row.metadata.video.segment_metadata = segment
    return row


def test_from_conjure_populates_aggregates_from_segment_metadata():
    clients = MagicMock()
    segment = MagicMock(
        num_frames=100, num_segments=3, scale_factor=2.0, media_duration_seconds=10.0, media_frame_rate=30.0
    )
    row = _video_row(segment)
    with patch("nominal.core.video_dataset_file._parse_common_file_fields", return_value=_common_kwargs(clients)):
        file = VideoDatasetFile._from_conjure(clients, row)

    assert isinstance(file, DatasetFile)
    assert (file.num_frames, file.num_segments, file.scale_factor) == (100, 3, 2.0)
    assert (file.media_duration_seconds, file.media_frame_rate) == (10.0, 30.0)
    assert file._timestamp_manifest is row.metadata.video.timestamp_manifest


def test_from_conjure_leaves_aggregates_none_without_segment_metadata():
    clients = MagicMock()
    row = _video_row(segment=None)
    with patch("nominal.core.video_dataset_file._parse_common_file_fields", return_value=_common_kwargs(clients)):
        file = VideoDatasetFile._from_conjure(clients, row)

    assert file.num_frames is None
    assert file.num_segments is None
    assert file.media_duration_seconds is None
    assert file.media_frame_rate is None
    assert file.scale_factor is None


def test_timestamp_manifest_excluded_from_repr_and_equality():
    clients = MagicMock()
    shared = dict(
        **_common_kwargs(clients),
        num_frames=1,
        num_segments=1,
        media_duration_seconds=1.0,
        media_frame_rate=1.0,
        scale_factor=1.0,
    )
    a = VideoDatasetFile(**shared, _timestamp_manifest=MagicMock(name="m1"))
    b = VideoDatasetFile(**shared, _timestamp_manifest=MagicMock(name="m2"))

    assert a == b  # differ only by the excluded manifest
    assert "timestamp_manifest" not in repr(a)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/core/test_video_dataset_file.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'nominal.core.video_dataset_file'`.

- [ ] **Step 3: Extract `_parse_common_file_fields` in `dataset_file.py`**

Add `Any` to the typing import at `nominal/core/dataset_file.py:10`:

```python
from typing import Any, Iterable, Mapping, Protocol, Sequence
```

Replace the body of `DatasetFile._from_conjure` (`dataset_file.py:253-299`) with a call to a new module-level function. The classmethod becomes:

```python
    @classmethod
    def _from_conjure(cls, clients: _Clients, dataset_file: scout_catalog.DatasetFile) -> Self:
        return cls(**_parse_common_file_fields(clients, dataset_file))
```

Add this module-level function immediately after the `DatasetFile` class (before the `IngestStatus` class at line 302):

```python
def _parse_common_file_fields(
    clients: DatasetFile._Clients, dataset_file: scout_catalog.DatasetFile
) -> dict[str, Any]:
    """Parse the fields shared by every DatasetFile subtype from a Catalog row."""
    upload_time = _SecondsNanos.from_flexible(dataset_file.uploaded_at).to_nanoseconds()
    ingest_time = (
        None
        if dataset_file.ingested_at is None
        else _SecondsNanos.from_flexible(dataset_file.ingested_at).to_nanoseconds()
    )
    delete_time = (
        None
        if dataset_file.deleted_at is None
        else _SecondsNanos.from_flexible(dataset_file.deleted_at).to_nanoseconds()
    )

    file_tags = None
    tag_columns = None
    if dataset_file.ingest_tag_metadata is not None:
        file_tags = dataset_file.ingest_tag_metadata.additional_file_tags
        tag_columns = dataset_file.ingest_tag_metadata.tag_columns

    timestamp_column = None
    timestamp_type = None
    if dataset_file.timestamp_metadata is not None:
        timestamp_column = dataset_file.timestamp_metadata.series_name
        timestamp_type = _catalog_timestamp_type_to_typed_timestamp_type(
            dataset_file.timestamp_metadata.timestamp_type
        )

    ingest_error = dataset_file.ingest_status.error
    ingest_error_message = None if ingest_error is None else f"{ingest_error.message} ({ingest_error.error_type})"

    return dict(
        id=dataset_file.id,
        dataset_rid=dataset_file.dataset_rid,
        name=dataset_file.name,
        bounds=None if dataset_file.bounds is None else Bounds._from_conjure(dataset_file.bounds),
        uploaded_at=upload_time,
        ingested_at=ingest_time,
        deleted_at=delete_time,
        ingest_status=IngestStatus._from_conjure(dataset_file.ingest_status),
        timestamp_channel=timestamp_column,
        timestamp_type=timestamp_type,
        file_tags=file_tags,
        tag_columns=tag_columns,
        _clients=clients,
        _ingest_error_message=ingest_error_message,
    )
```

- [ ] **Step 4: Create `nominal/core/video_dataset_file.py`**

```python
from __future__ import annotations

from dataclasses import dataclass, field

from nominal_api import scout_catalog, scout_video_api
from typing_extensions import Self

from nominal.core.dataset_file import DatasetFile, _parse_common_file_fields


@dataclass(frozen=True, kw_only=True)
class VideoDatasetFile(DatasetFile):
    """A video file stored as a dataset channel.

    A specialization of `DatasetFile` for video files. Supports all inherited dataset-file
    behavior (refresh/download/delete/poll/etc.) and exposes read-only aggregate metadata
    produced by segmentation. `update()` is intentionally not yet available.
    """

    # Private, unsupported ingest provenance. Excluded from repr and equality.
    _timestamp_manifest: scout_video_api.VideoFileTimestampManifest = field(repr=False, compare=False)

    num_frames: int | None = None
    num_segments: int | None = None
    media_duration_seconds: float | None = None
    media_frame_rate: float | None = None
    scale_factor: float | None = None

    @classmethod
    def _from_conjure(cls, clients: DatasetFile._Clients, dataset_file: scout_catalog.DatasetFile) -> Self:
        if dataset_file.metadata is None or dataset_file.metadata.video is None:
            raise ValueError(f"dataset file {dataset_file.id!r} has no video metadata")
        video_meta = dataset_file.metadata.video
        segment = video_meta.segment_metadata
        return cls(
            **_parse_common_file_fields(clients, dataset_file),
            _timestamp_manifest=video_meta.timestamp_manifest,
            num_frames=None if segment is None else segment.num_frames,
            num_segments=None if segment is None else segment.num_segments,
            media_duration_seconds=None if segment is None else segment.media_duration_seconds,
            media_frame_rate=None if segment is None else segment.media_frame_rate,
            scale_factor=None if segment is None else segment.scale_factor,
        )
```

- [ ] **Step 5: Export from `nominal/core/__init__.py`**

Add the import after `dataset_file` (near line 25):

```python
from nominal.core.video_dataset_file import VideoDatasetFile
```

Add `"VideoDatasetFile"` to `__all__` (alphabetically near `"VideoFile"`, line 88):

```python
    "Video",
    "VideoDatasetFile",
    "VideoFile",
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/core/test_video_dataset_file.py -v`
Expected: PASS (3 tests).

- [ ] **Step 7: Regression + typecheck**

Run: `pytest tests/core/test_dataset_file.py -v && mypy nominal/core/dataset_file.py nominal/core/video_dataset_file.py`
Expected: existing dataset-file tests PASS; mypy reports no errors.

- [ ] **Step 8: Commit**

```bash
git add nominal/core/dataset_file.py nominal/core/video_dataset_file.py nominal/core/__init__.py tests/core/test_video_dataset_file.py
git commit -m "$(cat <<'EOF'
feat(video): add VideoDatasetFile dataset-file subtype

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Free dispatch function + wire the four build-from-row sites

**Files:**
- Modify: `nominal/core/dataset_file.py` (add `_dataset_file_from_conjure`)
- Modify: `nominal/core/dataset.py:662,678,1059`
- Modify: `nominal/core/ingestion_job.py:173`
- Test: `tests/core/test_video_dataset_file.py` (dispatcher tests), `tests/core/test_dataset.py`

**Interfaces:**
- Consumes: `VideoDatasetFile._from_conjure`, `DatasetFile._from_conjure` (Task 1).
- Produces: `_dataset_file_from_conjure(clients: DatasetFile._Clients, dataset_file: scout_catalog.DatasetFile) -> DatasetFile` in `dataset_file.py`.

- [ ] **Step 1: Write the failing dispatcher tests**

Append to `tests/core/test_video_dataset_file.py`:

```python
from nominal.core.dataset_file import _dataset_file_from_conjure


def test_dispatch_returns_video_subtype_for_video_metadata():
    clients = MagicMock()
    row = MagicMock()
    row.metadata.video = MagicMock()  # video arm present
    with patch.object(VideoDatasetFile, "_from_conjure", return_value="video-file") as video_factory:
        result = _dataset_file_from_conjure(clients, row)
    assert result == "video-file"
    video_factory.assert_called_once_with(clients, row)


def test_dispatch_returns_base_type_when_no_video_metadata():
    clients = MagicMock()
    row = MagicMock()
    row.metadata = None  # no metadata at all
    with patch.object(DatasetFile, "_from_conjure", return_value="base-file") as base_factory:
        result = _dataset_file_from_conjure(clients, row)
    assert result == "base-file"
    base_factory.assert_called_once_with(clients, row)


def test_dispatch_returns_base_type_when_metadata_present_without_video_arm():
    clients = MagicMock()
    row = MagicMock()
    row.metadata.video = None  # metadata present, but not a video row
    with patch.object(DatasetFile, "_from_conjure", return_value="base-file") as base_factory:
        result = _dataset_file_from_conjure(clients, row)
    assert result == "base-file"
    base_factory.assert_called_once_with(clients, row)
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/core/test_video_dataset_file.py -k dispatch -v`
Expected: FAIL — `ImportError: cannot import name '_dataset_file_from_conjure'`.

- [ ] **Step 3: Add the dispatcher to `dataset_file.py`**

Add immediately after `_parse_common_file_fields`:

```python
def _dataset_file_from_conjure(
    clients: DatasetFile._Clients, dataset_file: scout_catalog.DatasetFile
) -> DatasetFile:
    """Build the correct DatasetFile subtype for a Catalog row.

    Returns a VideoDatasetFile when the row carries video metadata, otherwise a plain DatasetFile.
    This is the only place that knows about both types; the class factories stay type-specific.
    """
    # Local import avoids an import cycle (video_dataset_file imports this module).
    from nominal.core.video_dataset_file import VideoDatasetFile

    metadata = dataset_file.metadata
    if metadata is not None and metadata.video is not None:
        return VideoDatasetFile._from_conjure(clients, dataset_file)
    return DatasetFile._from_conjure(clients, dataset_file)
```

- [ ] **Step 4: Wire the four generic sites**

In `nominal/core/dataset.py`, add to the import from `dataset_file` (currently `from nominal.core.dataset_file import DatasetFile, ...`) the name `_dataset_file_from_conjure`, then replace:

- Line 662, `list_files`:
  ```python
          for file in files:
              yield _dataset_file_from_conjure(self._clients, file)
  ```
- Line 678, `get_dataset_file`:
  ```python
              raw_file = self._clients.catalog.get_dataset_file(self._clients.auth_header, self.rid, dataset_file_id)
              return _dataset_file_from_conjure(self._clients, raw_file)
  ```
- Line 1059, `_iter_search_dataset_files`:
  ```python
      for raw_file in search_dataset_files_paginated(clients.catalog, clients.auth_header, dataset_rid, query):
          yield _dataset_file_from_conjure(clients, raw_file)
  ```

In `nominal/core/ingestion_job.py`, add `_dataset_file_from_conjure` to the `from nominal.core.dataset_file import ...` line, then replace line 173:
  ```python
              for dataset_file in page.files:
                  yield _dataset_file_from_conjure(self._clients, dataset_file)
  ```

- [ ] **Step 5: Write a `list_files` specialization test**

Add to `tests/core/test_dataset.py`:

```python
from unittest.mock import MagicMock, patch

from nominal.core.dataset import Dataset
from nominal.core.video_dataset_file import VideoDatasetFile


def test_list_files_specializes_video_rows():
    ds = MagicMock()
    video_row = MagicMock(name="video_row")
    plain_row = MagicMock(name="plain_row")
    with (
        patch.object(Dataset, "_list_files", return_value=[video_row, plain_row]),
        patch("nominal.core.dataset._dataset_file_from_conjure",
              side_effect=lambda clients, row: "VIDEO" if row is video_row else "PLAIN"),
    ):
        result = list(Dataset.list_files(ds, successful_only=False))
    assert result == ["VIDEO", "PLAIN"]
```

- [ ] **Step 6: Run tests + typecheck**

Run: `pytest tests/core/test_video_dataset_file.py tests/core/test_dataset.py tests/core/test_ingestion_job.py -v && mypy nominal/core/dataset_file.py nominal/core/dataset.py nominal/core/ingestion_job.py`
Expected: PASS; mypy clean.

- [ ] **Step 7: Commit**

```bash
git add nominal/core/dataset_file.py nominal/core/dataset.py nominal/core/ingestion_job.py tests/core/test_video_dataset_file.py tests/core/test_dataset.py
git commit -m "$(cat <<'EOF'
feat(video): specialize video rows across generic dataset-file paths

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Video timestamp-manifest + ingest-options builders

**Files:**
- Create: `nominal/core/_video_ingest.py`
- Test: `tests/core/test_video_ingest.py`

**Interfaces:**
- Consumes: `nominal.core.video._upload_frame_timestamps` (existing).
- Produces:
  - `build_video_timestamp_manifest(auth_header: str, workspace_rid: str | None, upload_client: upload_api.UploadService, *, start: datetime | IntegralNanosecondsUTC | None = None, frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None, mcap_topic: str | None = None, header_provider: HeaderProvider | None = None) -> scout_video_api.VideoFileTimestampManifest`
  - `build_video_ingest_options(target_rid: str, channel: str, tags: Mapping[str, str] | None, s3_path: str, timestamp_manifest: scout_video_api.VideoFileTimestampManifest, overwrite_overlapping: bool) -> ingest_api.IngestOptions`

- [ ] **Step 1: Write the failing tests**

Create `tests/core/test_video_ingest.py`:

```python
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core._video_ingest import build_video_ingest_options, build_video_timestamp_manifest


def test_manifest_from_start_uses_no_manifest_arm():
    manifest = build_video_timestamp_manifest("auth", None, MagicMock(), start=1_000_000_000)
    assert manifest.no_manifest is not None
    assert manifest.s3path is None
    assert manifest.mcap is None


def test_manifest_from_mcap_topic_uses_mcap_arm():
    manifest = build_video_timestamp_manifest("auth", None, MagicMock(), mcap_topic="/camera/front")
    assert manifest.mcap is not None
    assert manifest.mcap.mcap_channel_locator.topic == "/camera/front"


def test_manifest_from_frame_timestamps_uploads_and_uses_s3path():
    with patch("nominal.core._video_ingest._upload_frame_timestamps", return_value="s3://path") as upload:
        manifest = build_video_timestamp_manifest("auth", "ws", MagicMock(), frame_timestamps=[1, 2, 3])
    assert manifest.s3path == "s3://path"
    upload.assert_called_once()


def test_manifest_requires_exactly_one_mode():
    with pytest.raises(ValueError):
        build_video_timestamp_manifest("auth", None, MagicMock())
    with pytest.raises(ValueError):
        build_video_timestamp_manifest("auth", None, MagicMock(), start=1, mcap_topic="/t")


def test_ingest_options_builds_video_v2():
    manifest = MagicMock(name="manifest")
    opts = build_video_ingest_options("ds-rid", "camera/front", {"vehicle": "alpha"}, "s3://p", manifest, True)
    assert opts.video_v2 is not None
    assert opts.video_v2.channel == "camera/front"
    assert opts.video_v2.tags == {"vehicle": "alpha"}
    assert opts.video_v2.over_write_segments is True
    assert opts.video_v2.target.existing.dataset_rid == "ds-rid"
    assert opts.video_v2.timestamp_manifest is manifest


def test_ingest_options_none_tags_becomes_empty_and_no_overwrite_is_none():
    opts = build_video_ingest_options("ds-rid", "c", None, "s3://p", MagicMock(), False)
    assert opts.video_v2.tags == {}
    assert opts.video_v2.over_write_segments is None
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/core/test_video_ingest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'nominal.core._video_ingest'`.

- [ ] **Step 3: Create `nominal/core/_video_ingest.py`**

```python
from __future__ import annotations

from datetime import datetime
from typing import Mapping, Sequence

from nominal_api import api, ingest_api, scout_video_api, upload_api

from nominal.core._utils.networking import HeaderProvider
from nominal.core.video import _upload_frame_timestamps
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


def build_video_timestamp_manifest(
    auth_header: str,
    workspace_rid: str | None,
    upload_client: upload_api.UploadService,
    *,
    start: datetime | IntegralNanosecondsUTC | None = None,
    frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
    mcap_topic: str | None = None,
    header_provider: HeaderProvider | None = None,
) -> scout_video_api.VideoFileTimestampManifest:
    """Build a timestamp manifest for dataset-backed video ingest.

    Exactly one of `start`, `frame_timestamps`, or `mcap_topic` must be provided.
    """
    provided = [p for p in (start, frame_timestamps, mcap_topic) if p is not None]
    if len(provided) != 1:
        raise ValueError("exactly one of 'start', 'frame_timestamps', or 'mcap_topic' must be provided")

    if mcap_topic is not None:
        return scout_video_api.VideoFileTimestampManifest(
            mcap=scout_video_api.McapTimestampManifest(api.McapChannelLocator(topic=mcap_topic))
        )
    if frame_timestamps is not None:
        s3_path = _upload_frame_timestamps(
            auth_header, workspace_rid, upload_client, frame_timestamps, header_provider=header_provider
        )
        return scout_video_api.VideoFileTimestampManifest(s3path=s3_path)
    return scout_video_api.VideoFileTimestampManifest(
        no_manifest=scout_video_api.NoTimestampManifest(starting_timestamp=_SecondsNanos.from_flexible(start).to_api())
    )


def build_video_ingest_options(
    target_rid: str,
    channel: str,
    tags: Mapping[str, str] | None,
    s3_path: str,
    timestamp_manifest: scout_video_api.VideoFileTimestampManifest,
    overwrite_overlapping: bool,
) -> ingest_api.IngestOptions:
    """Build IngestOptions for a VideoOptsV2 ingest into an existing dataset channel."""
    return ingest_api.IngestOptions(
        video_v2=ingest_api.VideoOptsV2(
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            target=ingest_api.DatasetIngestTarget(
                existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=target_rid)
            ),
            timestamp_manifest=timestamp_manifest,
            channel=channel,
            tags={**(tags or {})},
            over_write_segments=overwrite_overlapping or None,
        )
    )
```

- [ ] **Step 4: Run tests + typecheck**

Run: `pytest tests/core/test_video_ingest.py -v && mypy nominal/core/_video_ingest.py`
Expected: PASS (6 tests); mypy clean.

- [ ] **Step 5: Commit**

```bash
git add nominal/core/_video_ingest.py tests/core/test_video_ingest.py
git commit -m "$(cat <<'EOF'
feat(video): add dataset video ingest manifest/options builders

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `_handle_video_ingest_response` (ingest-response fallback)

**Files:**
- Modify: `nominal/core/dataset.py` (add method near `_handle_ingest_response`, line 128)
- Test: `tests/core/test_dataset.py`

**Interfaces:**
- Consumes: `_dataset_file_from_conjure` (Task 2), `IngestionJob._from_conjure` + `IngestionJob.dataset_files()` (existing), `VideoDatasetFile` (Task 1).
- Produces: `Dataset._handle_video_ingest_response(self, response: ingest_api.IngestResponse) -> VideoDatasetFile`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/core/test_dataset.py`:

```python
import pytest

from nominal.core.exceptions import NominalIngestError


def _video_response(dataset_file_id, ingest_job_rid="job-1"):
    response = MagicMock()
    response.details.dataset.dataset_rid = "ds-1"
    response.details.dataset.dataset_file_id = dataset_file_id
    response.ingest_job_rid = ingest_job_rid
    return response


def test_handle_video_response_prefers_direct_dataset_file_id():
    ds = MagicMock()
    video_file = MagicMock(spec=VideoDatasetFile)
    with patch("nominal.core.dataset._dataset_file_from_conjure", return_value=video_file):
        result = Dataset._handle_video_ingest_response(ds, _video_response("file-1"))
    assert result is video_file
    ds._clients.catalog.get_dataset_file.assert_called_once()


def test_handle_video_response_direct_id_non_video_raises():
    ds = MagicMock()
    with (
        patch("nominal.core.dataset._dataset_file_from_conjure", return_value=MagicMock()),  # not a VideoDatasetFile
        pytest.raises(NominalIngestError, match="not a video dataset file"),
    ):
        Dataset._handle_video_ingest_response(ds, _video_response("file-1"))


def test_handle_video_response_falls_back_to_ingest_job_single_video_file():
    ds = MagicMock()
    video_file = MagicMock(spec=VideoDatasetFile)
    job = MagicMock()
    # A non-video file (bare MagicMock) and the one video file; the handler must filter to the video.
    job.dataset_files.return_value = [MagicMock(), video_file]
    with patch("nominal.core.dataset.IngestionJob._from_conjure", return_value=job):
        result = Dataset._handle_video_ingest_response(ds, _video_response(None))
    assert result is video_file


def test_handle_video_response_fallback_zero_or_multiple_raises():
    ds = MagicMock()
    job = MagicMock()
    job.dataset_files.return_value = []
    with (
        patch("nominal.core.dataset.IngestionJob._from_conjure", return_value=job),
        pytest.raises(NominalIngestError, match="exactly one video file"),
    ):
        Dataset._handle_video_ingest_response(ds, _video_response(None))


def test_handle_video_response_no_id_and_no_job_raises():
    ds = MagicMock()
    with pytest.raises(NominalIngestError, match="neither a dataset-file id nor an ingest job"):
        Dataset._handle_video_ingest_response(ds, _video_response(None, ingest_job_rid=None))
```

> Note: `MagicMock(spec=VideoDatasetFile)` makes `isinstance(mock, VideoDatasetFile)` return True; a bare `MagicMock()` does not.

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/core/test_dataset.py -k handle_video_response -v`
Expected: FAIL — `AttributeError: ... does not have the attribute '_handle_video_ingest_response'` (patch target) / method missing.

- [ ] **Step 3: Add the method to `Dataset`**

In `nominal/core/dataset.py`, ensure `_dataset_file_from_conjure` is imported (Task 2) and `VideoDatasetFile` is imported at top (`from nominal.core.video_dataset_file import VideoDatasetFile`). `IngestionJob` and `NominalIngestError` are already imported. Add after `_handle_ingest_response` (line 144):

```python
    def _handle_video_ingest_response(self, response: ingest_api.IngestResponse) -> VideoDatasetFile:
        details = response.details.dataset
        if details is not None and details.dataset_file_id is not None:
            raw = self._clients.catalog.get_dataset_file(
                self._clients.auth_header, details.dataset_rid, details.dataset_file_id
            )
            file = _dataset_file_from_conjure(self._clients, raw)
            if not isinstance(file, VideoDatasetFile):
                raise NominalIngestError(f"ingested file {details.dataset_file_id!r} is not a video dataset file")
            return file

        # Backend compatibility: VideoOptsV2 may return a dataset RID without a dataset-file id.
        # Fall back to the ingest job and require exactly one produced video file.
        if response.ingest_job_rid is None:
            raise NominalIngestError("video ingest returned neither a dataset-file id nor an ingest job to track")
        job_conjure = self._clients.ingest_jobs.get_ingest_job(self._clients.auth_header, response.ingest_job_rid)
        job = IngestionJob._from_conjure(self._clients, job_conjure)
        video_files = [f for f in job.dataset_files() if isinstance(f, VideoDatasetFile)]
        if len(video_files) != 1:
            raise NominalIngestError(
                f"expected exactly one video file from ingest job {response.ingest_job_rid!r}, "
                f"found {len(video_files)}"
            )
        return video_files[0]
```

- [ ] **Step 4: Run tests + typecheck**

Run: `pytest tests/core/test_dataset.py -k handle_video_response -v && mypy nominal/core/dataset.py`
Expected: PASS (5 tests); mypy clean.

- [ ] **Step 5: Commit**

```bash
git add nominal/core/dataset.py tests/core/test_dataset.py
git commit -m "$(cat <<'EOF'
feat(video): resolve video ingest response with ingest-job fallback

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: `Dataset.add_video` + `Dataset.add_video_from_io`

**Files:**
- Modify: `nominal/core/dataset.py` (add methods; verify imports)
- Test: `tests/core/test_dataset.py`

**Interfaces:**
- Consumes: `build_video_timestamp_manifest`, `build_video_ingest_options` (Task 3); `_handle_video_ingest_response` (Task 4); `upload_multipart_io`, `FileType`, `FileTypes`, `path_upload_name`, `TextIOBase` (existing in `dataset.py`).
- Produces: `Dataset.add_video(path, *, channel, start|frame_timestamps, tags=None, overwrite_overlapping=False) -> VideoDatasetFile`; `Dataset.add_video_from_io(video, name, *, channel, start=None, frame_timestamps=None, file_type=FileTypes.MP4, tags=None, overwrite_overlapping=False) -> VideoDatasetFile`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/core/test_dataset.py`:

```python
import io


def test_add_video_from_io_requires_a_timestamp_mode():
    ds = MagicMock()
    with pytest.raises(ValueError, match="Either 'start' or 'frame_timestamps'"):
        Dataset.add_video_from_io(ds, io.BytesIO(b""), "v.mp4", channel="c")


def test_add_video_from_io_rejects_both_timestamp_modes():
    ds = MagicMock()
    with pytest.raises(ValueError, match="Only one of 'start' or 'frame_timestamps'"):
        Dataset.add_video_from_io(ds, io.BytesIO(b""), "v.mp4", channel="c", start=0, frame_timestamps=[1])


def test_add_video_from_io_rejects_text_stream():
    ds = MagicMock()
    with pytest.raises(TypeError, match="binary mode"):
        Dataset.add_video_from_io(ds, io.StringIO("x"), "v.mp4", channel="c", start=0)


def test_add_video_from_io_submits_video_v2_and_returns_handler_result():
    ds = MagicMock()
    ds.rid = "ds-rid"
    with (
        patch("nominal.core.dataset.build_video_timestamp_manifest", return_value="MANIFEST") as build_manifest,
        patch("nominal.core.dataset.build_video_ingest_options", return_value="OPTIONS") as build_opts,
        patch("nominal.core.dataset.upload_multipart_io", return_value="s3://p"),
    ):
        result = Dataset.add_video_from_io(
            ds, io.BytesIO(b"data"), "front.mp4", channel="camera/front", start=123, tags={"v": "a"}
        )

    build_manifest.assert_called_once()
    build_opts.assert_called_once_with("ds-rid", "camera/front", {"v": "a"}, "s3://p", "MANIFEST", False)
    ds._clients.ingest.ingest.assert_called_once()
    ds._handle_video_ingest_response.assert_called_once_with(ds._clients.ingest.ingest.return_value)
    assert result is ds._handle_video_ingest_response.return_value
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/core/test_dataset.py -k add_video_from_io -v`
Expected: FAIL — `AttributeError`/method missing.

- [ ] **Step 3: Add imports + methods to `Dataset`**

Ensure the top of `dataset.py` imports the builders:

```python
from nominal.core._video_ingest import build_video_ingest_options, build_video_timestamp_manifest
```

Add these methods to `Dataset` (place after `add_mcap_from_io`, before `add_ardupilot_dataflash`):

```python
    @overload
    def add_video(
        self,
        path: PathLike,
        *,
        channel: str,
        start: datetime | IntegralNanosecondsUTC,
        tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile: ...

    @overload
    def add_video(
        self,
        path: PathLike,
        *,
        channel: str,
        frame_timestamps: Sequence[IntegralNanosecondsUTC],
        tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile: ...

    def add_video(
        self,
        path: PathLike,
        *,
        channel: str,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile:
        """Upload a video file to this dataset as a channel.

        Exactly one of `start` (a single starting timestamp) or `frame_timestamps`
        (per-frame absolute nanosecond timestamps) must be provided.
        """
        path = Path(path)
        file_type = FileType.from_video(path)
        with open(path, "rb") as video:
            return self.add_video_from_io(
                video,
                path_upload_name(path, file_type),
                channel=channel,
                start=start,
                frame_timestamps=frame_timestamps,
                file_type=file_type,
                tags=tags,
                overwrite_overlapping=overwrite_overlapping,
            )

    @overload
    def add_video_from_io(
        self, video: BinaryIO, name: str, *, channel: str, start: datetime | IntegralNanosecondsUTC,
        file_type: tuple[str, str] | FileType = FileTypes.MP4, tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile: ...

    @overload
    def add_video_from_io(
        self, video: BinaryIO, name: str, *, channel: str, frame_timestamps: Sequence[IntegralNanosecondsUTC],
        file_type: tuple[str, str] | FileType = FileTypes.MP4, tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile: ...

    def add_video_from_io(
        self,
        video: BinaryIO,
        name: str,
        *,
        channel: str,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MP4,
        tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile:
        """Upload video data from a binary file-like object to this dataset as a channel."""
        if isinstance(video, TextIOBase):
            raise TypeError(f"video {video!r} must be open in binary mode, rather than text mode")
        if start is None and frame_timestamps is None:
            raise ValueError("Either 'start' or 'frame_timestamps' must be provided")
        if start is not None and frame_timestamps is not None:
            raise ValueError("Only one of 'start' or 'frame_timestamps' may be provided")

        file_type = FileType(*file_type)
        workspace_rid = self._clients.resolve_default_workspace_rid()
        timestamp_manifest = build_video_timestamp_manifest(
            self._clients.auth_header,
            workspace_rid,
            self._clients.upload,
            start=start,
            frame_timestamps=frame_timestamps,
            header_provider=self._clients.header_provider,
        )
        s3_path = upload_multipart_io(
            self._clients.auth_header,
            workspace_rid,
            video,
            name,
            file_type,
            self._clients.upload,
            header_provider=self._clients.header_provider,
        )
        request = ingest_api.IngestRequest(
            options=build_video_ingest_options(
                self.rid, channel, tags, s3_path, timestamp_manifest, overwrite_overlapping
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        return self._handle_video_ingest_response(response)
```

> If `FileType.from_video` does not exist, use `FileType.from_path_video` / the same helper legacy `Video.add_file` uses (`nominal/core/video.py` around line 160) — confirm the exact name and mirror it. `overwrite_overlapping=False` in `add_video_from_io`'s overloads keeps the two typed signatures aligned with the implementation.

- [ ] **Step 4: Run tests + typecheck**

Run: `pytest tests/core/test_dataset.py -k add_video -v && mypy nominal/core/dataset.py`
Expected: PASS; mypy clean.

- [ ] **Step 5: Commit**

```bash
git add nominal/core/dataset.py tests/core/test_dataset.py
git commit -m "$(cat <<'EOF'
feat(video): add Dataset.add_video and add_video_from_io

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `Dataset.add_mcap_video` + `Dataset.add_mcap_video_from_io`

**Files:**
- Modify: `nominal/core/dataset.py`
- Test: `tests/core/test_dataset.py`

**Interfaces:**
- Consumes: `build_video_timestamp_manifest(..., mcap_topic=...)`, `build_video_ingest_options`, `_handle_video_ingest_response`.
- Produces: `Dataset.add_mcap_video(path, *, channel, topic, tags=None, overwrite_overlapping=False) -> VideoDatasetFile`; `Dataset.add_mcap_video_from_io(mcap, name, *, channel, topic, file_type=FileTypes.MCAP, tags=None, overwrite_overlapping=False) -> VideoDatasetFile`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/core/test_dataset.py`:

```python
def test_add_mcap_video_from_io_rejects_text_stream():
    ds = MagicMock()
    with pytest.raises(TypeError, match="binary mode"):
        Dataset.add_mcap_video_from_io(ds, io.StringIO("x"), "v.mcap", channel="c", topic="/t")


def test_add_mcap_video_from_io_builds_mcap_manifest_and_submits():
    ds = MagicMock()
    ds.rid = "ds-rid"
    with (
        patch("nominal.core.dataset.build_video_timestamp_manifest", return_value="MANIFEST") as build_manifest,
        patch("nominal.core.dataset.build_video_ingest_options", return_value="OPTIONS") as build_opts,
        patch("nominal.core.dataset.upload_multipart_io", return_value="s3://p"),
    ):
        result = Dataset.add_mcap_video_from_io(
            ds, io.BytesIO(b"data"), "rec.mcap", channel="camera/front", topic="/camera/front/h264"
        )

    _, kwargs = build_manifest.call_args
    assert kwargs["mcap_topic"] == "/camera/front/h264"
    build_opts.assert_called_once_with("ds-rid", "camera/front", None, "s3://p", "MANIFEST", False)
    ds._clients.ingest.ingest.assert_called_once()
    assert result is ds._handle_video_ingest_response.return_value
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/core/test_dataset.py -k add_mcap_video -v`
Expected: FAIL — method missing.

- [ ] **Step 3: Add the methods to `Dataset`**

Add after `add_video_from_io`:

```python
    def add_mcap_video(
        self,
        path: PathLike,
        *,
        channel: str,
        topic: str,
        tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile:
        """Upload video data from an MCAP file to this dataset as a channel.

        Timestamps are obtained from the selected `topic`.
        """
        path = Path(path)
        file_type = FileType(*FileTypes.MCAP)
        with open(path, "rb") as mcap:
            return self.add_mcap_video_from_io(
                mcap,
                path_upload_name(path, file_type),
                channel=channel,
                topic=topic,
                file_type=file_type,
                tags=tags,
                overwrite_overlapping=overwrite_overlapping,
            )

    def add_mcap_video_from_io(
        self,
        mcap: BinaryIO,
        name: str,
        *,
        channel: str,
        topic: str,
        file_type: tuple[str, str] | FileType = FileTypes.MCAP,
        tags: Mapping[str, str] | None = None,
        overwrite_overlapping: bool = False,
    ) -> VideoDatasetFile:
        """Upload video data from a binary MCAP file-like object to this dataset as a channel."""
        if isinstance(mcap, TextIOBase):
            raise TypeError(f"mcap {mcap!r} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)
        workspace_rid = self._clients.resolve_default_workspace_rid()
        timestamp_manifest = build_video_timestamp_manifest(
            self._clients.auth_header,
            workspace_rid,
            self._clients.upload,
            mcap_topic=topic,
            header_provider=self._clients.header_provider,
        )
        s3_path = upload_multipart_io(
            self._clients.auth_header,
            workspace_rid,
            mcap,
            name,
            file_type,
            self._clients.upload,
            header_provider=self._clients.header_provider,
        )
        request = ingest_api.IngestRequest(
            options=build_video_ingest_options(
                self.rid, channel, tags, s3_path, timestamp_manifest, overwrite_overlapping
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        return self._handle_video_ingest_response(response)
```

- [ ] **Step 4: Run tests + typecheck**

Run: `pytest tests/core/test_dataset.py -k add_mcap_video -v && mypy nominal/core/dataset.py`
Expected: PASS; mypy clean.

- [ ] **Step 5: Commit**

```bash
git add nominal/core/dataset.py tests/core/test_dataset.py
git commit -m "$(cat <<'EOF'
feat(video): add Dataset.add_mcap_video and add_mcap_video_from_io

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: `Dataset.list_video_files` + `Dataset.get_video_file`

**Files:**
- Modify: `nominal/core/dataset.py`
- Test: `tests/core/test_dataset.py`

**Interfaces:**
- Consumes: `Dataset.list_files` and `Dataset.get_dataset_file` (now specializing subtypes, Task 2); `VideoDatasetFile`.
- Produces: `Dataset.list_video_files(*, successful_only: bool = True) -> Iterable[VideoDatasetFile]`; `Dataset.get_video_file(dataset_file_id: str) -> VideoDatasetFile`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/core/test_dataset.py`:

```python
def test_list_video_files_yields_only_video_subtypes():
    ds = MagicMock()
    video = MagicMock(spec=VideoDatasetFile)
    plain = MagicMock()  # not a VideoDatasetFile
    with patch.object(Dataset, "list_files", return_value=[video, plain, video]):
        result = list(Dataset.list_video_files(ds, successful_only=False))
    assert result == [video, video]
    Dataset.list_files.assert_called_once_with(ds, successful_only=False)


def test_get_video_file_returns_video_subtype():
    ds = MagicMock()
    video = MagicMock(spec=VideoDatasetFile)
    with patch.object(Dataset, "get_dataset_file", return_value=video):
        assert Dataset.get_video_file(ds, "file-1") is video


def test_get_video_file_raises_type_error_for_non_video():
    ds = MagicMock()
    with (
        patch.object(Dataset, "get_dataset_file", return_value=MagicMock()),  # not a VideoDatasetFile
        pytest.raises(TypeError, match="not a video dataset file"),
    ):
        Dataset.get_video_file(ds, "file-1")
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/core/test_dataset.py -k video_file -v`
Expected: FAIL — methods missing.

- [ ] **Step 3: Add the methods to `Dataset`**

Add near `get_dataset_file` / `search_files` (after `search_files`, line 709):

```python
    def list_video_files(self, *, successful_only: bool = True) -> Iterable[VideoDatasetFile]:
        """List video files ingested to this dataset.

        If successful_only, yields successfully-ingested video files only; otherwise also
        yields queued, ingesting, failed, and deletion-state video files.
        """
        for file in self.list_files(successful_only=successful_only):
            if isinstance(file, VideoDatasetFile):
                yield file

    def get_video_file(self, dataset_file_id: str) -> VideoDatasetFile:
        """Retrieve a video dataset file by ID.

        Raises:
            FileNotFoundError: the file does not exist in this dataset.
            TypeError: the ID identifies a non-video dataset file.
        """
        file = self.get_dataset_file(dataset_file_id)
        if not isinstance(file, VideoDatasetFile):
            raise TypeError(f"dataset file {dataset_file_id!r} is not a video dataset file")
        return file
```

- [ ] **Step 4: Run tests + full core suite + typecheck**

Run: `pytest tests/core -q && mypy nominal/core`
Expected: all PASS; mypy clean.

- [ ] **Step 5: Commit**

```bash
git add nominal/core/dataset.py tests/core/test_dataset.py
git commit -m "$(cat <<'EOF'
feat(video): add Dataset.list_video_files and get_video_file

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**1. Spec coverage (against the design doc's v1 acceptance criteria):**
- `VideoDatasetFile` is a `DatasetFile` subtype → Task 1.
- `_timestamp_manifest` absent from repr/equality → Task 1 (test).
- Aggregates populated when `segment_metadata` exists, else `None` → Task 1 (tests).
- Generic paths specialize video rows; refresh/polling preserve subtype → Task 2 (dispatcher wired into all four build-from-row sites; refresh preserved for free via `type(self)._from_conjure`, established in the design doc).
- `list_video_files(successful_only=False)` includes failed/ingesting → Task 7 (test passes `successful_only=False`; base `list_files` yields non-success rows).
- All four upload methods return an ingesting `VideoDatasetFile` + validate timestamp exclusivity → Tasks 5–6.
- Ingest-job fallback handles the missing dataset-file id → Task 4.
- No public op requires constructing video-channel/series internals → confirmed: `channel` is a plain `str`; no series type is exposed.
- `update()` → intentionally NOT covered (deferred; Global Constraints).

**2. Placeholder scan:** No "TBD"/"implement later". One conditional note in Task 5 (`FileType.from_video` name) with an explicit fallback + where to confirm — this is a named verification step, not an unresolved placeholder.

**3. Type consistency:** `_dataset_file_from_conjure`, `_parse_common_file_fields`, `build_video_timestamp_manifest`, `build_video_ingest_options`, `_handle_video_ingest_response` names/signatures are identical between their producing task and every consuming task. `VideoDatasetFile` field names match the conjure `VideoSegmentsMetadata` fields (`num_frames`, `num_segments`, `scale_factor`, `media_duration_seconds`, `media_frame_rate`). Upload methods consistently return `VideoDatasetFile`.

## Open verification items for the implementer

- Confirm `FileType.from_video(...)` exists (Task 5); mirror whatever helper legacy `Video.add_file` uses if the name differs.
- Confirm `VideoOptsV2.channel` accepts a plain `str` (conjure alias `api.Channel`); if it requires a wrapper type, adjust `build_video_ingest_options`.
- Confirm `self._clients.ingest_jobs.get_ingest_job(...)` is reachable from `Dataset._Clients` (it is used by `add_containerized` at `dataset.py:619`).

## Notes

- `_handle_ingest_response` (the tabular/mcap single-file handler, `dataset.py:128`) is intentionally left unchanged — it is only reached by non-video adders. Video uses the dedicated `_handle_video_ingest_response`.
