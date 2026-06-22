# Spatial Asset Package-Boundary Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split spatial-asset responsibilities so `nominal-ouster` owns file→CSV→Dagger-model creation (returning a `DaggerModel`) and core owns the `SpatialAsset` domain object, its typed metadata model, and `NominalClient.create_spatial_asset`.

**Architecture:** A new core value object `DaggerModel(dagger_uuid, source_handle)` and a typed `SpatialMetadata` family (today `PointCloudMetadata`, plus `ScanPattern` relocated from ouster) live in `nominal/core/spatial_asset.py`. `client.create_spatial_asset(name, *, dagger_model, metadata, …)` creates a `SpatialAsset` from those (scout `SpatialService` only — no Dagger). `nominal.ouster.create_dagger_model(client, csv_path, …) -> DaggerModel` replaces `upload_point_cloud`, keeping all Dagger orchestration in ouster.

**Tech Stack:** Python 3.10+, `uv` workspace, `pytest`, `mypy --strict`, `ruff`, conjure-generated `nominal_api`, `dagger_client` (ouster only).

## Global Constraints

- Python `>=3.10,<4`; use `from __future__ import annotations` (matches every module here).
- `ruff` line-length 120; `mypy --strict` must pass for `nominal`, `nominal.tdms`, `nominal.ouster`.
- Run all commands from the worktree root `C:/Users/dteid/workspace/nominal-client/.claude/worktrees/ouster` via `uv run …`.
- Typing principle: use enums/`Literal` only where the conjure API constrains the value. The spatial API's only metadata-field enum is `ScanPattern`; `sensor_model`/`coordinate_system` are free `str`.
- `dagger_client` may be imported **only** inside `packages/nominal-ouster`. Core must gain no Dagger dependency (`DaggerModel` is a plain string-only value object).
- Conventional-commit messages; end every commit body with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## Precursor (do first, not a TDD task)

The worktree currently has uncommitted, already-green Run/Asset spatial-scope work (`nominal/core/_utils/api_tools.py`, `nominal/core/asset.py`, `nominal/core/run.py`, `nominal/core/spatial_asset.py`). Commit it so the refactor starts from a clean tree:

```bash
git add nominal/core/_utils/api_tools.py nominal/core/asset.py nominal/core/run.py nominal/core/spatial_asset.py
git commit -m "feat(spatial): support spatial assets as run/asset data scopes

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
Verify clean: `git status --short` (expected: empty).

## File Structure

- `nominal/core/spatial_asset.py` (modify) — gains `ScanPattern`, `PointCloudMetadata`, `SpatialMetadata`, `_spatial_metadata_from_conjure`, `DaggerModel`, `_create_spatial_asset`; `SpatialAsset.sensor_model` → `SpatialAsset.metadata`.
- `nominal/core/__init__.py` (modify) — export `ScanPattern`, `PointCloudMetadata`, `SpatialMetadata`, `DaggerModel`.
- `nominal/core/client.py` (modify) — add `NominalClient.create_spatial_asset`.
- `packages/nominal-ouster/nominal/ouster/spatial.py` (modify) — `upload_point_cloud` → `create_dagger_model -> DaggerModel`; remove local `ScanPattern`; keep `ColumnDataType` + CSV/Dagger code.
- `packages/nominal-ouster/nominal/ouster/__init__.py` (modify) — export `convert_ouster_dataset`, `create_dagger_model`.
- `tests/core/test_spatial_asset.py` (create) — metadata model, `SpatialAsset._from_conjure`, `create_spatial_asset`.
- `packages/nominal-ouster/tests/test_spatial.py` (modify) — `upload_point_cloud` tests → `create_dagger_model` tests.

---

### Task 1: Core spatial value types (`DaggerModel`, metadata model, relocate `ScanPattern`)

**Files:**
- Modify: `nominal/core/spatial_asset.py`
- Modify: `nominal/core/__init__.py`
- Modify: `packages/nominal-ouster/nominal/ouster/spatial.py` (repoint `ScanPattern` import to core)
- Test: `tests/core/test_spatial_asset.py` (create)

**Interfaces:**
- Produces:
  - `class ScanPattern(Enum)` with `_to_conjure() -> scout_spatial_api.ScanPattern` and `classmethod _from_conjure(scout_spatial_api.ScanPattern) -> ScanPattern`.
  - `@dataclass(frozen=True) class PointCloudMetadata` with fields `sensor_model: str | None`, `coordinate_system: str | None`, `resolution_mm: float | None`, `scan_pattern: ScanPattern | None` (all default `None`) and `_to_conjure() -> scout_spatial_api.SpatialTypeMetadata`.
  - `SpatialMetadata: TypeAlias = PointCloudMetadata`.
  - `_spatial_metadata_from_conjure(type_metadata: scout_spatial_api.SpatialTypeMetadata) -> SpatialMetadata`.
  - `@dataclass(frozen=True) class DaggerModel` with `dagger_uuid: str`, `source_handle: str | None = None`.

- [ ] **Step 1: Write failing tests** in new file `tests/core/test_spatial_asset.py`

```python
from __future__ import annotations

from nominal_api import scout_spatial_api

from nominal.core.spatial_asset import (
    PointCloudMetadata,
    ScanPattern,
    _spatial_metadata_from_conjure,
)


def test_point_cloud_metadata_to_conjure_maps_fields_and_scan_pattern() -> None:
    """PointCloudMetadata._to_conjure produces a point_cloud union arm with mapped fields."""
    conjure = PointCloudMetadata(
        sensor_model="Ouster OS1-128",
        coordinate_system="ENU",
        resolution_mm=10.0,
        scan_pattern=ScanPattern.ROTATING,
    )._to_conjure()
    pc = conjure.point_cloud
    assert pc is not None
    assert pc.sensor_model == "Ouster OS1-128"
    assert pc.coordinate_system == "ENU"
    assert pc.resolution_mm == 10.0
    assert pc.scan_pattern == scout_spatial_api.ScanPattern.ROTATING


def test_point_cloud_metadata_to_conjure_omits_unset_scan_pattern() -> None:
    """A None scan_pattern stays None in the conjure metadata."""
    pc = PointCloudMetadata()._to_conjure().point_cloud
    assert pc is not None
    assert pc.scan_pattern is None


def test_spatial_metadata_from_conjure_reads_point_cloud() -> None:
    """_spatial_metadata_from_conjure maps a point_cloud union back to PointCloudMetadata."""
    conjure = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata(
            sensor_model="Ouster OS1-128",
            scan_pattern=scout_spatial_api.ScanPattern.ROTATING,
        )
    )
    md = _spatial_metadata_from_conjure(conjure)
    assert isinstance(md, PointCloudMetadata)
    assert md.sensor_model == "Ouster OS1-128"
    assert md.scan_pattern == ScanPattern.ROTATING
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/core/test_spatial_asset.py -q -p no:cacheprovider`
Expected: FAIL with `ImportError: cannot import name 'PointCloudMetadata' from 'nominal.core.spatial_asset'`.

- [ ] **Step 3: Add the new types to `nominal/core/spatial_asset.py`**

Add `from enum import Enum` and `from typing import TypeAlias` to the imports. Then add, near the top of the module (after imports, before `SpatialAsset`):

```python
class ScanPattern(Enum):
    """Point-cloud scan pattern, wrapping `nominal_api.scout_spatial_api.ScanPattern`."""

    FLASH = "FLASH"
    MECHANICAL = "MECHANICAL"
    ROTATING = "ROTATING"
    SOLID_STATE = "SOLID_STATE"
    UNKNOWN = "UNKNOWN"

    def _to_conjure(self) -> scout_spatial_api.ScanPattern:
        return _SCAN_PATTERN_TO_CONJURE[self]

    @classmethod
    def _from_conjure(cls, value: scout_spatial_api.ScanPattern) -> ScanPattern:
        return _SCAN_PATTERN_FROM_CONJURE.get(value, cls.UNKNOWN)


_SCAN_PATTERN_TO_CONJURE: Mapping[ScanPattern, scout_spatial_api.ScanPattern] = {
    ScanPattern.FLASH: scout_spatial_api.ScanPattern.FLASH,
    ScanPattern.MECHANICAL: scout_spatial_api.ScanPattern.MECHANICAL,
    ScanPattern.ROTATING: scout_spatial_api.ScanPattern.ROTATING,
    ScanPattern.SOLID_STATE: scout_spatial_api.ScanPattern.SOLID_STATE,
    ScanPattern.UNKNOWN: scout_spatial_api.ScanPattern.UNKNOWN,
}
_SCAN_PATTERN_FROM_CONJURE: Mapping[scout_spatial_api.ScanPattern, ScanPattern] = {
    v: k for k, v in _SCAN_PATTERN_TO_CONJURE.items()
}


@dataclass(frozen=True)
class PointCloudMetadata:
    """Point-cloud-specific metadata for a spatial asset."""

    sensor_model: str | None = None
    coordinate_system: str | None = None
    resolution_mm: float | None = None
    scan_pattern: ScanPattern | None = None

    def _to_conjure(self) -> scout_spatial_api.SpatialTypeMetadata:
        return scout_spatial_api.SpatialTypeMetadata(
            point_cloud=scout_spatial_api.PointCloudMetadata(
                sensor_model=self.sensor_model,
                coordinate_system=self.coordinate_system,
                resolution_mm=self.resolution_mm,
                scan_pattern=None if self.scan_pattern is None else self.scan_pattern._to_conjure(),
            )
        )


SpatialMetadata: TypeAlias = PointCloudMetadata


def _spatial_metadata_from_conjure(type_metadata: scout_spatial_api.SpatialTypeMetadata) -> SpatialMetadata:
    point_cloud = type_metadata.point_cloud
    if point_cloud is None:
        return PointCloudMetadata()
    return PointCloudMetadata(
        sensor_model=point_cloud.sensor_model,
        coordinate_system=point_cloud.coordinate_system,
        resolution_mm=point_cloud.resolution_mm,
        scan_pattern=None if point_cloud.scan_pattern is None else ScanPattern._from_conjure(point_cloud.scan_pattern),
    )


@dataclass(frozen=True)
class DaggerModel:
    """A reference to a created Dagger model plus the uploaded source's location."""

    dagger_uuid: str
    source_handle: str | None = None
```

Ensure `Mapping` and `dataclass`/`field` are imported (they already are for `_clients`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/core/test_spatial_asset.py -q -p no:cacheprovider`
Expected: PASS (3 tests).

- [ ] **Step 5: Export the new types from `nominal/core/__init__.py`**

Add to the imports block (alphabetically near the existing `from nominal.core.spatial_asset import SpatialAsset`):
```python
from nominal.core.spatial_asset import DaggerModel, PointCloudMetadata, ScanPattern, SpatialAsset, SpatialMetadata
```
Add to `__all__` (keep it sorted as the file is): `"DaggerModel"`, `"PointCloudMetadata"`, `"ScanPattern"`, `"SpatialMetadata"` (alongside the existing `"SpatialAsset"`).

- [ ] **Step 6: Repoint ouster's `ScanPattern` to core, delete ouster's local copy**

In `packages/nominal-ouster/nominal/ouster/spatial.py`:
- Delete the local `class ScanPattern(Enum): …` block and the `_SCAN_PATTERN_TO_CONJURE` dict.
- Add import: `from nominal.core.spatial_asset import ScanPattern`.
- Remove `from enum import Enum` if no longer used.

In `packages/nominal-ouster/nominal/ouster/__init__.py`, change the import to keep `ScanPattern` re-exported from core for now:
```python
from nominal.core.spatial_asset import ScanPattern
from nominal.ouster._convert import convert_ouster_dataset
from nominal.ouster.spatial import upload_point_cloud
__all__ = ["ScanPattern", "convert_ouster_dataset", "upload_point_cloud"]
```
(Both `ScanPattern` and `upload_point_cloud` are dropped from ouster in Task 4; this interim keeps everything green.)

- [ ] **Step 7: Verify the whole suite is still green**

Run: `uv run ruff check nominal/core packages/nominal-ouster && uv run mypy && uv run pytest -q -p no:cacheprovider`
Expected: ruff clean; mypy `Success`; pytest all pass.

- [ ] **Step 8: Commit**

```bash
git add nominal/core/spatial_asset.py nominal/core/__init__.py tests/core/test_spatial_asset.py packages/nominal-ouster/nominal/ouster/spatial.py packages/nominal-ouster/nominal/ouster/__init__.py
git commit -m "feat(spatial): add core spatial metadata model + DaggerModel; relocate ScanPattern

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `SpatialAsset.metadata` replaces flat `sensor_model`

**Files:**
- Modify: `nominal/core/spatial_asset.py` (`SpatialAsset` dataclass + `_from_conjure`)
- Test: `tests/core/test_spatial_asset.py`

**Interfaces:**
- Consumes: `SpatialMetadata`, `_spatial_metadata_from_conjure`, `PointCloudMetadata`, `ScanPattern` (Task 1).
- Produces: `SpatialAsset.metadata: SpatialMetadata` (the field `sensor_model` is removed).

- [ ] **Step 1: Write the failing test** (append to `tests/core/test_spatial_asset.py`)

```python
from unittest.mock import MagicMock

from nominal.core.spatial_asset import SpatialAsset


def test_spatial_asset_from_conjure_builds_typed_metadata() -> None:
    """SpatialAsset._from_conjure populates a typed `metadata` from the bean's type_metadata."""
    raw = MagicMock()
    raw.rid = "ri.scout.x.spatial.abc"
    raw.title = "scan"
    raw.description = "d"
    raw.labels = ["lidar"]
    raw.properties = {"k": "v"}
    raw.is_archived = False
    raw.dagger_uuid = "dagger-uuid"
    raw.created_at = 1_700_000_000_000_000_000
    raw.created_by = "ri.user.1"
    raw.type_metadata = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata(
            sensor_model="OS1-128", scan_pattern=scout_spatial_api.ScanPattern.ROTATING
        )
    )

    asset = SpatialAsset._from_conjure(MagicMock(), raw)

    assert asset.metadata == PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING)
    assert not hasattr(asset, "sensor_model")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_spatial_asset.py::test_spatial_asset_from_conjure_builds_typed_metadata -q -p no:cacheprovider`
Expected: FAIL (`SpatialAsset` has `sensor_model`, not `metadata`).

- [ ] **Step 3: Change the `SpatialAsset` field and `_from_conjure`**

In `nominal/core/spatial_asset.py`, in the `SpatialAsset` dataclass, replace the field line `sensor_model: str | None` with `metadata: SpatialMetadata`. In `_from_conjure`, replace the `sensor_model=...` line (the `point_cloud.sensor_model if point_cloud is not None else None` expression) with:
```python
            metadata=_spatial_metadata_from_conjure(raw_spatial.type_metadata),
```
and remove the now-unused `point_cloud = raw_spatial.type_metadata.point_cloud` line.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core/test_spatial_asset.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Verify nothing else read `sensor_model`; run full checks**

Run: `uv run mypy && uv run pytest -q -p no:cacheprovider`
Expected: `Success`; all pass. (If any code referenced `SpatialAsset.sensor_model`, mypy/pytest will flag it — none should, but fix via `.metadata.sensor_model` if so.)

- [ ] **Step 6: Commit**

```bash
git add nominal/core/spatial_asset.py tests/core/test_spatial_asset.py
git commit -m "feat(spatial): expose typed SpatialAsset.metadata instead of flat sensor_model

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `NominalClient.create_spatial_asset`

**Files:**
- Modify: `nominal/core/spatial_asset.py` (add `_create_spatial_asset` helper)
- Modify: `nominal/core/client.py` (add `create_spatial_asset`)
- Test: `tests/core/test_spatial_asset.py`

**Interfaces:**
- Consumes: `DaggerModel`, `SpatialMetadata` (Task 1), `SpatialAsset._from_conjure` (Task 2).
- Produces:
  - `_create_spatial_asset(auth_header, spatial_service, name, *, dagger_model, metadata, description, labels, properties, workspace_rid) -> scout_spatial_api.Spatial`.
  - `NominalClient.create_spatial_asset(name, *, dagger_model: DaggerModel, metadata: SpatialMetadata, description=None, labels=(), properties=None) -> SpatialAsset`.

- [ ] **Step 1: Write the failing test** (append to `tests/core/test_spatial_asset.py`)

```python
from nominal.core.spatial_asset import DaggerModel


def test_create_spatial_asset_builds_request_and_returns_asset() -> None:
    """create_spatial_asset posts a CreateSpatialRequest with the dagger uuid, metadata, and source handle."""
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    clients.resolve_default_workspace_rid.return_value = "ri.scout.x.workspace.w"
    created = MagicMock()
    created.rid = "ri.scout.x.spatial.abc"
    created.title = "scan"
    created.description = "d"
    created.labels = []
    created.properties = {}
    created.is_archived = False
    created.dagger_uuid = "dagger-uuid"
    created.created_at = 1_700_000_000_000_000_000
    created.created_by = "ri.user.1"
    created.type_metadata = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata(sensor_model="OS1-128")
    )
    clients.spatial.create.return_value = created
    nominal_client = MagicMock()
    nominal_client._clients = clients

    from nominal.core.client import NominalClient

    asset = NominalClient.create_spatial_asset(
        nominal_client,
        "scan",
        dagger_model=DaggerModel(dagger_uuid="dagger-uuid", source_handle="s3://bucket/scan.csv"),
        metadata=PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING),
        description="d",
        labels=["lidar"],
        properties={"k": "v"},
    )

    clients.spatial.create.assert_called_once()
    req = clients.spatial.create.call_args.args[1]
    assert req.title == "scan"
    assert req.dagger_uuid == "dagger-uuid"
    assert req.workspace == "ri.scout.x.workspace.w"
    assert req.type_metadata.point_cloud.sensor_model == "OS1-128"
    assert req.type_metadata.point_cloud.scan_pattern == scout_spatial_api.ScanPattern.ROTATING
    assert req.source_handle.s3 == "s3://bucket/scan.csv"
    assert isinstance(asset, SpatialAsset)
    assert asset.rid == "ri.scout.x.spatial.abc"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_spatial_asset.py::test_create_spatial_asset_builds_request_and_returns_asset -q -p no:cacheprovider`
Expected: FAIL (`NominalClient` has no `create_spatial_asset`).

- [ ] **Step 3: Add the `_create_spatial_asset` helper** to `nominal/core/spatial_asset.py`

Add `from nominal_api import api, scout_spatial, scout_spatial_api` (extend the existing `nominal_api` import to include `api` and `scout_spatial`). Add at module level:
```python
def _create_spatial_asset(
    auth_header: str,
    spatial_service: scout_spatial.SpatialService,
    name: str,
    *,
    dagger_model: DaggerModel,
    metadata: SpatialMetadata,
    description: str | None,
    labels: Sequence[str],
    properties: Mapping[str, str] | None,
    workspace_rid: str,
) -> scout_spatial_api.Spatial:
    request = scout_spatial_api.CreateSpatialRequest(
        title=name,
        dagger_uuid=dagger_model.dagger_uuid,
        type_metadata=metadata._to_conjure(),
        labels=list(labels),
        properties=dict(properties) if properties else {},
        marking_rids=[],
        description=description,
        source_handle=None if dagger_model.source_handle is None else api.Handle(s3=dagger_model.source_handle),
        workspace=workspace_rid,
    )
    return spatial_service.create(auth_header, request)
```
Ensure `Sequence` is imported from `typing` (it is, used by other signatures; add if missing).

- [ ] **Step 4: Add `create_spatial_asset` to `NominalClient`** in `nominal/core/client.py`

Add the import `from nominal.core.spatial_asset import DaggerModel, SpatialAsset, SpatialMetadata, _create_spatial_asset` to client.py's import block. Add the method (place near `create_video`):
```python
    def create_spatial_asset(
        self,
        name: str,
        *,
        dagger_model: DaggerModel,
        metadata: SpatialMetadata,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> SpatialAsset:
        """Create a spatial asset referencing an existing Dagger model.

        Args:
            name: Human-readable name for the spatial asset.
            dagger_model: The Dagger model (uuid + optional source handle) the asset references.
            metadata: Type-specific metadata, e.g. `PointCloudMetadata(...)`.
            description: Optional description.
            labels: Labels to apply.
            properties: Key-value properties to apply.

        Returns:
            The created spatial asset.
        """
        response = _create_spatial_asset(
            self._clients.auth_header,
            self._clients.spatial,
            name,
            dagger_model=dagger_model,
            metadata=metadata,
            description=description,
            labels=labels,
            properties=properties,
            workspace_rid=self._clients.resolve_default_workspace_rid(),
        )
        return SpatialAsset._from_conjure(self._clients, response)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/core/test_spatial_asset.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 6: Full checks**

Run: `uv run ruff check nominal/core && uv run mypy && uv run pytest -q -p no:cacheprovider`
Expected: clean / `Success` / all pass.

- [ ] **Step 7: Commit**

```bash
git add nominal/core/spatial_asset.py nominal/core/client.py tests/core/test_spatial_asset.py
git commit -m "feat(spatial): add NominalClient.create_spatial_asset

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: ouster `create_dagger_model` replaces `upload_point_cloud`

**Files:**
- Modify: `packages/nominal-ouster/nominal/ouster/spatial.py`
- Modify: `packages/nominal-ouster/nominal/ouster/__init__.py`
- Test: `packages/nominal-ouster/tests/test_spatial.py`

**Interfaces:**
- Consumes: `DaggerModel` (Task 1).
- Produces: `create_dagger_model(client: NominalClient, csv_path: PathLike, *, column_types: Mapping[str, ColumnDataType] | None = None) -> DaggerModel`. Removes `upload_point_cloud` and the ouster `ScanPattern` re-export.

- [ ] **Step 1: Rewrite the upload tests** in `packages/nominal-ouster/tests/test_spatial.py`

Replace the `uploaded` fixture, `_UploadResult`, and every `test_upload_point_cloud_*` test with tests of `create_dagger_model`. Keep the existing `make_clients` and `dagger_mocks` fixtures (they already patch `upload_multipart_file`, `AuthenticatedClient`, `put_object_space.sync_detailed`, `post_import.sync_detailed`). Drop the `CreateSpatialRequest` patch from `dagger_mocks` (no longer used) and the `scout_spatial_api`/`SpatialAsset`/`NominalIngestError`-on-create imports that are now unused. Replace with:

```python
@dataclass
class _DaggerMocks:
    upload_multipart_file: MagicMock
    authenticated_client: MagicMock
    put_object_space: MagicMock
    post_import: MagicMock


@pytest.fixture
def dagger_mocks() -> Iterator[_DaggerMocks]:
    """Patch the upload + dagger integration so create_dagger_model runs without network."""
    with (
        patch.object(spatial, "upload_multipart_file", return_value=_FAKE_S3_PATH) as upload,
        patch.object(spatial, "AuthenticatedClient") as authenticated_client,
        patch("nominal.ouster.spatial.put_object_space.sync_detailed", return_value=MagicMock(status_code=200)) as put,
        patch("nominal.ouster.spatial.post_import.sync_detailed", return_value=MagicMock(status_code=202)) as post,
    ):
        yield _DaggerMocks(upload, authenticated_client, put, post)


def test_create_dagger_model_returns_uuid_and_source_handle(
    tmp_path: Path,
    make_clients: Callable[..., tuple[MagicMock, MagicMock]],
    dagger_mocks: _DaggerMocks,
) -> None:
    """create_dagger_model uploads the CSV, imports it, and returns the model uuid + source handle."""
    csv_path = tmp_path / "ouster.csv"
    csv_path.write_text("x,y,z,time,reflectivity\n1.0,2.0,3.0,1700000000.0,42\n")
    clients, workspace = make_clients()
    nominal_client = MagicMock()
    nominal_client._clients = clients

    model = spatial.create_dagger_model(nominal_client, csv_path)

    assert model.source_handle == _FAKE_S3_PATH
    # dagger_uuid is the model_uuid passed to the import POST
    assert model.dagger_uuid == str(dagger_mocks.post_import.call_args.kwargs["model_uuid"])
    dagger_mocks.upload_multipart_file.assert_called_once()
    import_request = dagger_mocks.post_import.call_args.kwargs["body"]
    assert import_request.source_uri == _PRESIGNED_URL
    assert import_request.columns.geometry == [0, 1, 2]
    assert import_request.columns.real == [3]
    assert import_request.columns.int_ == [4]


def test_create_dagger_model_raises_on_missing_file() -> None:
    """create_dagger_model raises FileNotFoundError when the CSV path does not exist."""
    with pytest.raises(FileNotFoundError):
        spatial.create_dagger_model(MagicMock(), "/no/such/path.csv")


def test_create_dagger_model_propagates_dagger_failure(
    tmp_path: Path,
    make_clients: Callable[..., tuple[MagicMock, MagicMock]],
    dagger_mocks: _DaggerMocks,
) -> None:
    """Raises NominalIngestError when the dagger import endpoint returns a non-202 status."""
    csv_path = tmp_path / "ouster.csv"
    csv_path.write_text("x,y,z,t\n1,2,3,4\n")
    clients, _ = make_clients()
    nominal_client = MagicMock()
    nominal_client._clients = clients
    dagger_mocks.post_import.return_value = MagicMock(status_code=500, content=b"upstream broken")

    with pytest.raises(NominalIngestError, match="Dagger POST"):
        spatial.create_dagger_model(nominal_client, csv_path)
```
Keep the `NominalIngestError` import (used by the failure test) and the `_FAKE_S3_PATH`/`_PRESIGNED_URL` constants. Keep all the `_build_archetype`/`_classify`/`_dagger_base_url`/`_read_csv_header_and_samples`/`_extract_rid_locator_uuid`/`_scan_pattern*`→ note: the `_scan_pattern_to_conjure`/`ScanPattern` tests move with the enum; delete any `ScanPattern`-from-ouster usage here (covered in core tests now).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest packages/nominal-ouster/tests/test_spatial.py -q -p no:cacheprovider`
Expected: FAIL (`spatial.create_dagger_model` does not exist).

- [ ] **Step 3: Refactor `upload_point_cloud` → `create_dagger_model`** in `packages/nominal-ouster/nominal/ouster/spatial.py`

Replace the `upload_point_cloud` function (its signature through the final `return`) with:
```python
def create_dagger_model(
    client: NominalClient,
    csv_path: PathLike,
    *,
    column_types: Mapping[str, ColumnDataType] | None = None,
) -> DaggerModel:
    """Upload a point-cloud CSV and import it into a Dagger model.

    The CSV must contain at minimum x, y, z columns (case-insensitive); remaining columns
    are auto-classified as int/real/string by sampling the first ~1000 data rows. Pass
    ``column_types`` to override inference for specific columns.

    Returns:
        A `DaggerModel` referencing the created model (uuid + the uploaded CSV's s3 source handle).

    Raises:
        FileNotFoundError: If ``csv_path`` does not exist.
        NominalIngestError: If the Dagger object-space or import request fails.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    clients = client._clients

    workspace = clients.resolve_workspace(None)
    workspace_rid = workspace.rid
    object_space = _extract_rid_locator_uuid(workspace_rid)
    tenant = _extract_rid_locator_uuid(workspace.org)

    s3_path = upload_multipart_file(
        clients.auth_header,
        workspace_rid,
        path,
        clients.upload,
        file_type=FileTypes.CSV,
        header_provider=clients.header_provider,
    )

    presigned_url = _presign_download(clients, s3_path)

    header_line, sample_lines = _read_csv_header_and_samples(path)
    columns, archetype = _build_archetype(header_line, sample_lines, column_types or {})

    token = clients.auth_header.removeprefix("Bearer ")
    dagger_client = AuthenticatedClient(base_url=_dagger_base_url(clients), token=token)

    put_resp = put_object_space.sync_detailed(
        id=object_space,
        body=PutObjectSpaceRequest(),
        tenant=tenant,
        client=dagger_client,
    )
    if put_resp.status_code >= 400:
        raise NominalIngestError(
            f"Dagger PUT /v1/object-spaces failed: status={put_resp.status_code} body={put_resp.content!r}"
        )

    model_uuid = uuid.uuid4()
    import_request = ImportRequest(
        archetype=archetype,
        columns=columns,
        geometry_type=GeometryType.POINT,
        source_uri=presigned_url,
    )
    import_resp = post_import.sync_detailed(
        model_uuid=model_uuid,
        body=import_request,
        tenant=tenant,
        object_space=object_space,
        client=dagger_client,
    )
    if import_resp.status_code != 202:
        raise NominalIngestError(
            f"Dagger POST /v1/imports/{model_uuid} failed: "
            f"status={import_resp.status_code} body={import_resp.content!r}"
        )

    return DaggerModel(dagger_uuid=str(model_uuid), source_handle=s3_path)
```

Then in the same file:
- Add `from nominal.core.spatial_asset import DaggerModel` (and keep `from nominal.core.spatial_asset import ScanPattern` removed — `ScanPattern` is no longer used here).
- Remove now-unused imports: `scout_spatial_api` (if only used by the deleted metadata/create code — verify with ruff), `api` (Handle), and the `SpatialAsset` import. Keep `ingest_api` (used by `_presign_download`), `dagger_client` imports, `ColumnDataType`, `_build_archetype` and friends.
- Update the `ColumnDataType` comment that referenced `upload_point_cloud` to `create_dagger_model`.

- [ ] **Step 4: Update ouster `__init__.py`** (`packages/nominal-ouster/nominal/ouster/__init__.py`)

```python
from nominal.ouster._convert import convert_ouster_dataset
from nominal.ouster.spatial import create_dagger_model

__all__ = ["convert_ouster_dataset", "create_dagger_model"]
```
(`ScanPattern` is now imported from `nominal.core` by callers; `upload_point_cloud` is gone.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest packages/nominal-ouster/tests/test_spatial.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 6: Full checks (ruff catches unused imports)**

Run: `uv run ruff check packages/nominal-ouster && uv run ruff format --check packages/nominal-ouster nominal/core && uv run mypy && uv run pytest -q -p no:cacheprovider`
Expected: ruff clean (fix any unused-import findings it reports in `spatial.py`); mypy `Success`; full suite passes.

- [ ] **Step 7: Commit**

```bash
git add packages/nominal-ouster/nominal/ouster/spatial.py packages/nominal-ouster/nominal/ouster/__init__.py packages/nominal-ouster/tests/test_spatial.py
git commit -m "feat(ouster): replace upload_point_cloud with create_dagger_model

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:** Boundary (Task 1+3+4), metadata model + `ScanPattern` relocation + typing principle (Task 1), `DaggerModel` in core spatial file (Task 1), `SpatialAsset.metadata` (Task 2), `create_spatial_asset` mandatory kw-only `dagger_model`+`metadata` (Task 3), `create_dagger_model -> DaggerModel` + `ColumnDataType` stays + Dagger only in ouster (Task 4), exports (Tasks 1 & 4). Out-of-scope items (mesh variants, `search_spatial_assets`, Run/Asset scope tests) intentionally excluded — covered by the extensibility scaffold.

**Placeholder scan:** No TBD/TODO; every code step shows full code; the only "verify/fix if reported" notes are for ruff unused-import cleanup, which is deterministic and self-checking.

**Type consistency:** `DaggerModel(dagger_uuid, source_handle)`, `PointCloudMetadata` fields, `ScanPattern._to_conjure`/`_from_conjure`, `_spatial_metadata_from_conjure`, `_create_spatial_asset`, and `create_spatial_asset`/`create_dagger_model` signatures are used identically across tasks. `SpatialAsset.metadata` (Task 2) is consumed only by tests; Run/Asset code never referenced `sensor_model`.
