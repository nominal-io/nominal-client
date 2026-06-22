# Spatial Asset Package-Boundary Refactor — Design

## Goal

Cleanly separate responsibilities between the `nominal-ouster` package and the core
`nominal-client` for spatial (point-cloud) assets.

Today `nominal.ouster.upload_point_cloud` conflates three concerns: converting/uploading a
CSV, creating a Dagger model, **and** creating the core `SpatialAsset` (reaching into the
core spatial service). This refactor splits them along a clean boundary.

## Ownership / boundary

- **`nominal-ouster` owns:**
  - `convert_ouster_dataset(...)` — Ouster file(s) → clean `.csv` (unchanged).
  - `create_dagger_model(client, csv_path, ...) -> DaggerModel` — CSV → a Dagger model.
  - The `dagger_client` library and all Dagger orchestration live **only** here.
  - It never references `SpatialAsset` / `create_spatial_asset`.
- **core (`nominal-client`) owns:**
  - The `SpatialAsset` domain object and its creation from a `DaggerModel`.
  - The typed spatial metadata model (`SpatialMetadata` / `PointCloudMetadata` / `ScanPattern`).
  - The `DaggerModel` value object (a pure data type — **no `dagger_client` dependency**).
  - `NominalClient.create_spatial_asset(...)`.
- **Composition (caller):**
  ```python
  csv = convert_ouster_dataset(dataset_dir)[0]
  model = nominal.ouster.create_dagger_model(client, csv)
  asset = client.create_spatial_asset(
      "downtown-scan",
      dagger_model=model,
      metadata=PointCloudMetadata(sensor_model="Ouster OS1-128", scan_pattern=ScanPattern.ROTATING),
  )
  ```

## Core additions — `nominal/core/spatial_asset.py`

All of the following live in the existing `nominal/core/spatial_asset.py`.

### `ScanPattern` (moved here from `nominal-ouster`)
`enum.Enum` wrapping `scout_spatial_api.ScanPattern`: `FLASH`, `MECHANICAL`, `ROTATING`,
`SOLID_STATE`, `UNKNOWN`, with `_to_conjure()`.

### Spatial metadata model
```python
@dataclass(frozen=True)
class PointCloudMetadata:
    sensor_model: str | None = None        # API: free str — no enum
    coordinate_system: str | None = None   # API: free str — no enum
    resolution_mm: float | None = None     # API: float
    scan_pattern: ScanPattern | None = None  # API: scout_spatial_api.ScanPattern enum
    def _to_conjure(self) -> scout_spatial_api.SpatialTypeMetadata: ...

SpatialMetadata: TypeAlias = PointCloudMetadata   # extensible union; grows: | MeshMetadata | ...
```
- A module-level `_spatial_metadata_from_conjure(type_metadata) -> SpatialMetadata` dispatches the
  conjure `SpatialTypeMetadata` union to the matching dataclass. Today the union has only the
  `point_cloud` variant; if the variant is missing/unrecognized it falls back to an empty
  `PointCloudMetadata()`. New API variants are absorbed by adding a dataclass + a dispatch arm.
- **Typing principle (aligned to the API):** use enums/`Literal` only where the API constrains
  the value. The spatial API's only metadata-field enum is `ScanPattern`. `sensor_model` and
  `coordinate_system` are free `str` in the conjure model, so they stay `str | None` (an enum
  would over-constrain and reject backend-valid values). The type *discriminator* is the metadata
  class identity (`PointCloudMetadata` ⇄ the `point_cloud` arm) — `SpatialType` is never surfaced
  as a raw string.

### `DaggerModel` (new value object)
```python
@dataclass(frozen=True)
class DaggerModel:
    dagger_uuid: str
    source_handle: str | None = None   # s3 location of the uploaded source, for provenance
```
Pure data (strings only): no `dagger_client` dependency. Its two fields map 1:1 onto the
`Spatial` bean's `dagger_uuid` and `source_handle`.

### `SpatialAsset` change
Replace the flat `sensor_model` field with **`metadata: SpatialMetadata`**. `_from_conjure`
builds `metadata` via `_spatial_metadata_from_conjure(bean.type_metadata)`. `refresh` / `update`
/ `archive` / `unarchive` are unchanged (`update` keeps its current name/description/labels/
properties surface; metadata updates are out of scope).

## Core — `NominalClient.create_spatial_asset`

```python
def create_spatial_asset(
    self,
    name: str,
    *,
    dagger_model: DaggerModel,      # mandatory, keyword-only
    metadata: SpatialMetadata,      # mandatory, keyword-only
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
) -> SpatialAsset: ...
```
- Builds `scout_spatial_api.CreateSpatialRequest(title=name, dagger_uuid=dagger_model.dagger_uuid,
  type_metadata=metadata._to_conjure(), labels=list(labels), properties=dict(properties or {}),
  marking_rids=[], description=description, source_handle=api.Handle(s3=dagger_model.source_handle)
  if dagger_model.source_handle else None, workspace=<resolved default workspace rid>)`.
- Workspace auto-resolved like other `create_*` methods. Touches only the scout `SpatialService`
  (no Dagger).
- Returns `SpatialAsset._from_conjure(clients, response)`.

## `nominal-ouster` — `create_dagger_model` (replaces `upload_point_cloud`)

```python
def create_dagger_model(
    client: NominalClient,
    csv_path: PathLike,
    *,
    column_types: Mapping[str, ColumnDataType] | None = None,
) -> DaggerModel: ...
```
- This is today's `upload_point_cloud` **minus** the `SpatialAsset` creation and the metadata
  params. It keeps: CSV header/sample read, column inference (`_build_archetype`, `_classify`,
  etc.), multipart upload → s3, presign, Dagger `AuthenticatedClient`, object-space `PUT`, import
  `POST` (`geometry_type=POINT`, fresh `model_uuid`).
- Returns `DaggerModel(dagger_uuid=str(model_uuid), source_handle=<s3 path>)`.
- Raises `NominalIngestError` on Dagger failures (unchanged).
- `ColumnDataType` (the `Literal["int","real","string"]`) **stays in ouster** — it describes the
  Dagger import schema, a dagger-model concern.
- Imports `DaggerModel` from core.

## What moves / is removed

- `ScanPattern` enum: **`nominal-ouster` → core** (`nominal/core/spatial_asset.py`).
- `upload_point_cloud` (→ `SpatialAsset`): **removed**, replaced by `create_dagger_model`
  (→ `DaggerModel`).
- `nominal.ouster.__all__`: `convert_ouster_dataset`, `create_dagger_model` (drop
  `upload_point_cloud`, `ScanPattern`).
- core `nominal/core/__init__.py` `__all__`: add `ScanPattern`, `PointCloudMetadata`,
  `SpatialMetadata`, `DaggerModel` (`SpatialAsset` already exported).
- `SpatialAsset.sensor_model` flat field → `SpatialAsset.metadata`.

## Impact on existing code & tests

- **Run/Asset spatial-scope methods** (`add_spatial` / `get_spatial` / `list_spatials`, remove
  filters): functionally unaffected — they pass `SpatialAsset` instances around and don't touch
  the metadata. No changes beyond imports remaining valid.
- **`nominal-ouster` tests** (`tests/test_spatial.py`): substantially updated — `upload_point_cloud`
  → `create_dagger_model` returning a `DaggerModel`; assertions move to `dagger_uuid` /
  `source_handle`; `SpatialAsset` / `sensor_model` assertions removed. Any `ScanPattern` import
  switches from `nominal.ouster` to core.
- **`create_spatial_asset` unit test**: add a focused unit test (mock the spatial service; assert
  the `CreateSpatialRequest` fields incl. `type_metadata` from `metadata._to_conjure()`, and the
  returned `SpatialAsset` mapping).

## Out of scope / future

- **New spatial metadata variants** (mesh, occupancy grid, …): the `SpatialMetadata` union +
  dispatch is built to absorb them — add a dataclass + `_to_conjure` + a dispatch arm;
  `create_spatial_asset` is untouched.
- **`search_spatial_assets`** (would use a `SortField`-aligned enum for ordering) — separate
  feature.
- **Richer provenance / typed `Handle`**: `source_handle` is carried as the uploaded CSV's s3 path
  via `DaggerModel`; a fuller typed `Handle` union and broader provenance modeling are deferred.
- **Run/Asset spatial-scope tests**: previously deferred for lack of a lightweight spatial-asset
  creator; `create_spatial_asset` now makes constructing one straightforward, so these become
  feasible to add in a follow-up.
