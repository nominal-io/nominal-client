"""Upload 3D point clouds and align them with a run or asset on a timeline.

A spatial holds 3D data. Create one empty with :func:`create_point_cloud_spatial`, then fill it from
a CSV with :meth:`Spatial.add_point_cloud_csv`.

This package is experimental. It lives outside `nominal.core` and is not covered by the stability
guarantees of the core namespace.

# CSV format

- `x`, `y`, `z` columns are required and are matched case-insensitively. Every other column becomes
  an attribute.
- Column names given as `rgb_column` and `timestamp_column` are matched exactly.
- Quoted fields are rejected anywhere in the file, including the header. Rows are split on raw
  commas, so a quoted field shifts the position of every column after it. Every row is checked.
- Coordinates must be in a single cartesian frame. Latitude and longitude in `x`/`y` render as a
  near-flat sheet; project to a local metric frame and carry lat/lon as attribute columns.

# Column types

A column's type determines its reductions, and an attribute can drive colour or filtering only
through its reductions.

| Type | Assigned when | Reductions | Ramp colour | Range filter |
| --- | --- | --- | --- | --- |
| `real` | every sampled value is numeric | Min, Max, Mean | yes | yes |
| `int` | requested with `column_types` | Min, Max | yes | yes |
| `string` | any sampled value is non-numeric | none | no | no |
| colour | named with `rgb_column` | Mean | is the colour | no |

Types are inferred from the first 1000 data rows. `int` is never inferred, because an int-typed
attribute truncates every float in the column; request it with `column_types={"ring": "int"}`. A
column with no values in the sample is typed `string`.

Attribute order follows the type walk -- real, then int, then string, then colour -- not the order of
the columns in the header.

# Choosing attributes

A cloud renders in a single flat colour unless it carries at least one attribute that can drive
colour: a `real` or `int` column whose values vary, or a colour column.

Suitable attributes include intensity, reflectivity, range, return number, ring or laser id typed as
`int`, radial velocity, per-point time, and semantic class encoded as an integer rather than a text
label. A `string` column cannot colour or filter. A column holding one value across the whole scan
colours uniformly; bucket or normalise a value with an extreme distribution before uploading it.

# Colour

`rgb_column` names one column whose cells are exactly six hexadecimal digits, `rrggbb`, with no
leading `#`. Cells in any other form are not readable as colour and render black. Three separate
0-255 columns cannot drive colour.

# Per-point time

Naming a time column records the cloud's extent on the spatial, which is what lets a workbook's
playhead drive it. Without it the whole cloud renders at once.

```py
from datetime import datetime, timezone
from nominal.ts import Relative

job = spatial.add_point_cloud_csv(
    "scan.csv",
    timestamp_column="t_s",
    timestamp_type=Relative("seconds", start=datetime(2026, 3, 4, 9, 30, tzinfo=timezone.utc)),
)
```

`timestamp_column` and `timestamp_type` must be given together.

`timestamp_type` must be `Relative`. Filtering runs on the GPU in f32, where the spacing between
representable values at epoch magnitude exceeds two minutes, so absolute timestamps cannot be used.
The unit must be `nanoseconds`, `microseconds`, `milliseconds` or `seconds`.

`Relative.start` is the instant at which the column reads zero, not the instant of its first value.
A column running 9..180 begins nine seconds after its start.

# Example

```py
from datetime import timedelta

from nominal.core import NominalClient
from nominal.experimental.spatial import (
    PointCloudMetadata,
    ScanPattern,
    add_spatial_to_asset,
    create_point_cloud_spatial,
)

client = NominalClient.from_profile("my-profile")
asset = client.create_asset("downtown-survey")

spatial = create_point_cloud_spatial(
    client,
    "downtown-scan",
    metadata=PointCloudMetadata(
        sensor_model="OS1-128",
        coordinate_system="ENU",
        resolution_mm=10.0,
        scan_pattern=ScanPattern.ROTATING,
    ),
)

job = spatial.add_point_cloud_csv("scan.csv", rgb_column="color")

# Blocks until the import finishes, raises if it failed, and times out rather than hanging. An empty
# result means no catalog entry was produced, not that no points were ingested.
list(job.as_files_ingested(timeout=timedelta(minutes=30)))

add_spatial_to_asset(asset, "point_cloud", spatial)
```

Use the same data scope name for the same kind of spatial across assets. Checklists and templates
address data by scope name.

# Troubleshooting

| Symptom | Cause | Resolution |
| --- | --- | --- |
| Every point is black | colour cells are not six hex digits | use one `rrggbb` column, no `#` |
| Cloud renders in one flat colour | attribute has no reductions, or does not vary | add a varying `real`/`int` column |
| Cloud renders as a flat sheet | latitude/longitude used as `x`/`y` | project to a local metric frame |
| Whole cloud renders at once | ingested without a time column | re-ingest into a new spatial |
| Job does not finish | poll loop ends only on completed or failed | use `job.as_files_ingested(timeout=...)` |
| Points are duplicated | an ingest was retried | create a new spatial |
| Playhead sweeps the wrong range | `Relative.start` given the first value | pass the instant it reads zero |
| Properties missing after an update | `update(properties=...)` replaces the map | read, merge, then write |
| Spatial visible to the whole workspace | no `markings` set at creation | recreate with `markings=[...]` |
| `list_data_scopes()` omits the spatial | it returns only core scope kinds | use :func:`list_spatials_in_asset` |

# Limitations

- Point clouds are ingested from CSV only.
- An ingest is additive and cannot be undone. Retrying after a partial failure duplicates every
  point rather than resuming it.
- Points cannot be replaced or removed once ingested.
- The time range cannot be set or corrected after an ingest. If recording it fails, the failure is
  logged and the range is left unset.
- `markings` can only be set when the spatial is created.
- `PointCloudMetadata.coordinate_system` is a free-form label. It is not validated, and no
  transform is applied to the points.
- Removing a spatial from an asset requires its rid, passed to `Asset.remove_data_scopes`.
"""

from nominal.experimental.spatial._point_cloud import ColumnDataType
from nominal.experimental.spatial._scopes import (
    add_spatial_to_asset,
    add_spatial_to_run,
    get_spatial_from_asset,
    get_spatial_from_run,
    list_spatials_in_asset,
    list_spatials_in_run,
)
from nominal.experimental.spatial._spatial import (
    PointCloudMetadata,
    ScanPattern,
    Spatial,
    create_point_cloud_spatial,
    get_spatial,
)

__all__ = [
    "ColumnDataType",
    "PointCloudMetadata",
    "ScanPattern",
    "Spatial",
    "add_spatial_to_asset",
    "add_spatial_to_run",
    "create_point_cloud_spatial",
    "get_spatial",
    "get_spatial_from_asset",
    "get_spatial_from_run",
    "list_spatials_in_asset",
    "list_spatials_in_run",
]
