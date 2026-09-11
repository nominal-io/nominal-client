# Nominal Spatials

Spatials hold 3D data — today, point clouds — and line them up against the rest of a run or asset on a
timeline. This package is experimental: the shape of the API is still moving, so it lives outside
`nominal.core` and nothing here is covered by the stability guarantees the core namespace carries.

## Creating a spatial and adding a point cloud

A spatial is created empty and then filled. Creating it reserves the model that will hold the data;
adding a CSV uploads the file and submits it to the import pipeline.

```py
from nominal.core import NominalClient
from nominal.experimental.spatial import PointCloudMetadata, ScanPattern, create_point_cloud_spatial

client = NominalClient.from_profile("my-profile")

spatial = create_point_cloud_spatial(
    client,
    "downtown-scan",
    metadata=PointCloudMetadata(
        sensor_model="Ouster OS1-128",
        coordinate_system="ENU",
        resolution_mm=10.0,
        scan_pattern=ScanPattern.ROTATING,
    ),
)

job = spatial.add_point_cloud_csv("scan.csv")
```

The CSV needs `x`, `y`, `z` columns (matched case-insensitively). Every other column becomes an
attribute you can colour or filter by in a workbook's 3D panel.

The import runs asynchronously, so `add_point_cloud_csv` returns as soon as the ingest is *accepted*.
Poll the job to wait for it:

```py
import time

from nominal.core.ingestion_job import IngestionJobStatus

while job.status not in (IngestionJobStatus.COMPLETED, IngestionJobStatus.FAILED):
    time.sleep(2)
    job = job.refresh()
```

`status` is the only signal the job carries. A point-cloud import writes into the spatial's own model
rather than the catalog, so `job.dataset_rid`, `job.produced_file_count` and `job.dataset_files()` stay
empty — an empty `as_files_ingested()` means there is no catalog entry, not that there are no points.

## Column types

Columns are classified by sampling the first ~1000 data rows. Anything numeric becomes a `real`
attribute; anything else becomes `string`.

`int` is never inferred, deliberately. A sample that looks integral is no evidence the rest of the
column is, and an integer-typed attribute silently truncates every float the importer reads into it
— for the whole file. Ask for it explicitly when you know a column holds integers:

```py
job = spatial.add_point_cloud_csv("scan.csv", column_types={"ring": "int", "gear": "int"})
```

Quoted CSVs are rejected rather than parsed. The importer splits rows on raw commas with no quote
handling, so honouring quotes here would compute column indices it never reads, shifting every
attribute after the quoted field. Every row is checked, not just the sampled ones.

## Per-point colour

Colour comes from **one** column holding a six-character hex string — `rrggbb`, no leading `#`:

```py
job = spatial.add_point_cloud_csv("scan.csv", rgb_column="color")
```

Three separate 0–255 columns will not work. The importer reads the cell as three 2-character hex
slices and skips it entirely unless it is exactly six characters, so every point ends up black
without an error.

## Point clouds captured over time

A cloud built or captured over time can be driven from a workbook's playhead, but only if the spatial
knows where its data sits on the wall clock. Name the column holding per-point time and say how to
read it:

```py
from datetime import datetime, timezone
from nominal.ts import Relative

job = spatial.add_point_cloud_csv(
    "scan.csv",
    timestamp_column="t_s",
    timestamp_type=Relative("seconds", start=datetime(2026, 3, 4, 9, 30, tzinfo=timezone.utc)),
)
```

Only `Relative` is accepted: per-point time has to be an offset from a start instant. Filtering happens
on the GPU in f32, where the spacing between representable values at epoch magnitude is over two
minutes — absolute timestamps cannot survive it.

`timestamp_column` and `timestamp_type` go together; passing one without the other is an error. Reading
the column costs one extra pass over the file, so it is only read when you name it. Without it, the
panel has no way to map playhead position onto per-point time and renders the whole cloud at once.

## Attaching to runs and assets

`Asset` and `Run` do not know about spatials — the type is experimental — so these are functions taking
the resource rather than methods on it:

```py
from nominal.experimental.spatial import (
    add_spatial_to_asset,
    get_spatial_from_asset,
    list_spatials_in_asset,
)

add_spatial_to_asset(asset, "cloud", spatial)

same = get_spatial_from_asset(asset, "cloud")
for data_scope_name, attached in list_spatials_in_asset(asset):
    print(data_scope_name, attached.rid)
```

`add_spatial_to_run`, `get_spatial_from_run` and `list_spatials_in_run` are the same three for a run,
keyed by ref name instead of data scope name.

Note that `Asset.list_data_scopes()` does **not** include spatials: its return type covers only the
scope kinds core knows about. Use `list_spatials_in_asset` alongside it.

## Reading and updating

```py
from nominal.experimental.spatial import get_spatial

spatial = get_spatial(client, "ri.scout.my-stack.spatial.<uuid>")

spatial.update(name="renamed-scan", labels=["lidar"], properties={"site": "downtown"})

spatial.archive()    # hides it from search; reversible
spatial.unarchive()
```

`update` replaces only the fields you pass and leaves the rest untouched. Archiving is a
search-visibility flag rather than a write lock — an archived spatial still accepts an ingest.
