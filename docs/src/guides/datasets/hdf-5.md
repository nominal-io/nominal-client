---
myst:
  html_meta:
    description: "Read arrays from an HDF5 file with h5py and upload them to a dataset in batches."
---

# HDF5

{.lead}
HDF5 files have no fixed layout, so you read the arrays you need with [h5py](https://docs.h5py.org/), shape them into columns and upload them as tables.
Reading in slices keeps files larger than memory out of RAM.

The examples also need `pip install h5py`.

```{include} /guides/_snippets/connect-the-client.md
```

## Inspect the file

This example writes a small sample file with one group of three sensor arrays:

```python
import h5py
import numpy as np

with h5py.File("sample.h5", "w") as f:
    group = f.create_group("sensors/imu")
    group["time_s"] = np.arange(10_000) / 100  # 100 Hz
    group["accel_x"] = np.random.randn(10_000)
    group["accel_y"] = np.random.randn(10_000)
```

List the file's groups and datasets to find the arrays to upload:

```python
with h5py.File("sample.h5") as f:
    f.visititems(lambda name, obj: print(name, getattr(obj, "shape", "")))
```

```text
sensors
sensors/imu
sensors/imu/accel_x (10000,)
sensors/imu/accel_y (10000,)
sensors/imu/time_s (10000,)
```

## Upload in batches

Read a slice of each array into a pandas DataFrame and upload it with {py:func}`~nominal.thirdparty.pandas.upload_dataframe_to_dataset`.
Each batch adds to the same {term}`dataset`.

```python
from datetime import datetime, timezone

import pandas as pd

from nominal import ts
from nominal.thirdparty.pandas import upload_dataframe_to_dataset

dataset = client.create_dataset("IMU from HDF5")
recording_start = datetime(2026, 9, 10, 14, 38, tzinfo=timezone.utc)
batch_size = 2_500

with h5py.File("sample.h5") as f:
    imu = f["sensors/imu"]
    for i in range(0, imu["time_s"].shape[0], batch_size):
        batch = pd.DataFrame({name: imu[name][i : i + batch_size] for name in ["time_s", "accel_x", "accel_y"]})
        upload_dataframe_to_dataset(
            dataset,
            batch,
            timestamp_column="time_s",
            timestamp_type=ts.Relative("seconds", start=recording_start),
        )
```

`upload_dataframe_to_dataset` waits for each batch to finish ingesting.
Multidimensional arrays need flattening into one column per element first, for example with `array[:, j]`.
For timestamp options, see [Timestamp formats](/guides/datasets/csv.md#timestamp-formats).
