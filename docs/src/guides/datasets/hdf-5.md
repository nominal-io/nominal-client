---
myst:
  html_meta:
    description: "Load an HDF5 file in Python and upload it to the Nominal platform"
---

# Load an HDF5 file in Python

{.lead}
Load an HDF5 file in Python and upload it to the Nominal platform

This guide describes an automatable pipeline for uploading HDF5 files to Nominal.

[HDF5](https://en.wikipedia.org/wiki/Hierarchical_Data_Format) (Hierarchical Data Format V5) is a highly flexible binary file format that stores data contiguously in memory, making memory-mapping highly effective.

It's frequently used for storing numerical data, and can be manipulated using both the `h5py` library, as well as [numpy](https://docs.h5py.org/en/stable/high/dataset.html) and [vaex](https://vaex.readthedocs.io/en/docs/getting_data_in_vaex.html#producing-a-hdf5-file).

Because hdf5 (.h5) files are flexible, the processing logic changes per file. This tutorial covers how to inspect an
hdf5 file's layout and walks through a specific example of uploading data from it.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Download sample data

For convenience and educational purposes, Nominal hosts test sample data on the [Nominal Hugging Face](https://huggingface.co/nominal-io) account.

Download a 3D hdf5 (h5) file containing x, y, and z coordinates from the hub.

Install the packages used in this example: `pip install nominal h5py huggingface_hub`.

```python
from huggingface_hub import hf_hub_download

repo_id = "nominal-io/hdf5-sample"
filename = "3d.h5"

local_path = hf_hub_download(
    repo_id=repo_id,
    filename=filename,
    repo_type="dataset"
)
```

`local_path` now contains the path to the hdf5 file.

To inspect the file's layout and find where the data is and its schema, use the `local_path` from the download command above:
```python

import h5py

def print_structure(name, obj):
    if isinstance(obj, h5py.Group):
        print(f"Group: {name}")
    elif isinstance(obj, h5py.Dataset):
        print(f"Dataset: {name}, Shape: {obj.shape}, Type: {obj.dtype}")

with h5py.File(local_path, 'r') as f:
    f.visititems(print_structure)
```

The output shows the layout of the file, with data residing in `data/1/meshes/B` in "columns" x, y, and z. Each of these
columns is a 3D matrix of shape (47, 47, 47). Column E also contains x, y, and z, but those columns are empty, so there is nothing to upload.
```
Group: data
Group: data/1
Group: data/1/meshes
Group: data/1/meshes/B
Dataset: data/1/meshes/B/x, Shape: (47, 47, 47), Type: float64
Dataset: data/1/meshes/B/y, Shape: (47, 47, 47), Type: float64
Dataset: data/1/meshes/B/z, Shape: (47, 47, 47), Type: float64
Group: data/1/meshes/E
Group: data/1/meshes/E/x
Group: data/1/meshes/E/y
Group: data/1/meshes/E/z
```

## Upload your data to Nominal

The right upload approach depends on the use case. A benefit of hdf5, beyond flexibility, is its ease of indexing and slicing.
Slicing in batches avoids bringing the entire dataset into memory and allows uploading files larger than memory.

The example below uploads the dataset in batches, flattening it into `x1, x2, x3, y1, y2, y3, z1, z2, z3`. The steps are:

1. Index the data into a reasonable size.
2. Create the flattened structure.
3. Create a pandas dataframe.
4. Upload each batch to the same dataset with
   {py:func}`~nominal.thirdparty.pandas.upload_dataframe_to_dataset`, waiting for ingestion before readback.

The sample has no recorded timestamps, so the example's synthetic `ts` column assigns
sequential epoch seconds. Use the measurements' actual timestamps for real test data.

```python
import h5py
import pandas as pd
from nominal.core import NominalClient
from nominal.thirdparty.pandas import upload_dataframe_to_dataset

client = NominalClient.from_profile("default")

dataset = client.create_dataset(name="h5_flattened")
num_rows = 0
with h5py.File(local_path, "r") as file:
    data = file["data"]["1"]["meshes"]["B"]

    batch_size = 5
    total_size = data["x"].shape[0]  # Assuming the same shape for 'x', 'y', and 'z'

    for start_idx in range(0, total_size, batch_size):
        end_idx = min(start_idx + batch_size, total_size)

        batch = {}
        for col in ["x", "y", "z"]:
            for col_idx in [0, 1, 2]:
                # Fetch the batch using slicing (start_idx:end_idx)
                batch[f"{col}{col_idx + 1}"] = data[col][
                    start_idx:end_idx, :, col_idx
                ].ravel()

        df = pd.DataFrame(batch)
        df["ts"] = list(range(num_rows, num_rows + len(df)))
        upload_dataframe_to_dataset(
            dataset,
            df,
            timestamp_column="ts",
            timestamp_type="epoch_seconds",
            wait_until_complete=True,
        )
        num_rows += len(df)
```

Inspect the uploaded dataset by running:
```python
from nominal.thirdparty.pandas import datasource_to_dataframe

datasource_to_dataframe(dataset)
```

After upload, navigate to Nominal's [Datasets](https://app.gov.nominal.io/data-sources?sidebar=allDatasets) page (login required). The file appears at the top of the list.

```{include} /guides/_snippets/timestamp-types.md
```
