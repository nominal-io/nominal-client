---
myst:
  html_meta:
    description: "Overview and recipes for working with Nominal's Asset primitive in Python"
---

# Assets in Nominal with Python

{.lead}
Overview and recipes for working with Nominal's Asset primitive in Python

```{include} /guides/_snippets/install-warning.md
```

Assets organize test data around physical systems, rather than around individual test events.
For example, an asset might be an airplane or a satellite. Each asset can connect to runs, workbooks, and checklists.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create an asset

To create an asset, use {py:obj}`create_asset() <nominal.core.NominalClient.create_asset>`.
You can optionally add a description, properties, and labels:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
uav_drone_asset = client.create_asset(
    name="Quadcopter-456",
    description="Airborne observation platform",
    properties={"rotors": "4"},
    labels=("camera", "airborne"),
)

print("Created asset:", uav_drone_asset.rid)
```

## Add data to an asset

Use {py:meth}`~nominal.core.Asset.get_or_create_dataset` to retrieve or create a dataset
under a data scope name, then upload through the dataset. This uses `uav_drone_asset`
from the previous example. Install `huggingface_hub` to download the sample CSV.

```python
from huggingface_hub import hf_hub_download

csv_path = hf_hub_download(
    repo_id="nominal-io/frosty-flight",
    filename="frosty_flight_1k_rows.csv",
    repo_type="dataset",
)
csv_dataset = uav_drone_asset.get_or_create_dataset(
    data_scope_name="high-precipitation-flight",
    name="Frosty Flight",
)
dataset_file = csv_dataset.add_tabular_data(
    csv_path,
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)
dataset_file.poll_until_ingestion_completed()
```

The dataset is attached to the asset by `get_or_create_dataset()`. Polling waits for
this file to finish ingestion and raises if ingestion fails. Reusing the data scope
reuses the dataset; running the upload again still adds another file.

```{include} /guides/_snippets/what-is-a-dataset.md
```

## Asset data with data scope names

To attach an existing dataset instead of creating one through the asset, set the `data_scope_name` parameter in {py:obj}`Asset.add_dataset() <nominal.core.Asset.add_dataset>`.

Replace `<DATASET_RID>` with an existing dataset RID and choose a scope name not already
used by this asset:

```python
existing_dataset = client.get_dataset("<DATASET_RID>")
uav_drone_asset.add_dataset(
    dataset=existing_dataset, data_scope_name="ground-station-telemetry"
)
```

:::{note}

Data scope names are a namespace for data sources that share common channels, but do not necessarily belong to the same asset.
They allow data sources with similar schema to be referenced as a group. For example, data sources with the same data scope
name can share workbook templates and Checklists.
:::

## Check if an asset exists

You can search for an asset with {py:obj}`search_assets() <nominal.core.NominalClient.search_assets>`.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

found_assets = client.search_assets(search_text="000-123")

if len(found_assets) == 0:
    matching_asset = client.create_asset(
        name="Quadcopter-000-123",
    )
```

`search_text` is a fuzzy search, so inspect the returned matches before treating one as
a specific asset. This search-and-create example does not guarantee uniqueness.
It illustrates searching for a separate asset; the update, attachment, and archive
examples below continue to operate on `uav_drone_asset` created at the start of this guide.

## Update an asset

Asset metadata can be updated with {py:obj}`Asset.update() <nominal.core.Asset.update>`:

For example, to set an asset's title:

```python
uav_drone_asset.update(name="Quadcopter-001-456")
```

Please see {py:obj}`Asset.update() <nominal.core.Asset.update>` for all updatable metadata.

## Asset attachments

File attachments such as PDF reports or PowerPoints can be added to Assets:

```python
filepath = "/path/to/my_report.pdf"
attachment = client.create_attachment(filepath)
uav_drone_asset.add_attachments(attachments=[attachment])
```

## Retrieve an asset

Like Datasets, Assets can be retrieved by their resource ID ("RID"). Replace
`<ASSET_RID>` with an asset RID from your workspace:

```python
asset = client.get_asset(
    "<ASSET_RID>"
)
asset.update(labels=["NEW-MOTOR-VENDOR"])
```

To retrieve an Asset's RID, visit its detail page and click on the clipboard icon next to "ID" in the right-hand drawer:

![run-metadata](/guides/images/b4/asset_metadata_drawer_srnsdh.png)

```{include} /guides/_snippets/what-is-a-rid.md
```

## Query Assets

Assets can be queried with {py:obj}`search_assets() <nominal.core.NominalClient.search_assets>`.

For example, to retrieve all assets with the label "NEW-MOTOR-VENDOR":

```python
quadcopter_assets = client.search_assets(labels=["NEW-MOTOR-VENDOR"])
```

See {py:obj}`search_assets() <nominal.core.NominalClient.search_assets>` for all Assets search parameters.

## Remove Asset Data Sources

Pass Connection, Dataset, or Video instances or their RIDs in `scopes`. You can also
remove scopes by their names with `names=["high-precipitation-flight"]`. Replace the
RID placeholders below with resources from your workspace. Removing a scope detaches
the source from the asset; it does not delete the source.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

asset = client.get_asset(
    "<ASSET_RID>"
)

asset.remove_data_scopes(
    scopes=["<DATASET_RID>"]
)
```

## Archive an asset

Once an asset is no longer needed, it can be {py:obj}`archived <nominal.core.Asset.archive>`.
This does not delete the asset, but makes it invisible on the web.
If you save the `rid` of the asset, you can {py:obj}`retrieve <nominal.core.NominalClient.get_asset>` and {py:obj}`unarchive <nominal.core.Asset.unarchive>` it at a later date.

```python
uav_drone_asset.archive()
```
