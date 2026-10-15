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

To add a Dataset to an asset, use {py:obj}`Asset.add_dataset() <nominal.core.Asset.add_dataset>`:

```python
import polars as pl
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

df = pl.read_csv("hf://datasets/nominal-io/frosty-flight/frosty_flight_1k_rows.csv")
df.write_csv("frosty_flight_1k_rows.csv")

csv_dataset = client.create_dataset("Frosty Flight")

csv_dataset.add_tabular_data(
    "frosty_flight_1k_rows.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)

uav_drone_asset.add_dataset(
    dataset=csv_dataset, data_scope_name="high-precipitation-flight"
)
```

```{include} /guides/_snippets/what-is-a-dataset.md
```

## Asset data with data scope names

To add a Dataset with a data scope name to an asset, set the `data_scope_name` parameter in {py:obj}`Asset.add_dataset() <nominal.core.Asset.add_dataset>`.

```python
uav_drone_asset.add_dataset(
    dataset=csv_dataset, data_scope_name="high-precipitation-flight"
)
```

:::{note}

Data scope names are a namespace for data sources that share common channels, but do not necessarily belong to the same asset.
They allow data sources with similar schema to be referenced as a group. For example, data sources with the same data scope
name can share workbook templates and Checklists.
:::

## Check if an asset exists

You can check for a asset's existence with {py:obj}`search_assets() <nominal.core.NominalClient.search_assets>`.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

found_assets = client.search_assets(search_text="000-123")

if len(found_assets) == 0:
    uav_drone_asset = client.create_asset(
        name="Quadcopter-000-123",
    )
```

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
with open(filepath, "rb") as f:
    attachment = client.create_attachment_from_io(f, "KittyHawk.pdf")
uav_drone_asset.add_attachments(attachments=[attachment])
```

## Retrieve an asset

Like Datasets, Assets can be retrieved by their resource ID ("RID"):

```python
asset = client.get_asset(
    "ri.scout.cerulean-staging.asset.22697726-5454-4fad-a3ea-fe45e9fa9f09"
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

The list `data_sources` can contain Connection, Dataset, Video instances, or rids as string.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

asset = client.get_asset(
    "ri.scout.cerulean-staging.asset.6c41d7fd-a48f-4d60-baa7-f34492264156"
)

asset.remove_data_scopes(
    scopes=["ri.catalog.cerulean-staging.dataset.d4f413c4-4787-4259-a142-1286587b50af"]
)
```

## Archive an asset

Once an asset is no longer needed, it can be {py:obj}`archived <nominal.core.Asset.archive>`.
This does not delete the asset, but makes it invisible on the web.
If you save the `rid` of the asset, you can {py:obj}`retrieve <nominal.core.NominalClient.get_asset>` and {py:obj}`unarchive <nominal.core.Asset.unarchive>` it at a later date.

```python
uav_drone_asset.archive()
```
