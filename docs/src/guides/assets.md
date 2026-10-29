---
myst:
  html_meta:
    description: "Create, find, update and archive assets, and attach datasets to them with data scopes."
---

# Assets

{.lead}
An asset represents a system you test, such as a vehicle, satellite or test stand.
It collects that system's datasets, runs, events and attachments across every test.

```{include} /guides/_snippets/connect-the-client.md
```

## Create or find an asset

Properties identify the physical system, such as a serial number.
{py:meth}`~nominal.core.NominalClient.get_or_create_asset_by_properties` returns the asset with those properties, creating it if none exists, so a script can run more than once:

```python
asset = client.get_or_create_asset_by_properties(
    {"serial": "QC-456"},
    name="Quadcopter 456",
    description="Airborne observation platform",
    labels=["camera", "airborne"],
)
```

It raises `ValueError` if more than one asset has those properties.
To always create a new asset, use {py:meth}`~nominal.core.NominalClient.create_asset`.

## Add data with data scopes

An asset holds each dataset under a data scope name, such as `"telemetry"`.
Use the same data scope name for the same kind of data on every asset, so workbook templates and checklists work across them.

{py:meth}`~nominal.core.Asset.get_or_create_dataset` returns the dataset under a data scope name, creating it if needed.
```{include} /guides/_snippets/sample-flight-csv.md
```

```python
dataset = asset.get_or_create_dataset("telemetry", name="Quadcopter 456 telemetry")
file = dataset.add_tabular_data("flight.csv", timestamp_column="time", timestamp_type="iso_8601")
file.poll_until_ingestion_completed()  # raises NominalIngestError if the ingest fails
```

Running the upload again adds another file to the same dataset.
See [Datasets](/guides/datasets/overview.md) for file formats and waiting for ingestion.

To attach a dataset that already exists, use {py:meth}`~nominal.core.Asset.add_dataset`:

```python
asset.add_dataset("ground-station", client.get_dataset("<DATASET_RID>"))
```

{py:meth}`~nominal.core.Asset.remove_data_scopes` detaches data by data scope name or by dataset. It doesn't delete the dataset.

```python
asset.remove_data_scopes(names=["ground-station"])
```

## Find assets

{py:meth}`~nominal.core.NominalClient.search_assets` combines its filters with AND:

```python
assets = client.search_assets(labels=["airborne"], properties={"serial": "QC-456"})
```

`search_text` is a fuzzy match, so results may not contain the text verbatim.
Use `exact_substring` when they must.

To retrieve one asset, use its RID from the asset's page in Nominal:

```python
asset = client.get_asset("<ASSET_RID>")
```

## Update metadata and attachments

{py:meth}`~nominal.core.Asset.update` replaces each field you pass and leaves the others unchanged.
To add a label, merge it with the existing ones:

```python
asset.update(labels=[*asset.labels, "new-motor-vendor"])
```

Attach files that describe the system across tests, such as a specification:

```python
asset.add_attachments([client.create_attachment("spec.pdf")])
```

Attach files about a single test to its [run](/guides/runs.md) instead.

## Archive an asset

{py:meth}`~nominal.core.Asset.archive` hides the asset from views and searches without deleting its data.
{py:meth}`~nominal.core.Asset.unarchive` restores it.

```python
asset.archive()
```
