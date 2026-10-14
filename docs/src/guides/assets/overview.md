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

```{literalinclude} /guides/_snippets/code/sdk/python/assets/create_asset.py
:language: python
```

## Add data to an asset

To add a Dataset to an asset, use {py:obj}`Asset.add_dataset() <nominal.core.Asset.add_dataset>`:

```{literalinclude} /guides/_snippets/code/sdk/python/assets/add_data_to_asset.py
:language: python
```

```{include} /guides/_snippets/what-is-a-dataset.md
```

## Asset data with data scope names

To add a Dataset with a data scope name to an asset, set the `data_scope_name` parameter in {py:obj}`Asset.add_dataset() <nominal.core.Asset.add_dataset>`.

```{literalinclude} /guides/_snippets/code/sdk/python/assets/add_dataset_with_data_scope_name.py
:language: python
```

```{include} /guides/_snippets/what-is-a-data-scope-name.md
```

## Check if an asset exists

You can check for a asset's existence with {py:obj}`search_assets() <nominal.core.NominalClient.search_assets>`.

```{literalinclude} /guides/_snippets/code/sdk/python/assets/check_asset_exists.py
:language: python
```

## Update an asset

Asset metadata can be updated with {py:obj}`Asset.update() <nominal.core.Asset.update>`:

For example, to set an asset's title:

```{literalinclude} /guides/_snippets/code/sdk/python/assets/update_asset.py
:language: python
```

Please see {py:obj}`Asset.update() <nominal.core.Asset.update>` for all updatable metadata.

## Asset attachments

File attachments such as PDF reports or PowerPoints can be added to Assets:

```{literalinclude} /guides/_snippets/code/sdk/python/assets/add_asset_attachments.py
:language: python
```

## Retrieve an asset

Like Datasets, Assets can be retrieved by their resource ID ("RID"):

```{literalinclude} /guides/_snippets/code/sdk/python/assets/retrieve_asset.py
:language: python
```

To retrieve an Asset's RID, visit its detail page and click on the clipboard icon next to "ID" in the right-hand drawer:

![run-metadata](/guides/images/b4/asset_metadata_drawer_srnsdh.png)

```{include} /guides/_snippets/what-is-a-rid.md
```

## Query Assets

Assets can be queried with {py:obj}`search_assets() <nominal.core.NominalClient.search_assets>`.

For example, to retrieve all assets with the label "NEW-MOTOR-VENDOR":

```{literalinclude} /guides/_snippets/code/sdk/python/assets/query_assets.py
:language: python
```

See {py:obj}`search_assets() <nominal.core.NominalClient.search_assets>` for all Assets search parameters.

## Remove Asset Data Sources

The list `data_sources` can contain Connection, Dataset, Video instances, or rids as string.

```{literalinclude} /guides/_snippets/code/sdk/python/assets/remove_asset_data_sources.py
:language: python
```

## Archive an asset

Once an asset is no longer needed, it can be {py:obj}`archived <nominal.core.Asset.archive>`.
This does not delete the asset, but makes it invisible on the web.
If you save the `rid` of the asset, you can {py:obj}`retrieve <nominal.core.NominalClient.get_asset>` and {py:obj}`unarchive <nominal.core.Asset.unarchive>` it at a later date.

```{literalinclude} /guides/_snippets/code/sdk/python/assets/archive_asset.py
:language: python
```
