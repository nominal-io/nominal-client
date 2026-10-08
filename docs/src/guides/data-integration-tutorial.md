---
myst:
  html_meta:
    description: "Learn how to use the python client for automating the most common data"
---

# Data Integration Tutorial for Python

{.lead}
Learn how to use the python client for automating the most common data

The [quickstart](/guides/quickstart.md) covered the mechanics of setting up the Nominal Python client and uploading simple datasets.
This tutorial considers the broader steps in data capture, how those steps correspond to the Nominal data model, and how to automate uploads using Python.

## Working with assets

```{include} /guides/_snippets/explaining-assets.md
```

## Case study: Electric gliders

Consider a hypothetical company producing electric gliders.
The company has two generations of product, codenamed `shimmer` and `sidewinder`.
Each glider has a uniquely identifying tail number, `sn-001` through `sn-999`.

While operating these gliders, the team generates the following types of data:

* a variety of files containing timeseries data, ranging from `.csv` files to proprietary binary file formats;
* an `.mcap` recording containing timeseries data and camera data;
* several `.mp4` videos from several cameras; and
* log files from `systemd` (read using [`journalctl`](https://www.freedesktop.org/software/systemd/man/latest/journalctl.html)).

A test event is coming up, and flight artifacts must be ingested for analysis in Nominal afterwards.

::::::{container} steps

:::{container} step

**Creating an Asset**

```{include} /guides/_snippets/python/creating-an-asset.md
```
:::

:::::{container} step

**Normalizing the data to a Nominal supported format**

The Nominal platform requires uploaded data to be in one of several supported file formats.
After the flight, post-process the data to convert any proprietary format to a known format and otherwise normalize the data to be compliant.

See below for how to set up processing for each data modality in the flight data:

::::{tab-set}

:::{tab-item} Tabular data files

```{include} /guides/_snippets/python/normalizing-tabular-data.md
```
:::

:::{tab-item} Mcap Files

```{include} /guides/_snippets/python/normalizing-mcap-data.md
```
:::

:::{tab-item} Video Files

```{include} /guides/_snippets/python/normalizing-video-data.md
```
:::

:::{tab-item} Log Files

```{include} /guides/_snippets/python/normalizing-log-data.md
```
:::
::::
:::::

:::::{container} step

**Uploading data to Nominal**

Once your data has been transformed into a neutral / normalized format, uploading and ingesting the data into Nominal is straightforward.

The first time data is uploaded to an asset, Nominal creates new datasets and video datasources for the asset.
On subsequent uploads, Nominal appends new files directly to existing datasources (datasets, videos, and so on).

See below for instructions on uploading data to an asset in each case:

:::{tip}

The following examples associate datasources with an asset using a "refname".
Refnames are a mechanism for performing two common tasks within Nominal:

    * Looking up a datasource on an asset to later edit / append data to, and
    * Comparing likewise datasources on different assets in multi-asset workflows.

Use a descriptive but terse refname when associating a datasource with an asset.
For example, if the gliders communicate data over mavlink, a common refname for data associated with that connection is `"mavlink_data"`.
For camera data and video files, a refname based on the context of the camera works well, such as `"front_center_camera"` or `"night_vision_camera"`.
:::

::::{tab-set}

:::{tab-item} Tabular data files

```{include} /guides/_snippets/python/uploading-tabular-data.md
```
:::

:::{tab-item} Mcap Files

```{include} /guides/_snippets/python/uploading-mcap-data.md
```
:::

:::{tab-item} Video Files

```{include} /guides/_snippets/python/uploading-video-data.md
```
:::

:::{tab-item} Log Files

```{include} /guides/_snippets/python/uploading-log-data.md
```
:::
::::
:::::

:::{container} step

**Creating runs in Nominal**

```{include} /guides/_snippets/python/creating-a-run.md
```
:::
::::::
