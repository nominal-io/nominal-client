---
myst:
  html_meta:
    description: "Guides, examples and reference for the Nominal Python Client."
---

# Nominal Python Client

{.lead}
An Open-source Python library for working with the [Nominal](https://nominal.io) platform.  It allows you to automate data upload, streaming, and retrieval. 

## Intro
Once you [install](/guides/install.md) the library  and set-up your [API keys](/guides/api-keys-setup.md), you can easily upload data to Nominal.

```python
from nominal.core import NominalClient

client  = NominalClient.from_profile("default")
asset   = client.get_or_create_asset_by_properties({"serial": "SN42"}, name="Test stand SN42")
dataset = asset.get_or_create_dataset("primary")
file    = dataset.add_tabular_data("run.csv", timestamp_column="time", timestamp_type="iso_8601")  # your CSV
file.poll_until_ingestion_completed()  # raises NominalIngestError if the ingest fails
print(asset.nominal_url)
```

## Let's go

::::{grid} 1 1 2 2
:gutter: 2

:::{grid-item-card} {octicon}`key` API keys setup
:link: guides/api-keys-setup
:link-type: doc

Create an API key and save it as a profile.
:::
:::{grid-item-card} {octicon}`rocket` Quickstart
:link: guides/quickstart
:link-type: doc

Upload a file, stream data, mark a run and read it back.
:::



:::{grid-item-card} {octicon}`code` Examples
:link: examples/index
:link-type: doc

Full, runnable scripts.
:::

:::{grid-item-card} {octicon}`book` Reference
:link: ref/index
:link-type: doc

Every class, function and `nom` command.
:::
::::

 
```{toctree}
:hidden:
:caption: Get started

Overview <self>
Install <guides/install>
API keys setup <guides/api-keys-setup>
Quickstart <guides/quickstart>
```

```{toctree}
:hidden:
:caption: Tutorials

Upload and stream tutorial <guides/longstart>
Data integration tutorial <guides/data-integration-tutorial>
```

```{toctree}
:hidden:
:caption: Command line interface

Overview <guides/cli/overview>
Download data <guides/cli/downloading-data>
Instrumentation sheets <guides/cli/instrumentation-sheets>
```

```{toctree}
:hidden:
:caption: Datasets

Overview <guides/datasets/overview>
CSV <guides/datasets/csv>
Journal JSON <guides/datasets/journal-json>
HDF5 <guides/datasets/hdf-5>
PX4 <guides/datasets/px-4>
Export data <guides/datasets/export-data>
Decimate data <guides/datasets/decimating-data>
Channels and data types <guides/datasets/channels>
Avro streams <guides/datasets/avro-streams>
```

```{toctree}
:hidden:
:caption: Concepts

Runs <guides/runs>
Assets <guides/assets>
Workbooks <guides/workbooks>
Events <guides/events>
```

```{toctree}
:hidden:
:caption: Stream

Overview <guides/streaming/overview>
Logs <guides/streaming/logs>
Trigger alerts <guides/streaming/streaming-checklists>
```

```{toctree}
:hidden:
:caption: Auto-flag events

Authoring checklists as code <guides/checklists/authoring>
Data review <guides/checklists/data-review>
```

```{toctree}
:hidden:
:caption: Video

Video ingest <guides/video/video-ingest>
MCAP video ingest <guides/video/mcap-video-ingest>
Live video streaming <guides/video/live-video-streaming>
Chart animations <guides/video/chart-animations>
Object identification <guides/video/object-identification>
```

```{toctree}
:hidden:
:caption: Drives

Overview <guides/drives/overview>
```

```{toctree}
:hidden:
:caption: Advanced Ingest Guides

Ingest jobs <guides/ingest/ingestion-jobs>
File extractors <guides/extractors/overview>
```


<!-- Examples section: the gallery page, then every page in examples/ except that index -->

```{toctree}
:hidden:
:caption: Examples
:glob:

Gallery <examples/index>
examples/*
```

```{toctree}
:hidden:
:caption: Reference
:maxdepth: 2

Overview <ref/index>
Core <ref/core/index>
Timestamps <ref/ts/index>
Exceptions <ref/exceptions>
```

```{toctree}
:hidden:
:caption: Command Line
:maxdepth: 1

nom CLI <ref/cli>
```

```{toctree}
:hidden:
:caption: Experiments
:maxdepth: 1

ref/experimental/compute
ref/experimental/compute-as-code
ref/experimental/derived_datasets
ref/experimental/extractors
ref/experimental/ingest
ref/experimental/migration
ref/experimental/spatial
ref/experimental/video
ref/experimental/video_processing
```

```{toctree}
:hidden:
:caption: Third-Party Integrations
:maxdepth: 1

ref/ibis/index
ref/thirdparty/matlab
ref/thirdparty/pandas
ref/thirdparty/polars
ref/thirdparty/tdms
```

```{toctree}
:hidden:
:caption: Releases

ref/changelog
```

```{toctree}
:hidden:
:caption: Project

contributing
license
```
