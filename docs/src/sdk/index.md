# SDK reference

{.lead}
The API reference for the `nominal` package. Start with the [installation instructions](/guides/install), then read the [usage guides](/index.md).

## Commonly used classes

Import these classes from `nominal.core`.
Connect with {py:class}`~nominal.core.NominalClient`, then create and find the resources in the
first table.

```{list-table}
:class: autosummary
:widths: 30 70

* - {py:class}`~nominal.core.NominalClient`
  - Connects to Nominal. Creates, gets, and searches all other resources.
* - {py:class}`~nominal.core.Asset`
  - Groups the [datasets](/guides/datasets/overview.md), [runs](/guides/runs.md),
    [events](/guides/events.md), and attachments of one physical or logical system.
* - {py:class}`~nominal.core.Dataset`
  - Holds [channels](/guides/datasets/channels.md). Takes uploaded files and streamed data.
* - {py:class}`~nominal.core.Run`
  - Marks a time interval of asset and dataset data, such as one test.
* - {py:class}`~nominal.core.Event`
  - Marks a point or interval in time, with a type and priority.
* - {py:class}`~nominal.core.Checklist`
  - A set of checks that evaluates a run or monitors streamed data.
    Create one with {py:class}`~nominal.experimental.compute_as_code.ChecklistBuilder`.
* - {py:class}`~nominal.core.Workbook`
  - An interactive view that plots data from assets and runs.
    Create one from a {py:class}`~nominal.core.WorkbookTemplate`, as the
    [Workbooks guide](/guides/workbooks.md) shows.
```

When working with the above resources, you may run into the following classes, among others:

```{list-table}
:class: autosummary
:widths: 30 70

* - {py:class}`~nominal.core.DatasetFile`
  - An uploaded file, with its ingestion status and time bounds.
    For example,
    {py:meth}`Dataset.add_tabular_data() <nominal.core.Dataset.add_tabular_data>`
    returns one.
* - {py:class}`~nominal.core.VideoDatasetFile`
  - A video file stored as a channel in a dataset.
    For example,
    {py:meth}`Dataset.add_video() <nominal.core.Dataset.add_video>`
    returns one.
* - {py:class}`~nominal.core.Channel`
  - One signal in a dataset. Gives its metadata and data.
    For example,
    {py:meth}`Dataset.get_channel() <nominal.core.Dataset.get_channel>`
    returns one.
* - {py:class}`~nominal.core.WriteStream`
  - Writes time-series points or log lines to a dataset in batches.
    For example,
    {py:meth}`Dataset.get_write_stream() <nominal.core.Dataset.get_write_stream>`
    returns a {py:data}`~nominal.core.DataStream`, and
    {py:meth}`Dataset.get_log_stream() <nominal.core.Dataset.get_log_stream>`
    returns a {py:data}`~nominal.core.LogStream`.
* - {py:class}`~nominal.core.DataReview`
  - The progress and results of one [checklist](/guides/checklists/authoring.md) execution.
    For example,
    {py:meth}`Checklist.execute() <nominal.core.Checklist.execute>`
    returns one.
```

## Sidebar sections

SDK
: [Core](core/index.md) (`nominal.core`) documents all client classes by topic.
  [Timestamps](ts/index.md) (`nominal.ts`) documents the timestamp formats that uploads and
  streams accept. [Exceptions](exceptions.rst) lists the errors and warnings that the client
  raises.

Command Line
: The [`nom` CLI reference](cli.md) documents each command and its options.

Experiments
: [Experimental modules](experimental/index.md) (`nominal.experimental`), such as compute,
  migration, and video processing. Their APIs can change between releases.

Third-Party Integrations
: [Integrations](thirdparty/index.md) with pandas, polars, Ibis, MATLAB, and TDMS.

Releases
: The [changelog](changelog.md) lists the changes in each release.
