---
myst:
  html_meta:
    description: "Ingest PX4 ULog flight logs with Nominal's native PX4 ULog extractor from Python."
---

# PX4 ULog

{.lead}
Upload [PX4](https://px4.io/) [ULog](https://docs.px4.io/main/en/dev_log/ulog_file_format.html) flight logs (`.ulg` or `.ulog`) with Nominal's native PX4 ULog extractor and {py:meth}`~nominal.core.Dataset.add_containerized`, then review them as a [run](/guides/runs.md).

The extractor reads the log in Nominal, writes one channel per logged field and places every sample on UTC time, so you don't convert files or align clocks yourself.
Native extractors are [file extractors](/guides/extractors/overview.md) that Nominal builds and maintains; a workspace admin enables **PX4 ULog** once under **File extractors** › **New extractor** › **Native extractor**.

:::{tip}
You can also upload PX4 logs without code: open **Upload data** in the app, add the `.ulg` file and pick **PX4 ULog** as the extractor.
See [Uploading data](https://docs.nominal.io/core/documentation/platform/data/uploading-data).
:::

The example downloads a sample quadrotor log, which also needs `pip install huggingface_hub`.

```{include} /guides/_snippets/connect-the-client.md
```

## Upload the log

Find the extractor by file suffix and get or create a dataset on the vehicle's [asset](/guides/assets.md):

```python
from pathlib import Path

from huggingface_hub import hf_hub_download

ulog_path = Path(
    hf_hub_download(
        repo_id="nominal-io/px4-ulog-quadrotor",
        filename="PX4-quadrotor-example-log.ulg",
        repo_type="dataset",
    )
)

(extractor,) = client.search_containerized_extractors(file_extension="ulg")
asset = client.get_or_create_asset_by_properties({"serial": "<serial number>"}, name="PX4 quadrotor")
dataset = asset.get_or_create_dataset("px4")
```

{py:meth}`~nominal.core.Dataset.add_containerized` uploads the log and returns an [ingest job](/guides/ingest/ingestion-jobs.md).
The extractor's input is `INPUT_FILE`; `arguments` sets its optional parameters:

| Argument | Effect |
| --- | --- |
| `TOPIC_FILTER` | Comma-separated topic names to convert, such as `vehicle_attitude,sensor_combined`. Converts every topic when unset. |
| `INCLUDE_PARAMETERS` | When `true`, also writes the log's PX4 parameters as `PARM.<NAME>` channels, with their startup values and in-flight changes. |

```python
job = dataset.add_containerized(
    extractor,
    sources={"INPUT_FILE": ulog_path},
    arguments={"INCLUDE_PARAMETERS": "true"},
)
for file in job.as_files_ingested():
    print(file.name, file.ingest_status.name)
```

The extractor writes one file per topic instance, such as `battery_status_0` and `battery_status_1`.

## Create a run

Once the files finish ingesting, {py:meth}`~nominal.core.IngestionJob.file_bounds` gives the time range of this flight's topics.
Create a run over it:

```python
bounds = job.file_bounds()
run = asset.create_run("PX4 quadrotor flight", start=bounds.start, end=bounds.end)
print(run.nominal_url)
```

Upload each new flight to the same dataset and give it its own run.
The job's bounds cover only that flight, even when the dataset holds earlier ones.
Open runs in Nominal to chart topics side by side or compare flights in a [workbook](/guides/workbooks.md).

## Without the native extractor

If your workspace can't enable native extractors, convert the log to one CSV per topic with [pyulog](https://github.com/PX4/pyulog)'s `ulog2csv` and upload them with {py:meth}`~nominal.core.Dataset.add_tabular_data`.
Each topic's `timestamp` column counts microseconds since boot, so pass a `ts.Relative` timestamp type that starts at the boot time.
The GPS topic's `time_utc_usec` gives the UTC time of boot.
