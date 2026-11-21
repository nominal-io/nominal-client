---
myst:
  html_meta:
    description: "Ingest PX4 ULog flight logs with Nominal's native PX4 ULog extractor, from the app or Python."
---

# PX4 ULog

{.lead}
Ingest [PX4](https://px4.io/) [ULog](https://docs.px4.io/main/en/dev_log/ulog_file_format.html) flight logs (`.ulg` or `.ulog`) with Nominal's native PX4 ULog extractor, then review them as a [run](/guides/runs.md).

We recommend the native extractor for PX4 logs.
It reads the log in Nominal, writes one channel per logged field and places every sample on UTC time, so you don't convert files or align clocks yourself.

## Enable the PX4 ULog extractor

Native extractors are [file extractors](/guides/extractors/overview.md) that Nominal builds and maintains.
A workspace admin enables one once per workspace:

1. Open **File extractors** in the side navigation.
1. Select **New extractor**, then **Native extractor**.
1. Select **PX4 ULog** and enable it.

The extractor then appears in the upload wizard and in {py:meth}`~nominal.core.NominalClient.search_containerized_extractors`.

The extractor takes two optional parameters:

| Parameter | Environment variable | Effect |
| --- | --- | --- |
| Topic filter | `TOPIC_FILTER` | Comma-separated topic names to convert, such as `vehicle_attitude,sensor_combined`. Converts every topic when unset. |
| Include parameters | `INCLUDE_PARAMETERS` | When `true`, also writes the log's PX4 parameters as `PARM.<NAME>` channels, with their startup values and in-flight changes. |

## Upload in the app

1. Select **Upload data** to open the upload wizard.
1. Choose the vehicle's asset as the destination.
1. Add the `.ulg` file. If the wizard doesn't route it to the extractor, pick **PX4 ULog** from **Choose an extractor…**.
1. Set **Topic filter** or **Include parameters** if you need them.
1. Turn on **Create run** to open a run over the flight, then select **Upload**.

See [Uploading data](https://docs.nominal.io/core/documentation/platform/data/uploading-data) for the rest of the wizard.

## Upload with Python

The example downloads a sample quadrotor log, which also needs `pip install huggingface_hub`.

```{include} /guides/_snippets/connect-the-client.md
```

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
Once they finish ingesting, create a run over the dataset's time range:

```python
dataset.refresh()
run = asset.create_run("PX4 quadrotor flight", start=dataset.bounds.start, end=dataset.bounds.end)
print(run.nominal_url)
```

Upload each new flight to the same dataset and give it its own run.
Open runs in Nominal to chart topics side by side or compare flights in a [workbook](/guides/workbooks.md).

## Without the native extractor

If your workspace can't enable native extractors, convert the log to one CSV per topic with [pyulog](https://github.com/PX4/pyulog)'s `ulog2csv` and upload them with {py:meth}`~nominal.core.Dataset.add_tabular_data`.
Each topic's `timestamp` column counts microseconds since boot, so pass a `ts.Relative` timestamp type that starts at the boot time.
The GPS topic's `time_utc_usec` gives the UTC time of boot.
