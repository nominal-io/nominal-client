---
myst:
  html_meta:
    description: "Convert PX4 ULog flight logs to CSV, align them to UTC and upload them to a run."
---

# PX4 ULog

{.lead}
Convert a [PX4](https://px4.io/) [ULog](https://docs.px4.io/main/en/dev_log/ulog_file_format.html) flight log into one CSV per topic, align their boot-relative clocks to UTC and upload them to a {term}`run`.

The examples also need `pip install pyulog polars huggingface_hub`.

```{include} /guides/_snippets/connect-the-client.md
```

## Convert the log to CSV

This example downloads a sample quadrotor log. [pyulog](https://github.com/PX4/pyulog)'s `ulog2csv` writes one CSV per topic next to it:

```python
import subprocess
from pathlib import Path

from huggingface_hub import hf_hub_download

ulog_path = Path(
    hf_hub_download(
        repo_id="nominal-io/px4-ulog-quadrotor",
        filename="PX4-quadrotor-example-log.ulg",
        repo_type="dataset",
    )
)
subprocess.run(["ulog2csv", ulog_path], check=True)
csv_files = sorted(ulog_path.parent.glob(f"{ulog_path.stem}_*.csv"))
```

Topic files are named like `<log>_vehicle_gps_position_0.csv`.
A topic can have several instances, such as `battery_status_0` and `battery_status_1`.

## Align the clocks to UTC

Each topic's `timestamp` column counts microseconds since the flight computer booted.
The GPS topic also records `time_utc_usec`, so one GPS row gives the UTC time of boot:

```python
import polars as pl

from nominal import ts

gps = pl.read_csv(ulog_path.parent / f"{ulog_path.stem}_vehicle_gps_position_0.csv")
anchor = gps.filter(pl.col("time_utc_usec") > 0).row(0, named=True)
boot_us = anchor["time_utc_usec"] - anchor["timestamp"] - anchor["timestamp_time_relative"]

boot_clock = ts.Relative("microseconds", start=boot_us * 1000)  # start is in epoch nanoseconds
```

`timestamp_time_relative` is the offset of the UTC reading from `timestamp`, as defined in the [PX4 GPS message](https://github.com/PX4/PX4-Autopilot/blob/v1.11.0/msg/vehicle_gps_position.msg).
Logs that span several boots need one clock per boot.

## Upload the topics to a run

Create one {term}`dataset` per topic, upload each instance's CSV to it, then attach the datasets to a run under the topic name:

```python
datasets = {}
first_us, last_us = [], []
for path in csv_files:
    if "timestamp" not in pl.read_csv(path, n_rows=1).columns:
        continue  # skip topics without a timestamp column
    stamps = pl.read_csv(path, columns=["timestamp"])["timestamp"]
    first_us.append(stamps.min())
    last_us.append(stamps.max())
    topic = path.stem.removeprefix(f"{ulog_path.stem}_").rsplit("_", 1)[0]  # drop the instance number
    if topic not in datasets:
        datasets[topic] = client.create_dataset(topic)
    datasets[topic].add_tabular_data(path, timestamp_column="timestamp", timestamp_type=boot_clock)

start_us = boot_us + min(first_us)
end_us = boot_us + max(last_us)
run = client.create_run("PX4 quadrotor flight", start=start_us * 1000, end=end_us * 1000)
for topic, dataset in datasets.items():
    run.add_dataset(topic, dataset)

print(run.nominal_url)
```

The run spans every uploaded topic, from the earliest sample to the latest.
Open the run in Nominal to chart topics side by side in a {term}`workbook`.

`ulog_info -v <log>.ulg`, also from pyulog, prints the log's start time, duration and flight computer details.
