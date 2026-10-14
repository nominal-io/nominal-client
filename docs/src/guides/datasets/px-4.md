---
myst:
  html_meta:
    description: "Load PX4 flight logs in Python and upload them to the Nominal platform"
---

# Load ULog and PX4 files in Python

{.lead}
Load PX4 flight logs in Python and upload them to the Nominal platform

This guide describes an automatable pipeline for uploading PX4 flight logs to Nominal.

[PX4](https://px4.io/) is the leading software standard for drones and unmanned aerial vehicles (UAVs).
PX4 provides a platform for controlling autonomous flight and navigation systems.

[ULog](https://docs.px4.io/main/en/dev_log/ulog_file_format.html) is a binary log file format used by
the PX4 autopilot system for recording flight data. It captures various flight parameters such as 
sensor readings, GPS data, battery status, and actuator outputs.

In Nominal, ULog data is used for post-flight analysis, debugging, and performance benchmarks.
This guide uses [flight logs](https://docs.px4.io/main/en/getting_started/flight_reporting.html)
from the PX4 community hub to demonstrate an automated pipeline for ingesting ULog files into Nominal.

## Download flight logs

```python
dataset_repo_id = 'nominal-io/px4-ulog-quadrotor'
dataset_filename = 'PX4-quadrotor-example-log.ulg'
```

```{include} /guides/_snippets/sample-data.md
```

## Convert ULog to CSV

Install [pyulog](https://github.com/PX4/pyulog): `pip install pyulog`.

It provides `ulog2csv`, which converts the PX4 ULog file to a folder of CSV logs:

```python
import subprocess

ulog_path = dataset_path
subprocess.run(["ulog2csv", ulog_path], capture_output=True, text=True, check=True)
```

### List all CSV flight logs

Depending on the flight, the ULog-to-CSV converter outputs ~50 CSV flight logs. Print each file name to see what is in the converter output:

```python
from pathlib import Path
folder_path = Path(ulog_path).parent
files = sorted(folder_path.glob("PX4-quadrotor-example-log_*.csv"))

for file in files:
    print(file.name.removeprefix('PX4-quadrotor-example-log'))
```

~50 CSV flight log names are printed.

```
_vehicle_land_detected_0.csv
_telemetry_status_0.csv
_vehicle_angular_velocity_0.csv
...
_vehicle_attitude_setpoint_0.csv
```

## Inspect PX4 GPS log

Inspect a single log file (`..log_vehicle_gps_position_0.csv`). The `timestamp` column uses microseconds since system start. This example assumes the selected logs share that boot-relative clock; it is not time since each sensor began recording.

```python

import polars as pl

full_path = folder_path / 'PX4-quadrotor-example-log_vehicle_gps_position_0.csv'

df_gps = pl.read_csv(full_path)

lat_normalized = pl.Series("lat_normalized", df_gps['lat']/10e6)
lon_normalized = pl.Series("lon_normalized", df_gps['lon']/10e6)

df_gps = df_gps.with_columns([
    lat_normalized,
    lon_normalized
])

df_gps.head().select(df_gps.columns[:6])
```

<table border="1" class="dataframe"><thead><tr><th>timestamp</th><th>time_utc_usec</th><th>lat</th><th>lon</th><th>alt</th><th>alt_ellipsoid</th></tr><tr><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>96689774</td><td>1581933170999989</td><td>473566110</td><td>85190396</td><td>423805</td><td>471145</td></tr><tr><td>96890716</td><td>1581933171199988</td><td>473566109</td><td>85190398</td><td>423779</td><td>471119</td></tr><tr><td>97089357</td><td>1581933171399988</td><td>473566107</td><td>85190398</td><td>423756</td><td>471096</td></tr><tr><td>97289265</td><td>1581933171599988</td><td>473566105</td><td>85190398</td><td>423687</td><td>471028</td></tr><tr><td>97490058</td><td>1581933171799988</td><td>473566103</td><td>85190400</td><td>423665</td><td>471005</td></tr></tbody></table>

Plot the recorded latitude vs longitude from the GPS sensor. Install the plotting packages with `pip install plotly kaleido`; exporting images also requires Chrome to be available to Kaleido.

```python
import plotly.express as px
import plotly.io as pio

pio.templates.default = 'plotly_dark'

fig = px.line(df_gps, x='lon_normalized', y='lat_normalized', width=500, height=500)

fig.write_image('drone_gps_flight.svg')
fig.show()
```

![drone-gps-map](https://res.cloudinary.com/didkpxvqu/image/upload/v1727900205/gw3g8xistu8del1rb5wk.svg)

## Upload a single PX4 GPS log file

### Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

### Upload to Nominal

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

dataset = client.create_dataset(name = 'PX4 GPS Sensor data')

dataset.add_tabular_data(
    path = full_path,
    timestamp_column = 'time_utc_usec',
    timestamp_type = 'epoch_microseconds',
)

print('Uploaded dataset:', dataset.rid)
```

After upload, navigate to Nominal's [Datasets](https://app.gov.nominal.io/data-sources?sidebar=allDatasets) page (login required). You'll see your CSV at the top!

## Create a PX4 log lookup table

The script below creates a dataframe with each log file's path, start time, and end time.

```python

from pathlib import Path

log_names = []
time_range_mins = []
time_range_maxs = []
log_path = []

for file in files:
    full_path = folder_path / file.name
    try:
        df_csv = pl.read_csv(full_path)
        if 'timestamp' in df_csv.columns:
            log_path.append(full_path)
            log_names.append(file.name.removeprefix('PX4-quadrotor-example-log'))
            time_range_mins.append(df_csv['timestamp'].min())
            time_range_maxs.append(df_csv['timestamp'].max())
    except pl.exceptions.PolarsError as exc:
        print(f"Skipping unreadable CSV {file.name}: {exc}")

df_log_time_ranges = pl.DataFrame({
    "log_files": log_names,
    "min_micro_s": time_range_mins,
    "max_micro_s": time_range_maxs,
    "log_path": log_path
})
```

Some log files do not have valid start and end times. Remove those rows from the dataframe:

```python
logs_to_filter = ["_mission_result_0.csv", "_sensor_correction_0.csv", "_sensor_selection_0.csv", "_mission_0.csv"]
df_filtered_logs = df_log_time_ranges.filter(pl.col("log_files").is_in(logs_to_filter) == False)
```

Finally, plot all of the log start and end times to identify any outliers.

```python
fig = px.scatter(df_filtered_logs.drop('log_path'),
           x = 'log_files',
           y = ['min_micro_s', 'max_micro_s'],
           height = 600,
           log_y = True,
           title = 'Flight log start and end times')

fig.update_layout(showlegend=False)
fig.update_layout(yaxis_title='Log time span (microseconds)')
fig.write_image('flight_log_start_and_end_times.png')
fig.show()
```

![flight-log-start-and-end-times](https://res.cloudinary.com/didkpxvqu/image/upload/v1727474333/flight_log_start_and_end_times_hs9i0b.svg)

Each log's start and end times vary slightly but are generally uniform. No sensors started mid-flight or stopped long after landing.

### Extract absolute flight time

The GPS log pairs `time_utc_usec` with a boot-relative measurement time from the same row. When `timestamp_time_relative` is present, that measurement time is `timestamp + timestamp_time_relative`; see the [PX4 GPS message definition](https://raw.githubusercontent.com/PX4/PX4-Autopilot/v1.10.0/msg/vehicle_gps_position.msg). Use one row with a valid UTC value to find the absolute origin of the boot clock; taking independent minima could pair different measurements. For logs without the offset field, this example assumes `timestamp` directly corresponds to `time_utc_usec`. It also assumes one boot session and one shared clock across the logs.

Keep the arithmetic in integer microseconds. The run uses the earliest and latest relative timestamps across the selected logs:

```python
from datetime import UTC, datetime, timedelta

valid_gps = df_gps.filter(pl.col("time_utc_usec") > 0)
if valid_gps.is_empty():
    raise ValueError("The GPS log has no valid UTC timestamp to anchor the boot clock")
gps_clock = valid_gps.row(0, named=True)
gps_boot_micro_s = int(gps_clock["timestamp"]) + int(gps_clock.get("timestamp_time_relative", 0))
boot_origin_micro_s = int(gps_clock["time_utc_usec"]) - gps_boot_micro_s
flight_start_micro_s = boot_origin_micro_s + int(df_filtered_logs["min_micro_s"].min())
flight_end_micro_s = boot_origin_micro_s + int(df_filtered_logs["max_micro_s"].max())

dt_flight_start = datetime.fromtimestamp(flight_start_micro_s // 1_000_000, UTC) + timedelta(
    microseconds=flight_start_micro_s % 1_000_000
)
flight_duration = timedelta(microseconds=flight_end_micro_s - flight_start_micro_s)
print(f"Flight start time in UTC: {dt_flight_start}")
print(f"Flight duration: {flight_duration}")
```

## Create PX4 log run

```{include} /guides/_snippets/what-is-a-run.md
```

Use the {py:obj}`create_run() <nominal.core.NominalClient.create_run>` routine to create a run with the flight start and end times identified above.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

quadrotor_run = client.create_run(
    name = 'PX4 single quadrotor flight',
    start = flight_start_micro_s * 1_000,
    end = flight_end_micro_s * 1_000,
    description = 'https://logs.px4.io/plot_app?log=89b87d6f-d286-4703-b36b-573191a907f1',
)
```

If you head over to the [Runs page](https://app.gov.nominal.io/runs) on Nominal (login required), you'll see the "PX4 single quadrotor flight" at the top:

![nominal-runs-page](/guides/images/b3/54d71e11-630a-4681-97b6-ec10facb667a.png)

## Bulk upload all PX4 logs

To upload all ~50 PX4 log files to the quadrotor run, iterate through the lookup table created and validated above.

There may be multiple CSV files associated with each log category, for example `_battery_status_0.csv` and `_battery_status_1.csv`. Append these to one dataset for that category; no intermediate combined CSV is needed.

For each category, create a dataset using {py:obj}`client.create_dataset <nominal.core.NominalClient.create_dataset>`, and then use
{py:obj}`Dataset.add_tabular_data() <nominal.core.Dataset.add_tabular_data>` to upload related CSV files. After every file finishes ingesting, use
{py:obj}`Run.add_datasets() <nominal.core.Run.add_datasets>` to associate the datasets with the run in one call.

```python
import nominal.ts as ts

log_datasets = {}
uploaded_files = []
for row in df_filtered_logs.iter_rows(named=True):
    file_name = row["log_files"]
    ref_name = '-'.join(file_name.split('_')[1:-1])
    csv_name = f'{ref_name}.csv'
    if ref_name not in log_datasets:
        print('Creating dataset:', csv_name)
        log_datasets[ref_name] = client.create_dataset(name=csv_name)
    dataset = log_datasets[ref_name]

    print(f'Adding `{file_name}` to dataset `{csv_name}`')
    dataset_file = dataset.add_tabular_data(
        path=row["log_path"],
        timestamp_column="timestamp",
        timestamp_type=ts.Relative("microseconds", start=boot_origin_micro_s * 1_000),
    )
    uploaded_files.append(dataset_file)

# Wait for every uploaded file before using the run in a workbook.
for dataset_file in uploaded_files:
    dataset_file.poll_until_ingestion_completed()

quadrotor_run.add_datasets(log_datasets)
print("Added log datasets to run")
```

```
...
Creating dataset: actuator-outputs.csv
Adding `_actuator_outputs_0.csv` to dataset `actuator-outputs.csv`

Creating dataset: battery-status.csv
Adding `_battery_status_0.csv` to dataset `battery-status.csv`
Adding `_battery_status_1.csv` to dataset `battery-status.csv`

...

Added log datasets to run
```

On Nominal, navigate from the [Runs page](https://app.gov.nominal.io/runs) to "PX4 single quadrotor flight".
In the "Data scopes" tab, you should see ~40 datasets.

![datasets-in-runs](/guides/images/b3/30034ba7-d4f1-4338-8faa-8d1afd1c6d78.png)

## Create a workbook from PX4 logs

Now that all of the flight data is organized as a test run on Nominal, 
it can be collaboratively visualized, analyzed, and benchmarked as a reference for future flights.
See [Starting a new workbook](https://docs.nominal.io/core/documentation/platform/workbooks/overview#starting-a-new-workbook) for more information.

![px4-data-dashboard](/guides/images/b3/c4e62f86-1126-4b02-9d7f-47d5b1d10ce8.png)

## Appendix

### Inspect ULog metadata

Run the `ulog_info` command to extract high-level log file parameters such as the flight computer RTOS and version.

```python

result = subprocess.run(["ulog_info", "-v", ulog_path], capture_output=True, text=True)
print(result.stdout)
```

```
Logging start time: 0:01:36, duration: 0:13:39
No Dropouts
Info Messages:
 sys_mcu: STM32F76xxx, rev. Z
 sys_name: PX4
 sys_os_name: NuttX
 ...
```
