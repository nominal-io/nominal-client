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

Install [pyulog](https://github.com/PX4/pyulog), Polars, and Plotly: `pip install pyulog polars plotly`.

pyulog provides `ulog2csv`, which converts the PX4 ULog file to a folder of CSV logs:

```python
import subprocess

ulog_path = dataset_path
subprocess.run(["ulog2csv", ulog_path], check=True, capture_output=True, text=True)
```

### List all CSV flight logs

Depending on the flight, the ULog-to-CSV converter outputs ~50 CSV flight logs. Print each file name to see what is in the converter output:

```python
from pathlib import Path
folder_path = Path(ulog_path).parent
files = sorted(folder_path.glob(f"{Path(ulog_path).stem}_*.csv"))

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

Inspect a single log file (`..log_vehicle_gps_position_0.csv`). Its `timestamp` column, like the other topic timestamps, is measured in microseconds since system boot. The GPS log also provides `time_utc_usec`, an absolute UTC timestamp in microseconds. We will use it to align all topics to UTC before bulk upload.

```python

from pathlib import Path

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

Plot the recorded latitude vs longitude from the GPS sensor:

```python
import plotly.express as px
import plotly.io as pio

pio.templates.default = 'plotly_dark'

fig = px.line(df_gps, x='lon_normalized', y='lat_normalized', width=500, height=500)

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
    except pl.exceptions.PolarsError as error:
        print(f"Skipping {file.name}: {error}")

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
fig.show()
```

![flight-log-start-and-end-times](https://res.cloudinary.com/didkpxvqu/image/upload/v1727474333/flight_log_start_and_end_times_hs9i0b.svg)

Each log's start and end times vary slightly but are generally uniform. No sensors started mid-flight or stopped long after landing.

### Extract absolute flight time

Use a GPS row with a valid UTC timestamp to map the boot clock to UTC. In this log,
`timestamp + timestamp_time_relative` is the boot-relative time corresponding to
`time_utc_usec`, as defined by the [PX4 GPS message schema](https://github.com/PX4/PX4-Autopilot/blob/v1.11.0/msg/vehicle_gps_position.msg).
Subtract both fields to get the UTC time of system boot.

```python
from datetime import datetime, UTC, timedelta

from nominal.ts import Relative

gps_anchor = df_gps.filter(pl.col("time_utc_usec") > 0).row(
    0, named=True
)
boot_epoch_us = (
    gps_anchor["time_utc_usec"]
    - gps_anchor["timestamp"]
    - gps_anchor["timestamp_time_relative"]
)
timestamp_type = Relative(
    unit="microseconds", start=boot_epoch_us * 1000
)

# All retained topics share this boot clock. Run bounds use UTC nanoseconds.
run_start_us = boot_epoch_us + df_filtered_logs["min_micro_s"].min()
run_end_us = boot_epoch_us + df_filtered_logs["max_micro_s"].max()
dt_flight_start = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
    microseconds=run_start_us
)
flight_duration_us = run_end_us - run_start_us
hours, remainder = divmod(flight_duration_us // 1_000_000, 3600)
minutes, seconds = divmod(remainder, 60)

print(f"Flight data start time in UTC: {dt_flight_start}")
print(f"Flight duration: {hours}h {minutes}m {seconds}s")
```

```text
Flight data start time in UTC: 2020-02-17 09:52:49.236470+00:00
Flight duration: 0h 2m 0s
```

The `Relative` start is the UTC time of **system boot**, rather than the first GPS
sample's UTC time. Using `epoch_microseconds` for the boot-relative `timestamp` column
would place the flight data near January 1970. For logs spanning multiple boots,
compute a separate clock mapping for each boot.

## Create PX4 log run

```{include} /guides/_snippets/what-is-a-run.md
```

Use the {py:obj}`create_run() <nominal.core.NominalClient.create_run>` routine to create a run with the flight start and end times identified above.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

quadrotor_run = client.create_run(
    name = 'PX4 single quadrotor flight',
    start = run_start_us * 1000,
    end = run_end_us * 1000,
    description = 'https://logs.px4.io/plot_app?log=89b87d6f-d286-4703-b36b-573191a907f1',
)
```

If you head over to the [Runs page](https://app.gov.nominal.io/runs) on Nominal (login required), you'll see the "PX4 single quadrotor flight" at the top:

![nominal-runs-page](/guides/images/b3/54d71e11-630a-4681-97b6-ec10facb667a.png)

## Bulk upload all PX4 logs

To upload all ~50 PX4 log files to the quadrotor run, iterate through the lookup table created and validated above.

There may be multiple CSV files associated with each log category, for example `_battery_status_0.csv` and `_battery_status_1.csv`. Upload them to the same dataset for that category.

For each category, create a dataset using {py:obj}`client.create_dataset <nominal.core.NominalClient.create_dataset>`, and then use
{py:obj}`Dataset.add_tabular_data() <nominal.core.Dataset.add_tabular_data>` to upload related CSV files. Then use {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>` to associate the dataset with the run.

```python
import traceback

log_files = {}

# Add logs to the dataset
for row in df_filtered_logs.iter_rows():
    file_name = row[0]
    full_path = row[3]
    df_csv = pl.read_csv(full_path)
    if 'timestamp' in df_csv.columns:
        try:
            ref_name = '-'.join(file_name.split('_')[1:-1])
            csv_name = f'{ref_name}.csv'

            if ref_name in log_files:
                dataset = log_files[ref_name]
            else:
                print('\nCreating dataset:', csv_name)
                dataset = client.create_dataset(name=csv_name)
                log_files[ref_name] = dataset

            print(f'Adding `{file_name}` to dataset `{csv_name}`')
            dataset.add_tabular_data(
                path=full_path,
                timestamp_column = 'timestamp',
                timestamp_type = timestamp_type
            )
        except Exception:
            print('Error uploading: ', file_name)
            print(traceback.format_exc())

print()
for ref_name, dataset in log_files.items():
    print(f'Adding dataset `{dataset.name}` to run with ref_name `{ref_name}`')
    quadrotor_run.add_dataset(
        ref_name=ref_name,
        dataset=dataset
    )
```

```
...
Creating dataset: actuator-outputs.csv
Adding `_actuator_outputs_0.csv` to dataset `actuator-outputs.csv`

Creating dataset: battery-status.csv
Adding `_battery_status_0.csv` to dataset `battery-status.csv`
Adding `_battery_status_1.csv` to dataset `battery-status.csv`

...

Adding dataset `actuator-outputs.csv` to run with ref_name `actuator-outputs`
Adding dataset `battery-status.csv` to run with ref_name `battery-status`
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
