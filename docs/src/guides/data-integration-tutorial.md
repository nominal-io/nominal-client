---
myst:
  html_meta:
    description: "Design an upload script for a fleet: one asset per vehicle, one dataset per data source and one run per test."
---

# Data integration tutorial

{.lead}
Plan a script that uploads every test's telemetry, MCAP recordings, video and logs for a fleet of vehicles, and marks each test as a {term}`run`.
It applies the [asset](/guides/assets.md), [dataset](/guides/datasets/overview.md) and [run](/guides/runs.md) guides to one example.

```{include} /guides/_snippets/connect-the-client.md
```

## The example

A company flies two models of electric glider, `shimmer` and `sidewinder`, with serial numbers `SN-001` to `SN-999`.
After each test flight, a glider has:

- telemetry as CSV and in a proprietary binary format,
- an MCAP recording with telemetry and a camera,
- MP4 video from other cameras, and
- logs from `journald`.

The script runs after every flight, so each step finds what already exists rather than creating it again.

## One asset per glider

{term}`Properties <property>` that identify the physical glider let the script find its asset every time:

```python
asset = client.get_or_create_asset_by_properties(
    {"platform": "shimmer", "serial_num": "SN-001"},
    name="Shimmer SN-001",
)
```

Choose properties your organization already uses, such as a model and tail number.

## One dataset per data source

Each data source on the glider gets a dataset under a {term}`data scope` name that's the same on every glider, such as `flight_data` or `front_camera`.
{py:meth}`~nominal.core.Asset.get_or_create_dataset` returns the existing dataset after the first flight, so each upload adds to it:

```python
telemetry = asset.get_or_create_dataset("flight_data", prefix_tree_delimiter=".")
mcap = asset.get_or_create_dataset("mcap_data")
front_camera = asset.get_or_create_dataset("front_camera")
logs = asset.get_or_create_dataset("flight_controller_logs")
```

Matching data scope names let one {term}`workbook template` or {term}`checklist` work on every glider.

## Convert each format

Convert anything Nominal can't read before uploading it:

- **Tabular data:** write CSV or Parquet with one timestamp column, as in [CSV and Parquet](/guides/datasets/csv.md). Parquet files are smaller and ingest faster.
  Flatten nested values into dotted column names, such as `gps.lat`, and the `prefix_tree_delimiter` above groups them in the UI.
  For a proprietary format you upload often, a [file extractor](/guides/extractors/overview.md) can convert it during ingest.
- **MCAP:** no conversion needed for protobuf, ROS and ROS 2 messages, or H.264 and H.265 video.
- **Video:** convert to H.264 with [`normalize_video`](/guides/video/video-ingest.md#convert-the-video).
- **Logs:** export them with `journalctl --output json`, as in [Journal JSON](/guides/datasets/journal-json.md).
  Programs can log to `journald` with libraries such as [systemd-python](https://pypi.org/project/systemd-python/) for Python, [spdlog](https://github.com/gabime/spdlog) for C++ and [systemd-journal-logger](https://docs.rs/systemd-journal-logger/) for Rust.

Splitting data across many files is fine. Every file uploaded to a dataset adds to the same {term}`channels <channel>`, and new columns add new channels.
Uploading the same channel and timestamp twice overwrites the earlier value.

## Upload the flight

```python
from datetime import datetime, timezone
from pathlib import Path

for path in sorted(Path("flight_042").glob("*.parquet")):
    telemetry.add_tabular_data(path, timestamp_column="unix_time", timestamp_type="epoch_seconds")

mcap.add_mcap("flight_042/recording.mcap")
mcap.add_mcap_video("flight_042/recording.mcap", channel="cockpit_cam", topic="video")

front_camera.add_video(
    "flight_042/front.mp4",
    channel="front_camera",
    start=datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc),
)
logs.add_journal_json("flight_042/controller.jsonl")
```

To upload many files as one tracked batch, use an [ingest job](/guides/ingest/ingestion-jobs.md) instead.
For ArduPilot dataflash `.bin` files, use {py:meth}`~nominal.core.Dataset.add_ardupilot_dataflash`.

## One run per flight

An asset collects every flight. A run selects one:

```python
run = asset.create_run(
    "Flight 042: unpowered glide",
    start=datetime(2026, 9, 10, 14, 30, tzinfo=timezone.utc),
    end=datetime(2026, 9, 10, 15, 30, tzinfo=timezone.utc),
    properties={"flight": "042"},
)
```

The run's bounds decide what {term}`workbooks <workbook>` and checklists on it see.
If you only know them once the data is read, create the run with `end=None` first and set the bounds later with {py:meth}`~nominal.core.Run.update`.

From here, create [workbooks](/guides/workbooks.md) from templates and run [data reviews](/guides/checklists/data-review.md) on each flight's run.
