---
myst:
  html_meta:
    description: "Automate uploads of telemetry, video, and logs, and organize flight tests with assets and runs."
---

# Data Integration Tutorial for Python

{.lead}
Automate uploads of telemetry, video, and logs, and organize flight tests with assets and runs.

The [quickstart](/guides/quickstart.md) covered the mechanics of setting up the Nominal Python client and uploading simple datasets.
This tutorial considers the broader steps in data capture, how those steps correspond to the Nominal data model, and how to automate uploads using Python.

## Working with assets

In Nominal, an asset is the digital representation of a physical device operated during tests.
For example, if you are handling data for a fleet of airplanes, then each of those airplanes could be represented as an asset within Nominal.

:::{tip}

While it is simple to describe an asset as a 1:1 relation between physical assets, an asset may also refer to a shared concept.
For example, an asset in Nominal could be used to group simulation runs for a planned future aircraft that doesn't exist physically yet.
:::

Assets have (amongst other things):

* A human readable name and a description.
* Properties which may help programmatically identify an asset from others. Some examples:
    * For airplanes: a model and tail number are commonly used to map between a physical plane and the asset in Nominal.
    * For self-driving food delivery cars: a license plate number, make / model, sensors fitted (`has_lidar`, `has_radar`, `has_night_vision_cameras`, etc.)
    * For vibration stands: a serial number, type, and warehouse number.

Other associated metadata may include labels, URLs to resources associated with the asset, file attachments, etc.
As shown later, an asset can be found by name or by searching metadata.

:::{tip}

Being able to uniquely identify an asset in Nominal via a combination of properties enables easy lookup and search based on conventions contextual to your organization.
:::

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

::::::::::::{container} steps

::::::::::{container} step

**Creating an Asset**

To start, create an `Asset` to upload data to, one per glider.
With only a few assets, this could be done via the frontend.
With thousands of gliders, automate the process using Python.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
asset = client.get_or_create_asset_by_properties(
    {"platform": "shimmer", "serial_num": "SN-001"},
    name="Shimmer SN-001",
    description="Our very first shimmer glider produced",
)
```

Use this same call for each flight test. It returns the asset matching these properties,
or creates it if none exists. Multiple matches raise `ValueError`; use
{py:meth}`~nominal.core.NominalClient.search_assets` directly if you need custom selection behavior.

See the {py:obj}`Asset function reference <nominal.core.Asset>` for more information on what you can do with a `nominal.core.Asset`.
::::::::::

::::::::::{container} step

**Normalizing the data to a Nominal supported format**

The Nominal platform requires uploaded data to be in one of several supported file formats.
After the flight, post-process the data to convert any proprietary format to a known format and otherwise normalize the data to be compliant.

See below for how to set up processing for each data modality in the flight data:

::::::::{tab-set}

::::::{tab-item} Tabular data files

Tabular data comes in a large variety of formats, ranging from simple `.csv` files to complex proprietary binary formats.
Prior to uploading data, it should be converted into a format supported by Nominal.
Today, the most commonly used intermediary file formats are CSV and Parquet.
Parquet is generally preferred, since it produces smaller files that can be ingested faster than CSV files.

The platform imposes a few additional requirements:

* Use [supported channel data types](/guides/datasets/channels.md).

* Each file of data *must* have one known column with timestamps.
    Nominal supports a wide array of timestamp types, with some of the most popular including:
    * Absolute floating point \{hours, minutes, seconds, milliseconds, microseconds, nanoseconds\} since unix epoch
    * Relative floating point \{hours, minutes, seconds, milliseconds, microseconds, nanoseconds\} since a provided absolute timestamp
    * [ISO8601 Timestamps](https://en.wikipedia.org/wiki/ISO_8601)
    * Custom string-based timestamp formats with a provided [jodatime format string](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html)
* The platform supports *viewing* channels in a hierarchical manner.
    However, data must be flattened during ingest.

    Consider the following example data, in JSON:

    ```json
    {
      "timestamp": 12345,
      "gps": {
        "lat": 12.1,
        "lon": 12.3,
        "height": 10000
      },
      "imu": {
        "roll": 1.23,
        "pitch": -0.2,
        "yaw": 0.0
      }
    }
    ```

    You can preserve the hierarchical structure of this data by naming columns appropriately:

    | timestamp | gps.lat | gps.lon | gps.height | imu.roll | imu.pitch | imu.yaw |
    | --- | --- | --- | --- | --- | --- | --- |
    | 12345 | 12.1 | 12.3 | 10000 | 1.23 | -0.2 | 0.0 |

    When creating the dataset using the Python client, you must specify a `prefix_tree_delimiter` of `"."` for the columns to be interpreted hierarchically.

    ::::{tip}

    When flattening data, do not feel compelled to jam-pack it all into a single file to upload to Nominal.
    In many cases, it is easier to produce a folder of `.csv` or `.parquet` files and to upload those in a for-loop.
    You can upload as many files as you want to a dataset, and they will all be combined into a unified source.

    This method can be used both to *concatenate* and to *join* additional data across files. New columns will result in additional channels being created, and new timestamps for existing channels will add additional data to those existing channels.

    :::{warning}

    Uploading data from multiple files to the same channels with duplicate timestamps will overwrite existing data.
    :::
    ::::
::::::

::::::{tab-item} Mcap Files

Nominal supports MCAP files containing any combination of the following:

* Protobuf messages
* ROS messages (detected via `ros1` message encoding)
* ROS2 messages (detected via CDR message encoding)
* H264/H265 encoded video frames

When ingesting data in any of these formats to Nominal, no additional pre-transformations are required.
::::::

::::::{tab-item} Video Files

```{include} /guides/_snippets/python/normalizing-video-data.md
```
::::::

::::::{tab-item} Log Files

Today, human-readable text logs are only supported when using `.jsonl` files as generated by `journalctl`.

`journalctl` is an industry standard program for viewing logs produced by `journald` processes, typically associated with `systemd`.
While there are many guides available on the internet for using `journalctl` (such as [this one](https://www.loggly.com/ultimate-guide/using-journalctl)), there are two key things to keep in mind:

* The software processes running on your asset must log to `journald` to be picked up by `journalctl`
* When extracting logs using `journalctl`, you *must* output in `.jsonl` format-- i.e., run `journalctl --output json ...`

::::{admonition} Sending logs to systemd
:class: note

The first point above may seem daunting, but should be straightforward using standard logging libraries for your programming language.
Here are some recommendations for common programming languages:

* Python
    * you may use the [systemd-python](https://pypi.org/project/systemd-python/) library and its [`JournalHandler`](https://github.com/systemd/python-systemd/blob/main/systemd/journal.py#L504) with a standard setup using the built in `logging` library.
* C++
    * [Log4CPlus](https://github.com/wilx/log4cplus/) logging library (used by, e.g. ROS1) and its [syslog appender](https://github.com/wilx/log4cplus/blob/master/include/log4cplus/syslogappender.h)
    * [spdlog](https://github.com/gabime/spdlog) logging library and its [systemd_sink](https://github.com/gabime/spdlog/blob/v1.x/include/spdlog/sinks/systemd_sink.h)
* Rust
    * [systemd_journal_logger](https://docs.rs/systemd-journal-logger/latest/systemd_journal_logger/) crate can be used to get a vanilla rust logger that directly logs to systemd.

:::{admonition} Not seeing your language or logging library here?
:class: tip

Reach out to your Nominal representative for help integrating your logging infrastructure with `systemd` to get started with Nominal's log integration today!
:::
::::
::::::
::::::::
::::::::::

::::::::::{container} step

**Uploading data to Nominal**

Once your data has been transformed into a neutral / normalized format, uploading and ingesting the data into Nominal is straightforward.

Create or retrieve a dataset for each data source on the asset, then append files to it.
Video is stored in named channels on a dataset, alongside telemetry and logs.

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

::::::::{tab-set}

::::::{tab-item} Tabular data files

When uploading tabular data, there are a few common formats Nominal supports, as well as some more specialized formats.
These include (but are not limited to):

* `.csv` files (or gzip-compressed CSV files, `.csv.gz`)
* `.parquet` files
* `.bin` ardupilot dataflash files

```python
from nominal.core import NominalClient

# Assumes authentication has already been performed.
# Replace "default" with your profile name.
client = NominalClient.from_profile("default")

# Use the same identifying properties for each flight.
asset = client.get_or_create_asset_by_properties(
    {"platform": "shimmer", "serial_num": "SN-001"}, name="Shimmer SN-001"
)

# Get or create dataset linked to the asset.
# If a dataset with this data scope name already exists on the asset, it is returned.
# Otherwise, a new dataset is created and linked to the asset automatically.
dataset = asset.get_or_create_dataset(
    # Data scope name (reference name) for this dataset within the asset.
    # This can be used later to retrieve a reference to this dataset via the same reference name for
    # future flight tests.
    data_scope_name="flight_data",
    # Human readable name for the dataset
    name="Flight Data",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
    # Optional description for the dataset, useful for storing notes for future readers
    description="Flight data from onboard telemetry platform",
)
dataset.add_tabular_data(
    path="path/to/data.parquet",
    # Column within the parquet file containing absolute or relative timestamp information
    # for all other columns within the file
    timestamp_column="unix_timestamps",
    # Type of data contained within the timestamp column.
    # Using absolute floating point seconds from unix epoch for this example, but a wide variety
    # of formats are supported, such as other resolutions of time, relative timestamps, or even custom-formatted
    # string timestamps (e.g. ISO8601).
    timestamp_type="epoch_seconds",
)
```

Since `get_or_create_dataset` returns the existing dataset if one already exists with that data scope name, you can use the same code for subsequent flight test events.
You can also retrieve the dataset directly by its data scope name:

```python
# NOTE: refname needs to match the refname used to add the dataset to the asset
dataset = asset.get_dataset("flight_data")

dataset.add_tabular_data(
    "path/to/other/data.parquet",
    # column containing timestamp information for this data file.
    # NOTE: Need not be the same as other files in this dataset.
    timestamp_column="unix_timestamps",
    # type of timestamps stored in this data file.
    # NOTE: Need not be the same as other files in this dataset.
    timestamp_type="epoch_seconds",
)
```

:::{note}

Want to ingest Ardupilot Dataflash `.bin` files?
When adding data to the dataset, you can use `Dataset.add_ardupilot_dataflash`.
:::
::::::

::::::{tab-item} Mcap Files

MCAP files are uploaded to Nominal analogously to any other tabular file format, though they differ
slightly in the sense that they may also have video content. Video frames go to a video channel on
the same dataset, so telemetry and video share one time domain.

```python
from nominal.core import NominalClient

# Assumes authentication has already been performed.
# Replace "default" with your profile name.
client = NominalClient.from_profile("default")

# Use the same identifying properties for each flight.
asset = client.get_or_create_asset_by_properties(
    {"platform": "shimmer", "serial_num": "SN-001"}, name="Shimmer SN-001"
)

# Reuse the dataset for this data scope across flight tests.
dataset = asset.get_or_create_dataset(
    "mcap_flight_data",
    name="MCAP Flight Data",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
    # Optional description for the dataset, useful for storing notes for future readers
    description="Flight data from onboard telemetry platform",
)

# Add the MCAP file to the dataset.
# Use include_topics or exclude_topics to select which telemetry topics to ingest.
dataset.add_mcap("path/to/data.mcap")

# Video frames from the same MCAP land on a channel of the same dataset,
# so telemetry and video share one time domain.
# The `topic` is the MCAP topic carrying the video data; per-frame timestamps
# come from that topic's messages.
dataset.add_mcap_video(
    "path/to/data.mcap",
    # Name of the video channel within the dataset
    channel="mcap_flight_video",
    topic="video_data_topic",
)
```

Repeat the same get-or-create and upload calls for subsequent flight tests. Files are
appended to the dataset already linked as `mcap_flight_data`.
::::::

::::::{tab-item} Video Files

Once video files have been transformed into a format Nominal can ingest, uploading and ingesting the files is straightforward.

Video is stored as a **video channel** on a dataset, alongside your other test data. Create the
dataset once per video source, then add files to a named channel on it:

```python
import datetime

from nominal.core import NominalClient

# Assumes authentication has already been performed
# Replace "default" with your profile name
client = NominalClient.from_profile("default")

# Use the same identifying properties for each flight.
asset = client.get_or_create_asset_by_properties(
    {"platform": "shimmer", "serial_num": "SN-001"}, name="Shimmer SN-001"
)

# Reuse the camera's dataset across flight tests.
# Make sure to run nominal.experimental.video_processing.normalize_video()
# on your raw video files first!
dataset = asset.get_or_create_dataset(
    "front_center_camera",
    name="Front center camera video",
    # Optional description for the dataset, useful for storing notes and details
    # about the video (e.g., details about the camera)
    description="Front-facing go pro",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
        "position": "front_center",
        "video_type": "rgb",
    },
)

video_file = dataset.add_video(
    path="path/to/video.mp4",
    # Name of the video channel within the dataset that these frames belong to
    channel="front_center_camera",
    # Starting timestamp, in absolute time, for this particular video file
    start=datetime.datetime(2025, 2, 7, 14, 38, 19, tzinfo=datetime.timezone.utc),
)
```

For subsequent flight test events, or even just additional video files
from the original test event, skip creating a new dataset and
instead add files to the existing channel:

```python
import datetime

# NOTE: refname needs to match the refname used to add the dataset to the asset
dataset = asset.get_dataset("front_center_camera")
dataset.add_video(
    "path/to/video_pt2.mp4",
    channel="front_center_camera",
    start=datetime.datetime(2025, 2, 7, 15, 17, 3, tzinfo=datetime.timezone.utc),
)
```

:::{note}

Standalone `Video` objects (`client.create_video()`, `Video.add_file()`, `Asset.add_video()`) are
deprecated in favor of video channels. Using them emits a `LegacyVideoDeprecationWarning`.
:::
::::::

::::::{tab-item} Log Files

`.jsonl` log files (or gzip-compressed JSON Lines files, `.jsonl.gz`) from `journalctl` can be uploaded to Nominal analagously to tabular data files.

```python
from nominal.core import NominalClient

# Assumes authentication has already been performed
# Replace "default" with your profile name
client = NominalClient.from_profile("default")

# Use the same identifying properties for each flight.
asset = client.get_or_create_asset_by_properties(
    {"platform": "shimmer", "serial_num": "SN-001"}, name="Shimmer SN-001"
)

# Reuse the dataset for this data scope across flight tests.
dataset = asset.get_or_create_dataset(
    "flight_controller_logs",
    name="Flight Controller Logs",
    # Key-value properties that are useful for looking up and finding this dataset
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
    # Optional description for the dataset, useful for storing notes for future readers
    description="Systemd log data from the flight controller",
)

# Add the systemd journal JSON file to the dataset
dataset.add_journal_json("path/to/log.jsonl")
```

Repeat the same get-or-create and upload calls to append journal files from later flight tests.

:::{admonition} How is this different from tabular data?
:class: note

The same `nominal.core.Dataset` class handles uploading text logs *and* tabular data.
You *can* add text logs to a dataset created from `.csv` files, assuming that the dataset does not already have a channel named `logs`.
The text logs and numeric tabular data will show up as separate channels, and can be added/visualized in a workbook as normal.
:::
::::::
::::::::
::::::::::

::::::::::{container} step

**Creating runs in Nominal**

When working with assets, users generally work with data from many distinct test events.
As shown in other sections of the tutorial, this is generally accomplished by repeatedly uploading data to a set of datasources within an asset.
For example, if every test event generates some CSV, Parquet, log, and video files, these will be uploaded to the same set of datasources for each test event.
However, as useful as it is to see all of the data for an asset in a single place, frequently it is useful to investigate a single test event and all of the data associated with it.

This is where Runs come in to play!
A run is defined by a start and end time on an existing asset that is a view on all of the files and datasources attached to that asset.
When creating workbooks, running checklists, or doing other validation on your data, it is useful to be able to perform these tasks on a single flight test.
Create these runs with {py:meth}`~nominal.core.Asset.create_run`, which associates the asset automatically.

```python
import datetime

from nominal.core import NominalClient

# Assumes authentication has already been performed
# Replace "default" with your profile name
client = NominalClient.from_profile("default")

# Use the same identifying properties for each flight.
asset = client.get_or_create_asset_by_properties(
    {"platform": "shimmer", "serial_num": "SN-001"}, name="Shimmer SN-001"
)

run = asset.create_run(
    name="Test Flight #2 10/09/2024",
    start=datetime.datetime(
        year=2024,
        month=10,
        day=9,
        hour=13,
        minute=3,
        second=5,
        tzinfo=datetime.timezone.utc,
    ),
    end=datetime.datetime(
        year=2024,
        month=10,
        day=9,
        hour=14,
        minute=2,
        second=17,
        tzinfo=datetime.timezone.utc,
    ),
    # Useful for looking up runs later on by asset properties
    properties={"platform": "shimmer", "serial_num": "SN-001"},
    # Optional human description of the run, useful for describing what maneuvers we tested
    description="Testing unpowered glide",
)
```

:::{tip}

Determining the correct start / end bounds for a `nominal.core.Run` can be challenging when you have a large number of data files being ingested.
A good practice is to create the `nominal.core.Run` early on in the data ingestion script and to update the bounds as you go.

An example of doing this would look like:

```python
import time

run = asset.create_run(
    name="Test Flight #2",
    # The flight happened in the past, so its data starts before now.
    start=time.time_ns(),
    end=None,
)

# Read the earliest and latest UTC timestamps from the flight data, in nanoseconds.
data_start_ns = 1_728_478_985_000_000_000
data_end_ns = 1_728_482_537_000_000_000

# Run.start and Run.end also use integer UTC nanoseconds.
run.update(
    start=min(run.start, data_start_ns),
    end=data_end_ns if run.end is None else max(run.end, data_end_ns),
)
```

Whether you add run bounds ahead of time or as you go, it is
important to set them correctly, since they influence data
visualization on the website ( e.g. when viewing workbooks on
runs).
:::
::::::::::
::::::::::::
