---
myst:
  html_meta:
    description: "Interactive CLI for browsing and downloading time series data from Nominal"
---

# Downloading Data

{.lead}
Interactive CLI for browsing and downloading time series data from Nominal

The `nom download` command provides an interactive terminal interface for browsing your Nominal data and downloading it to disk. This is particularly useful for exporting data to use in MATLAB, Python notebooks, or other analysis tools.

## Quick Start

To start the interactive download wizard:

```bash
nom download
```

The wizard will guide you through:

1. **Selecting an asset** from your workspace
2. **Choosing a dataset** within that asset
3. **Filtering {abbr}`channels (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)`** to download
4. **Specifying time bounds** for the data
5. **Downloading** the data to disk in your preferred format

## Command Options

```bash
nom download [OPTIONS]
```

**Options:**

- `--format [csv|parquet]` - Output file format (default: `parquet`)
  - `parquet` - Recommended for large datasets, better compression and faster read times
  - `csv` - More widely compatible, easier to inspect manually
- `--channels-per-request INTEGER` - Number of channels to request in parallel (default: `10`)
  - Lower values: Better parallelization for small channel counts
  - Higher values: More efficient for many channels
  - Nominal recommends `--channels-per-request` be approximately the number of channels / number of cores (rounded up) for best performance
- `--points-per-file INTEGER` - Maximum points per output file (default: `25,000,000`)
  - Data is split into multiple files if it exceeds this limit
  - Prevents memory issues with large downloads

## Interactive Workflow

### Step 1: Select an asset

The wizard displays a table of available assets in your workspace:

```
Available Assets (15)
# | Name              | Description                    | Labels        | Properties
--|-------------------|--------------------------------|---------------|------------------
0 | Flight Test #42   | Hover stability test           | 'test'        | 'vehicle'='X1'
1 | Engine Bench Test | Static thrust characterization | 'bench-test'  | 'engine'='v2.1'
...
```

Enter the number of the asset you want to work with.

:::{tip}

Assets are sorted by last updated timestamp, so recent tests appear at the top.
:::

### Step 2: Choose a Dataset

If the asset contains multiple datasets, you'll select which one to download from:

```
Available Datasets (2)
# | Name          | Refname  | Description             | Labels  | Properties
--|---------------|----------|-------------------------|---------|------------
0 | Telemetry     | main     | Primary flight data     | -       | -
1 | Ground System | ground   | Support equipment data  | -       | -
```

:::{note}

If an asset only has one dataset, it will be selected automatically.
:::

### Step 3: Filter Channels

Instead of downloading all channels, you can filter by exact substring matches:

```
There are 1,243 total channels in dataset Flight Test Telemetry.
Enter exact channel substrings, separated by a comma [* for all]:
```

**Examples:**

- `*` - Download all channels (use with caution!)
- `RPM, TEMP, PRESS` - Download all channels containing "RPM", "TEMP", or "PRESS"
- `ENGINE_1` - Download all channels containing "ENGINE\_1"

The wizard will show you how many channels matched your query and offer options to:

- **View the channel list** - See names, descriptions, data types, and units
- **Edit the list** - Opens your default text editor to manually add/remove channels
- **Refine your search** - Try different substring patterns

:::{warning}

Downloading more than 500 channels may result in slow export times. Consider filtering to only the channels you need.
:::

### Step 4: Specify Time Bounds

Choose how to define the time range for your download:

**Option 1: Event**

```
Enter event rid (copy + paste from nominal): ri.nominal.event.abc123
```

- Downloads data for the duration of the event
- If the event has no duration, you'll be prompted to specify one

**Option 2: Run**

```
Enter run rid (copy + paste from nominal): ri.nominal.run.def456
```

- Downloads data for the entire run timespan
- If the run has no end time, you'll be prompted for one

**Option 3: Custom**

```
Enter start timestamp (UTC), e.g. 2025-09-03T10:15:00Z: 2025-10-10T14:30:00Z
Enter end timestamp (UTC): 2025-10-10T15:45:00Z
```

- Manually specify start and end timestamps in ISO 8601 format

After selecting bounds, you can review and edit them:

```
Bounds preview (4500 seconds):
Start (UTC): 2025-10-10T14:30:00+00:00
End   (UTC): 2025-10-10T15:45:00+00:00

Edit timestamps? (y/N):
```

### Step 5: Download Data

After confirming your selections, the wizard downloads the data:

```bash
Enter download directory: [./out]: /Users/yourname/flight_data
About to download 4500 seconds of data from 45 channels. Are you sure? (y/N): y
```

The data is saved to disk in multiple files if needed:

```
/Users/yourname/flight_data/
  ├── abc123-part_0.parquet
  ├── abc123-part_1.parquet
  └── abc123-part_2.parquet
```

## Working with Downloaded Data

### MATLAB

After the download completes, the wizard displays code snippets for loading the data in MATLAB.

**MATLAB 2023b or newer:**

```matlab
data_dir = fullfile( ...
    "/Users/yourname/flight_data", ...
    "abc123-part_*.parquet" ...
);
nominal_data = sortrows(readall(parquetDatastore(data_dir)), "timestamp");
```

**MATLAB 2019b to 2023a:**

```matlab
files=dir(fullfile( ...
    "/Users/yourname/flight_data", ...
    "abc123-part_*.parquet" ...
));
nominal_data = sortrows( ...
    feval( ...
        @(c) vertcat (c{:}), ...
        cellfun( ...
            @parquetread, ...
            fullfile({files.folder}, {files.name}), ...
            "uni", ...
            0 ...
        ) ...
    ), ...
    "timestamp" ...
);
```

:::{tip}

Replace `.parquet` with `.csv` in the code snippets if you downloaded in CSV format.
:::

### Python

**Reading Parquet files:**

```{literalinclude} /guides/_snippets/code/cli/download_data_1.py
:language: python
```

**Using Pandas:**

```{literalinclude} /guides/_snippets/code/cli/download_data_2.py
:language: python
```

## Advanced Usage

### Optimizing Download Performance

**For many channels (100+):**

```bash
nom download --channels-per-request 50 --format parquet
```

- Higher `channels-per-request` reduces overhead
- Parquet format is faster and more compact

**For few than ten channels with long time ranges:**

- Downloads complete faster by requesting fewer channels at once

**For large datasets:**

```bash
nom download --points-per-file 10000000 --format parquet
```

- Smaller file sizes if you're hitting memory limits
- Makes it easier to process data incrementally

### Channel Selection Tips

**Hierarchical channel names:**
If your channels use a delimiter-based hierarchy (e.g., `VEHICLE.ENGINE_1.RPM`):

```
Enter exact channel substrings: VEHICLE.ENGINE_1
```

This selects all channels under the `ENGINE_1` subsystem.

**Multiple subsystems:**

```
Enter exact channel substrings: ENGINE_1, ENGINE_2, FUEL_SYSTEM
```

Downloads all channels from multiple subsystems.

**Using the text editor:**
After getting initial results, choose "Edit the list of channels" to:

- Remove unwanted channels
- Add specific channel names you know exist
- Copy channel lists from documentation

## Data Format

Downloaded data includes:

| Column           | Type   | Description                        |
| ---------------- | ------ | ---------------------------------- |
| `timestamp`      | int64  | Nanoseconds since Unix epoch (UTC) |
| `<channel_name>` | varies | One column per selected channel    |

**Notes:**

- All timestamps are in nanoseconds (1e-9 seconds) since 1970-01-01 00:00:00 UTC
- Channel data types match their original types (float64, int64, string, bool)
- Missing data is represented as null/NaN
- Files are sorted by timestamp within each part file

## Troubleshooting

### "No assets available!"

**Cause:** You don't have access to any assets in the current workspace.

**Solution:** 

- Verify you're authenticated: `nom config profile list`
- Check you're using the correct workspace
- Contact your workspace administrator for access

### "No channels found matching query..."

**Cause:** Your substring filter didn't match any channel names.

**Solution:**

- Try broader search terms
- Use `*` to see all available channels
- Check the dataset page in Nominal to see exact channel names

### Download is slow

**Possible causes:**

- Too many channels selected (>500)
- Long time range with high sample rates
- Network connectivity issues

**Solutions:**

- Reduce the number of channels
- Download shorter time windows
- Increase `--channels-per-request` for better batching
- Use `--format parquet` for faster writes

### Out of memory errors

**Cause:** Trying to download too much data at once.

**Solution:**

- Reduce `--points-per-file` to create smaller output files
- Download smaller time windows
- Reduce the number of channels

### "Failed to get event/run... try again!"

**Cause:** The RID you entered is invalid or you don't have access.

**Solution:**

- Copy the RID directly from the Nominal UI
- Ensure you have permission to access the event/run
- Check for typos in the RID

## Example Session

Here's a complete example session:

```bash
$ nom download --format parquet

# Select asset #5 from the list
Select an asset #: 5
Selected asset: Flight Test #42 (ri.nominal.asset.xyz)

# Select dataset #0
Select a dataset #: 0
Selected dataset: 'Telemetry' (ri.nominal.dataset.abc123)

# Filter for engine and fuel channels
There are 1,243 total channels in dataset Telemetry.
Enter exact channel substrings, separated by a comma [* for all]: ENGINE, FUEL

45 channel(s) selected! View channels? (y/N): y

# Review channels, confirm selection
Are you sure you want to proceed with 45 channel(s)? (y/N): y

# Choose time bounds from a run
Choose an option for providing time bounds for download:
  - event
  - run
  - custom
> run

Enter run rid: ri.nominal.run.def456

Bounds preview (3600 seconds):
Start (UTC): 2025-10-10T14:00:00+00:00
End   (UTC): 2025-10-10T15:00:00+00:00

Edit timestamps? (y/N): n

# Confirm and download
Enter download directory [./out]: ~/flight_data
About to download 3600 seconds of data from 45 channels. Are you sure? (y/N): y

# Data is downloaded and saved
✓ Download complete!

# MATLAB code snippets are displayed for loading the data
```

## See Also

- [Dataset CLI](/guides/datasets/overview.md) - Working with datasets in Python
- [Data integration tutorial](/guides/data-integration-tutorial.md) - Data integration tutorial for Python
