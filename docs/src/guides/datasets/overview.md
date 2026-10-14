---
myst:
  html_meta:
    description: "Overview and recipes for working with Nominal's Dataset primitive in"
---

# Datasets in Nominal with Python

{.lead}
Overview and recipes for working with Nominal's Dataset primitive in

```{include} /guides/_snippets/install-warning.md
```

Datasets are Nominal's primitive for ingesting and working with tabular data files.
They must have at least one timestamp column.

Once uploaded to Nominal, Datasets can be organized into *Runs* with other 
data sources - including video files, database connections, and log files.

```{include} /guides/_snippets/what-is-a-dataset.md
```

This guide details common patterns for working with Nominal Datasets in Python.

For data that does not fit a tabular shape, where channels sample at different rates or at
unrelated moments, see [Avro stream files](/guides/datasets/avro-streams.md). To fix a
channel's data type before any data arrives, see
[Channels and data types](/guides/datasets/channels.md).

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Upload a Dataset

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

dataset = client.create_dataset('Frosty Flight')
dataset.add_tabular_data(
    'frosty_flight_1k_rows.csv',
    timestamp_column = 'source_time',
    timestamp_type = 'iso_8601',
)

print('Uploaded dataset:', dataset.rid)
```

Replace `frosty_flight_1k_rows.csv` with a path to a CSV file on your own computer.

If you don't have a CSV file handy, you can download 'frosty_flight_1k_rows.csv' 
by copy-pasting the below 4 lines into your Python terminal.

```python

sample_csv = 'hf://datasets/nominal-io/frosty-flight/frosty_flight_1k_rows.csv'
df = pl.read_csv(sample_csv)
df.write_csv('frosty_flight_1k_rows.csv')
```

Head over to the [CSV files](/guides/datasets/csv.md) page for more options for CSV file upload.

## Set channel units

Units are channel metadata that tell Nominal what each numeric value represents, such as `m`, `Cel`, or `1/min`. Add units after a CSV or other tabular upload has created channels, or register them before data arrives if a downstream workflow requires channel metadata in advance.

Units help Nominal label data consistently across data sources and workbooks. UCUM-compatible units power workbook analysis: the **Unit conversion** transform can create converted variables, derived variables can preserve or compute units during transforms, and Nominal can warn when formulas mix incompatible units. Display-only units are useful for custom labels that should appear in the UI but should not participate in UCUM-based conversions.

Nominal validates unit symbols against [UCUM](https://ucum.org/ucum) by default. Use display-only units only when no UCUM symbol represents the label.

### Update units on existing channels

Use `set_channel_units()` to update several channels in one request. Pass `None` as a unit value to clear a channel's unit.

```python
dataset.set_channel_units(
    {
        "latitude": "deg",
        "longitude": "deg",
        "altitude": "m",
        "temperature": "Cel",
    },
    validate_schema=True,
)

dataset.set_channel_units({"diagnostic_flag": None})
```

When `validate_schema=True`, Nominal raises an error if the mapping contains a channel name that does not exist. With the default `validate_schema=False`, Nominal ignores unknown channel names.

Use `Channel.update()` when you already have the channel object and only need to update one channel.

```python
channel = dataset.get_channel("engine.speed")

channel.update(unit="1/min")
channel.update(unit=None)
```

Use `allow_display_only_units=True` only for labels that should be displayed but should not participate in UCUM-based conversions.

```python
dataset.set_channel_units(
    {"engine.speed": "RPM"},
    allow_display_only_units=True,
)
```

Check unit symbols before applying them with the unit helpers on `NominalClient`.

```python
unit = client.get_unit("Cel")
if unit is None:
    raise ValueError("Unknown unit symbol")

compatible_units = client.get_commensurable_units("m")
all_units = client.get_all_units()
```

### Register channels before data exists

If you need units before a file upload or stream write creates channels, register the channels on an empty dataset first. Use `add_channel()` for one new channel, or `batch_add_channels()` for many channels. These methods create channel metadata without writing points.

```python
from nominal.core import ChannelDataType, NominalClient
from nominal.core.datasource import CreateChannelRequest

client = NominalClient.from_profile("default")
dataset = client.get_dataset("<DATASET_RID>")

dataset.add_channel(
    name="engine.speed",
    data_type=ChannelDataType.DOUBLE,
    description="Engine shaft speed",
    unit="1/min",
)

dataset.batch_add_channels([
    CreateChannelRequest(
        name="latitude",
        data_type=ChannelDataType.DOUBLE,
        unit="deg",
    ),
    CreateChannelRequest(
        name="longitude",
        data_type=ChannelDataType.DOUBLE,
        unit="deg",
    ),
    CreateChannelRequest(
        name="vehicle.mode",
        data_type=ChannelDataType.STRING,
        description="Current vehicle mode",
    ),
])
```

`batch_add_channels()` skips channels that already exist, so it works well in startup code that can run more than once.

## Relative timestamps

The example above uploaded a Dataset with an absolute time series column (`source_time`). 
In lay person's terms, absolute times refers to the date + time  on a calendar + clock when recording a measurement. 
Absolute time is usually expressed in reference to a global standard 
(such at [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time)) that normalizes the 
timestamp timezone. 

Sometimes, test measurements are recorded in *relative* time, where 0 represents the start of the
measurement and 0 + (i * time unit) represent the timestamp of subsequent measurements. 

For example, say that you're measuring the pressure in an engine chamber once per second. In relative time,
your first measurement would be at time 0 and your 1000th measurement would be at 1000 seconds.

Nominal has first class support for measurements in either relative or absolute time, down to
picosecond resolution.

To upload a dataset clocked in relative time, set the `timestamp_type` parameter to
`nominal.ts.Relative`. It takes the unit your measurements are clocked in, and the absolute
`start` time that time 0 corresponds to:

```python

ts.Relative("seconds", start=0)  # time 0 is the Unix epoch
```

Valid units are `picoseconds`, `nanoseconds`, `microseconds`, `milliseconds`, `seconds`,
`minutes`, `hours`, and `days`. Pass a `datetime` as `start` to anchor the data to a real
point in time, or `0` to leave it at the Unix epoch so that every run overlaps on one
timeline.

Consider this [jet engine simulation](https://huggingface.co/datasets/nominal-io/nasa-turbofan-degradation) from NASA.
The relative timestamp column is `cycle`, which represents one operational cycle of a jet engine.
(Nominal has no `cycles` time unit, so the example uses `hours` as a proxy.)

First, download and inspect this CSV:

```python

dataset_name = 'NASA_turbofan_train_engine_1_FD001.csv'
link_to_csv = f'hf://datasets/nominal-io/nasa-turbofan-degradation/{dataset_name}'

df_engine1 = pl.read_csv(link_to_csv)
df_engine1.write_csv(dataset_name)

df_engine1.head().select(df_engine1.columns[:8])
```

<table border="1" class="dataframe"><thead><tr><th>engine</th><th>cycle</th><th>setting_1</th><th>setting_2</th><th>setting_3</th><th>(Fan inlet temperature) (◦R)</th><th>(LPC outlet temperature) (◦R)</th><th>(HPC outlet temperature) (◦R)</th></tr><tr><td>i64</td><td>i64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>1</td><td>1</td><td>-0.0007</td><td>-0.0004</td><td>100.0</td><td>518.67</td><td>641.82</td><td>1589.7</td></tr><tr><td>1</td><td>2</td><td>0.0019</td><td>-0.0003</td><td>100.0</td><td>518.67</td><td>642.15</td><td>1591.82</td></tr><tr><td>1</td><td>3</td><td>-0.0043</td><td>0.0003</td><td>100.0</td><td>518.67</td><td>642.35</td><td>1587.99</td></tr><tr><td>1</td><td>4</td><td>0.0007</td><td>0.0</td><td>100.0</td><td>518.67</td><td>642.35</td><td>1582.79</td></tr><tr><td>1</td><td>5</td><td>-0.0019</td><td>-0.0002</td><td>100.0</td><td>518.67</td><td>642.37</td><td>1582.85</td></tr></tbody></table>

The column for relative time, `cycle`, spans from 1 to 192.

Inspect all of the column names:

```python
df_engine1.columns
```

```
['engine',
 'cycle',
 'setting_1',
 'setting_2',
 'setting_3',
 '(Fan inlet temperature) (◦R)',
 '(LPC outlet temperature) (◦R)',
 '(HPC outlet temperature) (◦R)',
 '(LPT outlet temperature) (◦R)',
 '(Fan inlet Pressure) (psia)',
 '(bypass-duct pressure) (psia)',
 '(HPC outlet pressure) (psia)',
 '(Physical fan speed) (rpm)',
 '(Physical core speed) (rpm)',
 '(Engine pressure ratio(P50/P2)',
 '(HPC outlet Static pressure) (psia)',
 '(Ratio of fuel flow to Ps30) (pps/psia)',
 '(Corrected fan speed) (rpm)',
 '(Corrected core speed) (rpm)',
 '(Bypass Ratio) ',
 '(Burner fuel-air ratio)',
 '(Bleed Enthalpy)',
 '(Required fan speed)',
 '(Required fan conversion speed)',
 '(High-pressure turbines Cool air flow)',
 '(Low-pressure turbines Cool air flow)']
 ```

 Finally, upload this dataset to Nominal with a relative `timestamp_type`:

```python

from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

dataset = client.create_dataset(dataset_name)
dataset.add_tabular_data(
    dataset_name,
    timestamp_column = 'cycle',
    timestamp_type = ts.Relative('hours', start=0),
)
```

Again, since Nominal has no `cycles` time unit (representing an operational cycle of a jet engine),
the example uses `hours` as a proxy.

If you navigate to your organization's [Datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets),
you'll see this dataset at the top:

![relative-dataset-list](/guides/images/b4/relative_dataset_list_tnajr7.png)

If you click on the dataset and inspect its metadata, you'll see that the timestamp type is set to "relative:"

![relative-dataset-list](/guides/images/b4/relative_dataset_detail_pgi1bu.png)

```{include} /guides/_snippets/timestamp-types.md
```

## Retrieve a Dataset

To download a Dataset on the Nominal platform, you'll first need to obtain the Dataset's resource identifier.

If you click on a Dataset from the [Datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets),
you can copy/paste its RID from the metadata drawer:

![rid-copy-paste](/guides/images/b4/dataset_rid_nsbe2y.png)

With the Dataset RID, download it in Python with {py:obj}`client.get_dataset(rid) <nominal.core.NominalClient.get_dataset>`:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

id = 'ri.catalog.cerulean-staging.dataset.e5ede17b-05f9-404d-aaf5-ba85c99761a2'

ds = client.get_dataset(id)
```

`ds` is a {py:obj}`Dataset <nominal.core.Dataset>` object
that contains the dataset's metadata (name, description, labels, etc). 

With the `ds` object that {py:obj}`client.get_dataset(id) <nominal.core.NominalClient.get_dataset>` returns,
you can inspect the Dataset's metadata. To export the actual data content, see the [Export data](/guides/datasets/export-data.md) guide.

```{include} /guides/_snippets/what-is-a-rid.md
```

## Set labels and properties

Inspect the labels and properties of this dataset:

```python
ds.properties
```
`{'Fault Modes': 'HPC Degradation'}`

```python
ds.labels
```
`('Simulation', 'NASA', 'Training data')`

The dataset's detail page shows these same labels and properties:

![dataset-labels-properties](/guides/images/b4/relative_dataset_labels_properties_ubecys.png)

You can set a dataset's labels and properties through the {py:obj}`Dataset.update() <nominal.core.Dataset.update>`
function.

For example, to remove a dataset's labels and properties:

```python
ds.update(
    labels = [],
    properties = {}
)
```

To set the dataset's labels and properties back to their original values:

```python
ds.update(
    labels = ['Simulation', 'NASA', 'Training data'],
    properties = {'Fault Modes': 'HPC Degradation'}
)
```

If you're appending to a dataset's labels or properties, you'll want to cache the originals to not overwrite them:

```python
existing_labels = list(ds.labels)
new_labels = ['Sea Level', 'Fan Failure']
ds.update(labels = existing_labels + new_labels)
```

## Append to a Dataset

You can append to an existing dataset with a CSV that has the same columns.

For example, download 1000 rows of this [flight test data](https://huggingface.co/datasets/nominal-io/frosty-flight/blob/main/frosty_flight_1k_rows.csv).
Split it into 2 dataframes that are 500 rows each and create a Nominal dataset with the first one:

```python

from nominal.core import NominalClient
from nominal.thirdparty.pandas import upload_dataframe

client = NominalClient.from_profile("default")  # replace with your profile name

df_1000_rows = pl.read_csv('hf://datasets/nominal-io/frosty-flight/frosty_flight_1k_rows.csv')

df_first_chunk = df_1000_rows.slice(0, 500)  # First 500 rows
df_second_chunk = df_1000_rows.slice(500, 1000)  # Next 500 rows
df_second_chunk.write_csv('frosty_flight_2nd_chunk.csv')
dataset = upload_dataframe(
    client,
    df_first_chunk.to_pandas(),
    name = 'Frosty Flight: First 500 rows',
    timestamp_column = 'source_time',
    timestamp_type = 'iso_8601'
)
```

Add the 2nd 500 rows with {py:obj}`Dataset.add_tabular_data() <nominal.core.Dataset.add_tabular_data>`.

```python
dataset.add_tabular_data(
    path = 'frosty_flight_2nd_chunk.csv',
    timestamp_column='source_time',
    timestamp_type='iso_8601'
)
```

Update the dataset name as well:

```python
dataset.update(name = 'Frosty Flight: Full 1000 rows')
```

You're now familiar with the most common ways of interacting with Nominal's Dataset primitive in Python.
For all methods available in the `Dataset` class, please see the {py:obj}`Function Reference <nominal.core.Dataset>`.
