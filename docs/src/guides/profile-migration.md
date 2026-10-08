---
# hidden in the Fern nav: linked from Authenticate, not listed in the sidebar
orphan: true
---

# Migration to the Profile API

There are two recent API changes that work in tandem:

1. Instead of calling methods directly on the `nominal` module (often
   imported as `nm`), it is now recommended to first create a
   {py:obj}`NominalClient <nominal.core.NominalClient>`,
   and to call methods on the client.
2. Instead of creating a client from token, Nominal recommends creating the
   client from a *profile* stored on disk, that encapsulates the root
   URL, token, and workspace.

## Migrating existing credentials

If you used `nom` to store credentials before profiles existed, migrate
your old configuration file (`~/.nominal.yml`) to the new format
(`~/.config/nominal/config.yml`).

You can do this with the following command:

```shell
nom config migrate

# Or, if `nom` is missing from your path:
python -m nominal.cli config migrate
```

## Creating credentials from scratch

See [Authentication](/guides/authentication.md).

## Creating a client from a profile

All operations will happen via the {py:obj}`NominalClient
interface <nominal.core.NominalClient>`. You
can construct a Client from profile or from token (see
[Authentication](/guides/authentication.md) for more detail).

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
```

## Migration patterns

Some of the search and listing functions from the old `nominal`
namespace are still available on the `NominalClient`, but most *creation* functions have a slightly different API.
Here are a few common patterns:

### Uploading CSVs to a dataset

Best practice is to upload multiple CSV of the same *type* into one
{py:obj}`Dataset <nominal.core.Dataset>`:

```python
# Do this once
dataset = client.create_dataset(
    name='engine',
    description='Engine measurements from multiple flights'
)

# Repeat this for each CSV file of the same type
dataset.add_tabular_data(
    path='my_data.csv',
    timestamp_column='time',
    timestamp_type='epoch_seconds'
)
```

See also [Timestamp reference](/reference/ts.md).

### Uploading Pandas dataframes

If you want to upload *multiple* dataframes into a single dataset, do so as follows:

```python

dataset = client.create_dataset(
    name='temperature',
    description='Engine temperate measurements from multiple flights'
)

for df in my_dataframes:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        df.to_parquet(f.name)
        dataset.add_tabular_data(
            f.name,
            timestamp_column='source_time',
            timestamp_type='iso_8601'
        )
```

Note that, if your timestamp column is the index, but the index is
unnamed, you should assign a name to it, e.g.:

```
df.index.name = 'source_time'
```

### Creating runs from data

Previously, it was possible to create a run directly from a CSV
dataset. Our concept of a run has since evolved: whereas before a run
was seen as a *container* of data, now it is seen as a time bounded
*view* onto data.

Typically, a run would therefore specify the start and end time of an
experiment (a flight, e.g.), and will narrow all data to that those
time bounds.

However, if you want to replicate the ability to create a run from
data, you can do so as follows:

1. Upload data
2. Get the data bounds
3. Create a run that matches those bounds

```python
# Wait until data is fully ingested, and time bounds have been computed
for f in dataset.list_files(successful_only=False):
    f.poll_until_ingestion_completed()
dataset.refresh()

run = client.create_run(
    name='Experimental run',
    description='First experimental run of Q3',
    start=dataset.bounds.start,
    end=dataset.bounds.end
)
```

### Uploading videos

Video is stored as a channel on a Dataset, alongside your other test data. Create
the Dataset, then add video files to a named channel on it:

```python
dataset = client.create_dataset(name='Engine Cam 6')

dataset.add_video(
    path='myfile.mp4',
    channel='engine_cam_6',
    start=dataframe['source_time'].min()
)
```

:::{note}

Standalone `Video` objects are deprecated in favor of video channels. `client.create_video()`,
`Video.add_file()`, `Asset.add_video()`, `Run.add_video()` and the rest of the `Video` API emit a
`LegacyVideoDeprecationWarning`. Use {py:obj}`Dataset.add_video() <nominal.core.Dataset.add_video>`
and attach the backing Dataset instead.
:::

### Writing logs

Logs are no longer separate objects, but are now entries added to an
existing Datasets.

Consider, e.g., a log with lines formatted as:

```
2025-04-08T14:26:28.679052Z [INFO] Sent ACTUATE_MOTOR command
```

You can add matching log entries to your Dataset like this:

```python
from nominal.core import LogPoint

def parse_logs_from_file(file_path):
    with open(file_path, "r") as f:
        for line in f:
            timestamp, message = line.removesuffix("\n").split(maxsplit=1)
            yield LogPoint.create(timestamp, message, None)

dataset = client.get_dataset("dataset_rid")
logs = parse_logs_from_file("logs.txt")
dataset.write_logs(logs)
```
