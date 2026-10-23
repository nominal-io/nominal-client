---
myst:
  html_meta:
    description: "Upload, stream, organize and read back data with the Nominal Python Client."
---

# Python quickstart

{.lead}
A simple quickstart to show some basic features of `NominalClient`.


To run this code locally, you need nominal-client  [installed](/guides/install.md) and your [API keys](/guides/api-keys-setup.md) setup.

 

## Connect

The first step is to open a connection to nominal through the NominalClient.  
```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default") # replace if different 
```

## Upload Data

Next, we synthesize some sample data and upload it to nominal. An asset represents the system you test, such as a vehicle or a test stand.
Its datasets hold the channels you record.

### Synthesize Data 
Generate one minute of sample engine data as a CSV:

```python
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

# A unique ID keeps this run's asset separate from anyone else's.
run_id = uuid.uuid4().hex[:8]
start = datetime.now(timezone.utc) - timedelta(minutes=1)

t = np.arange(0, 60, 0.1)  # 10 Hz for 60 seconds
pd.DataFrame({"timestamp": start.timestamp() + t,
              "rpm": 3000 + 500 * np.sin(t / 5),
              "coolant_temp_c": 80 + t / 6}
).to_csv("engine.csv", index=False)
```
### Upload  
Create the asset and a dataset on it, then upload the file:

```python
asset = client.get_or_create_asset_by_properties(
    {"serial": f"QS-{run_id}"},  # properties identify the physical system
    name=f"Quickstart engine {run_id}",
)
dataset = asset.get_or_create_dataset("engine")

file = dataset.add_tabular_data("engine.csv", timestamp_column="timestamp", timestamp_type="epoch_seconds")
file.poll_until_ingestion_completed()

print(asset.nominal_url)
```

Open the printed URL to see the asset and its `engine` dataset in Nominal.
Every other column in the CSV becomes a channel: `rpm` and `coolant_temp_c`.
For other formats, see [Datasets](/guides/datasets/overview.md).

## Stream live data

A write stream sends values as your code produces them, batching them in the background.
This block streams 10 seconds of values to the same dataset:

```python
import time

with dataset.get_write_stream() as stream:  # the stream flushes when the block exits
    for _ in range(100):
        now = datetime.now(timezone.utc)
        stream.enqueue("rpm", now, 3000 + 200 * np.random.randn())
        stream.enqueue("oil_pressure_kpa", now, 300 + 5 * np.random.randn())
        time.sleep(0.1)
```

To watch values arrive, open a workbook on the asset and click **Go Live**.
See [Streaming](/guides/streaming/overview.md) for the full set of options.

## Mark a run and an event

A run names a span of time on an asset, such as one test.
An event marks a moment or span inside it that a reviewer should see.

```python
from nominal.core import EventType

end = datetime.now(timezone.utc)
run = asset.create_run(f"Quickstart run {run_id}", start=start, end=end)
event = asset.create_event(
    "Coolant above 85 °C",
    EventType.FLAG,
    start=start + timedelta(seconds=30),
    duration=timedelta(seconds=30),
)

print(run.nominal_url)
```

Read more about [runs](/guides/runs.md) and [events](/guides/events.md).

## Read data back

`datasource_to_dataframe` downloads channels into a pandas DataFrame indexed by timestamp.

```python
from nominal.thirdparty.pandas import datasource_to_dataframe

df = datasource_to_dataframe(dataset, channel_exact_match=["rpm"])
print(df.describe())
```

The result includes both the uploaded and the streamed `rpm` values.
To export to files or decimate long recordings, see [Export data](/guides/datasets/export-data.md).

## Clean up

Archiving hides the objects from views and searches. It doesn't delete the data.

```python
event.archive()
run.archive()
dataset.archive()
asset.archive()
```

## Next steps

* [Upload and stream tutorial](/guides/longstart.md): a longer walkthrough with a racecar dataset and live workbooks.
* [Data integration tutorial](/guides/data-integration-tutorial.md): connect your own data sources.
* [Examples](/examples/index.md): runnable notebooks.
* [SDK reference](/sdk/index.md): every class and function.
