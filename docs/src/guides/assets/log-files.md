---
myst:
  html_meta:
    description: "Add log files (such as flight logs) to a Nominal Asset in Python"
---

# Add Logs to an Asset

{.lead}
Add log files (such as flight logs) to a Nominal Asset in Python

```{include} /guides/_snippets/install-warning.md
```

Log files are a standard output from Assets such as aircraft, land vehicles, manufacturing 
equipment, and practically any present-day machine with an on-board computer.

Nominal makes it simple to upload log files to a Nominal *Asset* for collaborative 
inspection, root-cause analysis, and automated alerting.

:::{note}

In Nominal, *Assets* are containers of multimodal test data - including *Datasets*, *Videos*, *Logs*, and database connections.

To see your organization's latest Assets, head over to the [Assets page](https://app.gov.nominal.io/assets)
:::

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create an asset

First, create an empty asset to upload the log file to:

```python
import polars as pl
from huggingface_hub import hf_hub_download
from nominal.core import NominalClient

# Download the file using huggingface_hub, then read with Polars
csv_path = hf_hub_download(
    repo_id="nominal-io/frosty-flight",
    filename="frosty_flight_1k_rows.csv",
    repo_type="dataset",
)
df = pl.read_csv(csv_path)
df.write_csv("frosty_flight_1k_rows.csv")

client = NominalClient.from_profile("default")  # replace with your profile name

dataset = client.create_dataset(
    name="Frosty Flight measurements",
)
dataset.add_tabular_data(
    "frosty_flight_1k_rows.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)

uav_drone_asset = client.create_asset(name="Frosty Flight")
uav_drone_asset.add_dataset(dataset=dataset, data_scope_name="dataset")

print(uav_drone_asset.rid)
```

If you navigate to your organization's [Assets page](https://app.gov.nominal.io/assets),
you'll see an asset at the top called "'Frosty Flight."

## Generate log entries

Generate demo log entries to add to the asset.

To make the demo log entries visually interesting and distinct, add a random sparkline chart to each log message:

```{literalinclude} /guides/_snippets/code/sdk/python/logs/generate_sparkline.py
:language: python
```

Nominal log entries are defined in Python as a list of {py:obj}`LogPoint <nominal.core.LogPoint>` objects:

```{literalinclude} /guides/_snippets/code/sdk/python/assets/generate_logs.py
:language: python
```

```
('2024-06-08T05:58:42.000Z', 'Log message 0 ▅▅▅▂▅█▆▆▁▅▆▅▇███▆▅▄█')
('2024-06-08T05:58:51.000Z', 'Log message 1 ▆▃▃▃▅▃▄▃▅▁▁▂▁▄▄█▄▇▂▇▆▁▇█▃▁▅▆▃▁█▆')
('2024-06-08T05:58:52.000Z', 'Log message 2 ▁▂▄▃▄▄▂▄▃▆▁▇▁▄▅▄▄▄▄█▁▄▇▂▂▁')
('2024-06-08T05:58:52.000Z', 'Log message 3 ▁█▅▅▆▃▇▂█▂▇▃▄▂▇▆▁▁▄▃▄▁█▄█▂')
('2024-06-08T05:58:52.000Z', 'Log message 4 ▄██▁▄▆▃▁█▄▃▂▄▁▇▄')
```

Finally, write the logs to the existing dataset using {py:obj}`dataset.write_logs() <nominal.core.Dataset.write_logs>`:

```python
dataset.write_logs(logs)
```

If you visit the Datasets tab of the "Frosty Flight" Asset page, you'll see a logs {abbr}`channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` in the datasets table.

To inspect the logs in the app:

1. Open the asset in an empty workbook
2. Click "Add panel" and select the "Logs" panel
3. In the channel list, drag and drop the logs channel into the "Logs" panel

To inspect them without leaving your terminal, consider using the [SQL interface](https://docs.nominal.io/developers/sql/overview) to query the `logs` table instead:

```sql
SELECT ts, channel, message
FROM logs
WHERE dataset_rid = '<dataset-rid>'
ORDER BY ts
LIMIT 100
```
