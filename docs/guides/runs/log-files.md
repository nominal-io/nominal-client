---
myst:
  html_meta:
    description: "Add log files (such as flight logs) to a Nominal Run in Python"
---

# Add logs to a Run

{.lead}
Add log files (such as flight logs) to a Nominal Run in Python

```{include} /guides/_snippets/install-warning.md
```

Log files are a standard output from aircraft, land vehicles, manufacturing equipment, 
and practically any present-day machine with an on-board computer.

Nominal makes it simple to upload log files to a Nominal *Run* for collaborative 
inspection, root-cause analysis, and automated alerting.

```{include} /guides/_snippets/what-is-a-run.md
```

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create a run

Consider the following dataset:

```{literalinclude} /guides/_snippets/code/sdk/python/logs/read_csv_data.py
:language: python
```

Next, upload that data to Nominal:

```{literalinclude} /guides/_snippets/code/sdk/python/logs/create_dataset_for_run.py
:language: python
```

This dataset is part of an experimental run. Create the run, and attach the dataset:

```{literalinclude} /guides/_snippets/code/sdk/python/logs/create_run_and_add_dataset.py
:language: python
```

If you navigate to your organization's [Runs page](https://app.gov.nominal.io/runs),
you'll see a run at the top called `Frosty Flight`.

## Generate log entries

Next, generate demo log entries and add them to the run.

To make the demo log entries visually interesting and distinct, add a random sparkline chart to each log message:

```{literalinclude} /guides/_snippets/code/sdk/python/logs/generate_sparkline.py
:language: python
```

Nominal log entries are defined in Python as a list of {py:obj}`LogPoint <nominal.core.LogPoint>` objects:

```{literalinclude} /guides/_snippets/code/sdk/python/logs/generate_logs.py
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

```{literalinclude} /guides/_snippets/code/sdk/python/logs/write_logs_to_dataset.py
:language: python
```

If you visit the Datasets tab of the "Frosty Flight" Run page, you'll see a logs {abbr}`channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` in the "Data scopes" tab.

To inspect the logs in the app:

1. Open the run in an empty workbook
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
