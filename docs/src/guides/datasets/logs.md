---
myst:
  html_meta:
    description: "Parse timestamped log entries, write them to a dataset, and view them with asset or run data."
---

# Logs in Python

{.lead}
Parse timestamped log entries and write them to a dataset's log channel.

Logs belong to a **dataset**, alongside its other channels. An asset can reference that
dataset, and a run can select a time interval using directly attached datasets or
associated assets. Neither an asset nor a run is required to write logs.

This guide reads a small text file, preserves each entry's timestamp and severity,
and sends the entries with {py:meth}`~nominal.core.Dataset.write_logs`. The optional
sections show how to make the same dataset available through an asset or run.

## Connect to Nominal

Install the SDK with `pip install nominal` and
[configure an authentication profile](/guides/authentication.md) before running the examples.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.create_dataset(name="Engine test logs")
```

To add logs to an existing dataset, use `client.get_dataset("<DATASET_RID>")` instead of
creating one. A dataset can contain both logs and telemetry; use the same absolute
clock for both so they line up during analysis.

## Parse log entries

Save these synthetic entries as `engine.log` in your working directory:

```text
2026-09-10T14:38:19Z [INFO] Sent ACTUATE_MOTOR command
2026-09-10T14:38:20Z [WARN] Motor temperature above target
2026-09-10T14:38:21Z [INFO] Motor returned to target temperature
```

Each line has an ISO 8601 timestamp, a bracketed level, and a message. The `Z` suffix
means UTC. Adapt the parser to your actual log format; it expects all three fields
on every nonblank line. Missing fields or invalid timestamps raise an error.

```python
from typing import Iterable

from nominal.core import LogPoint


def parse_logs(file_path: str) -> Iterable[LogPoint]:
    with open(file_path, encoding="utf-8") as log_file:
        for line in log_file:
            if not line.strip():
                continue
            timestamp, level, message = line.rstrip("\r\n").split(maxsplit=2)
            yield LogPoint.create(
                timestamp=timestamp,
                message=message,
                args={"level": level.removeprefix("[").removesuffix("]")},
            )
```

{py:class}`~nominal.core.LogPoint` holds an absolute timestamp, a message, and structured
string arguments. This parser stores the severity in `args["level"]` and keeps the
rest of the message intact, including spaces. `LogPoint.create` also accepts a
timezone-aware datetime or integer nanoseconds since the Unix epoch. Pass `args=None`
when an entry has no structured arguments.

## Write logs to the dataset

```python
dataset.write_logs(parse_logs("engine.log"), channel_name="engine_logs")
print("Dataset:", dataset.nominal_url)
```

The parser yields entries as it reads the file. `write_logs` consumes that iterable
and sends batches of 1,000 entries by default; set `batch_size` to change the number
of entries per API request. The default channel name is `"logs"`; this example uses
`"engine_logs"` to identify the source.

The call blocks until all batches have been sent to the API. It returns `None`, not a
`DatasetFile` or an ingestion job, so there is no file handle to poll. This sends parsed
log entries; it does not upload or preserve the original text file as a dataset file.

Earlier batches may already have been sent if parsing or a later request fails.
The operation is not an all-or-nothing file upload; account for partial progress before
retrying a file.

## View the logs

Open the dataset in Nominal and use a workbook's **Logs** panel to inspect the
`engine_logs` channel. You can view it alongside telemetry that covers the same times.
If you work through an asset or run, attach or associate the dataset as described below,
then select its log channel in the workbook.

For querying outside the app, see the [SQL interface](https://docs.nominal.io/developers/sql/overview).

## Use the dataset with an asset

An asset's named data scope references the dataset. Attaching it does not copy the logs:

```python
asset = client.create_asset(name="Engine test stand")
asset.add_dataset(data_scope_name="engine", dataset=dataset)
```

For an existing asset, retrieve it with `client.get_asset("<ASSET_RID>")`. If the dataset
is already attached, you can keep writing logs to it without attaching it again.
See the [Assets overview](/guides/assets/overview.md) for managing data scopes.

## Use the dataset with a run

A standalone run can reference the dataset directly. This interval covers the sample
entries above; choose your actual test interval for your own recording.

```python
from datetime import datetime, timezone

run = client.create_run(
    name="Engine temperature check",
    start=datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc),
    end=datetime(2026, 9, 10, 14, 38, 22, tzinfo=timezone.utc),
)
run.add_dataset(ref_name="engine", dataset=dataset)
```

If the dataset belongs to an asset, you can instead create an asset-associated run
for the test interval. That run uses the asset's data scopes; it does not require
another copy of the logs. See the [Runs overview](/guides/runs/overview.md) for the
standalone and asset-associated workflows.
