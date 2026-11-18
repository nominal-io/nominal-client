---
myst:
  html_meta:
    description: "Write timestamped frequency bins to a compatible dataset."
---

# Write Frequency data

{.lead}
Write timestamped frequency bins with the Nominal Python Client. You need a deployment
with Frequency ingestion enabled and an existing compatible dataset.
The server rejects writes when the dataset's storage backing does not support Frequency.

```{include} /guides/_snippets/connect-the-client.md
```

## Write a spectrum

{py:meth}`~nominal.core.Dataset.write_frequency` sends parallel timestamp, frequency
and value columns in one call. Repeat the capture timestamp for each bin.

```python
dataset = client.get_dataset("<Frequency dataset RID>")
# 2026-01-01T12:00:00.000000123Z
t0 = 1767268800000000123
dataset.write_frequency(
    "Acceleration",
    [t0] * 3,
    [10.0, 20.0, 30.0],  # Hz
    [0.25, 0.5, 0.125],
    unit="g^2/Hz",
    tags={"sensor": "x"},
)
```

Timestamps must be integral UTC epoch nanoseconds. The three columns
must have equal nonzero lengths, and frequencies and measurements must be finite.
Negative frequencies are accepted. A unit applies when the channel is first created.

## Limits and delivery

The call returns `None` when the batch is queued. Storage completes asynchronously.
Each call accepts up to 10,000 points within a 262,144-byte serialized protobuf request limit;
metadata and timestamps can reach the byte limit first. Send smaller batches explicitly
when needed.

Failed admissions are not automatically retried. Transport and admission errors raise
{py:class}`~nominal.exceptions.NominalError` or one of its subclasses.
A lost response leaves acceptance unknown, so repeating a call can duplicate data.
Successful earlier calls remain accepted if a later call fails.

This method does not read back Frequency data. Verify storage separately before treating
queued writes as delivered.
