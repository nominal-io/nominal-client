:::{note}

`timestamp_type` accepts either a string, for absolute timestamps, or a type from
[`nominal.ts`](/sdk/ts.md).

Acceptable string values are:

    `iso_8601`,
    `epoch_picoseconds`,
    `epoch_nanoseconds`,
    `epoch_microseconds`,
    `epoch_milliseconds`,
    `epoch_seconds`,
    `epoch_minutes`,
    `epoch_hours`,
    `epoch_days`

    Timestamps in the form `2024-06-08T05:58:42.000Z` will have a `timestamp_type` of `iso_8601`.

    Timestamps in the form `1581933170999989` will most likely be `epoch_microseconds`.

    `epoch_` timestamps refers to timestamps in [Unix format](https://en.wikipedia.org/wiki/Unix_time).

Relative timestamps have no string form. Use `nominal.ts.Relative`, which pairs a
unit with the absolute time that the relative values are measured from:

```python
import nominal.ts as ts

ts.Relative("seconds", start=0)  # relative to the Unix epoch
```

The `start` argument is required, and accepts a `datetime` or epoch nanoseconds.
Valid units are `picoseconds`, `nanoseconds`, `microseconds`, `milliseconds`,
`seconds`, `minutes`, `hours`, and `days`.

For more information about Nominal timestamps in Python, see the
[`nominal.ts`](/sdk/ts.md) docs page.
:::
