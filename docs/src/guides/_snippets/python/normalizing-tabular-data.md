Tabular data comes in a large variety of formats, ranging from simple `.csv` files to complex proprietary binary formats.
Prior to uploading data, it should be converted into a format supported by Nominal.
Today, the most commonly used intermediary file formats are CSV and Parquet.
Parquet is generally preferred, since it produces smaller files that can be ingested faster than CSV files.

The platform imposes a few additional requirements:

* Only floating point and string columns are supported.
    :::{note}

    Nominal is considering including other data types, such as vectors, uint64, and others.
    Please contact your Nominal representative if this is of interest.
    :::

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

    | timestamp | gps.lat | gps.lon | gps.height | imu.roll | imu.pitch | imu.roll |
    | --- | --- | --- | --- | --- | --- | --- |
    | 12345 | 12.1 | 12.3 | 10000 | 1.23 | -0.2 | 0.0 |

    When creating the dataset using the Python client, you must specify a `prefix_delimiter` of `"."` for the columns to be interpreted hierarchically.

    ::::{tip}

    When flattening data, do not feel compelled to jam-pack it all into a single file to upload to Nominal.
    In many cases, it is easier to produce a folder of `.csv` or `.parquet` files and to upload those in a for-loop.
    You can upload as many files as you want to a dataset, and they will all be combined into a unified source.

    This method can be used both to *concatenate* and to *join* additional data across files. New columns will result in additional channels being created, and new timestamps for existing channels will add additional data to those existing channels.

    :::{warning}

    Uploading data from multiple files to the same channels with duplicate timestamps will overwrite existing data.
    :::
    ::::
