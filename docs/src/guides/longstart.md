---
myst:
  html_meta:
    description: "Upload a racecar CSV to a vehicle asset, stream live data to it and watch the data arrive in a workbook."
---

# Upload and stream tutorial

{.lead}
Upload a race car's recorded telemetry to a vehicle asset, stream live data to the same asset, and watch it arrive in a workbook.
It uses two complete scripts, run from the terminal.

You need the [installed client](/guides/install.md) and a [profile](/guides/api-keys-setup.md). The scripts take the profile name with `--profile`.

## Upload a CSV to an asset

1. Download the sample data. Nominal ingests the gzipped CSV as it is:

    {download}`Download racecar_dataset.csv.gz </guides/data/racecar_dataset.csv.gz>`

1. Save this script as `fsae_asset_upload.py`. It finds or creates the `FSAE CT8 Vehicle` [asset](/guides/assets.md), uploads the CSV to its dataset and sets the channels' units:

    ```{literalinclude} /../examples/fsae_asset_upload.py
    :language: python
    ```

1. Run it:

    ```shell
    python fsae_asset_upload.py --profile <PROFILE> --file racecar_dataset.csv.gz
    ```

1. In the [Assets page](https://app.gov.nominal.io/assets), open `FSAE CT8 Vehicle`. The `racecar_dataset` file appears under **Data sources**:

    ```{video} https://res.cloudinary.com/didkpxvqu/image/upload/v1766096480/Documentation/quickstart/qq2_fast.mp4
    :loop:
    :alt: The uploaded racecar_dataset file under the asset's Data sources tab
    ```

## Stream data to the asset

1. Save this script as `stream_data_to_dataset.py`. It streams a sine wave, temperature, CPU use and a `cpu_info` struct to the same dataset until you stop it. See [Stream data](/guides/streaming/overview.md) for how streaming works.

    ```{literalinclude} /guides/_snippets/code/sdk/python/getting_started/stream_data_to_dataset.py
    :language: python
    ```

1. Run it, and leave it running for the next section:

    ```shell
    python stream_data_to_dataset.py --profile <PROFILE>
    ```

    It prints a session ID and tags every point with it as `tutorial_session_id`.

## Watch the data arrive

1. Open `FSAE CT8 Vehicle`, click **New workbook** and select **New empty workbook**.
1. Drag `sine_wave`, `temperature` or `cpu_utilization` from **Channel search** onto the chart.
1. Click **Go Live** in the bottom left. New points appear as the script sends them.

    ```{video} /guides/images/b5/stream-gif.mp4
    :loop:
    :alt: Streaming data flowing into a Nominal workbook
    ```

`cpu_info` is a struct, so a time series chart can't plot it directly.
Drag it onto a **Value grid** panel, or [transform it](https://docs.nominal.io/core/documentation/platform/workbooks/manipulating-time-series#transforming-variables) to a numeric field such as `frequency_ghz`.

If other people are running this tutorial in your workspace, show only your points with a tag filter:

1. In the workbook's **Inputs** panel, click **+** and set **Type** to **Tag filter**.
1. Set **Tag key** to `tutorial_session_id`, paste your session ID into **Tag values** and click **Save input**.
1. For channels you add later, click **+** next to the input and select **Apply to all variables**.

![Adding a tutorial_session_id tag filter in the workbook Inputs panel](/guides/images/b5/workbook-tutorial-session-filter.png)

Press `Ctrl+C` to stop streaming.

## Clean up

Archive the asset from its options menu on the Assets page, or from Python:

```python
client.get_asset("<ASSET_RID>").archive()
```

Archiving hides the asset without deleting its data. Only a workspace admin can delete the dataset.

To query the data with SQL, see the [SQL interface](https://docs.nominal.io/developers/sql/overview).
