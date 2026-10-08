---
myst:
  html_meta:
    description: "Getting started with the Nominal Python client."
---

# Python quickstart

{.lead}
Getting started with the Nominal Python client.

This page walks through creating an asset, uploading a CSV dataset, and streaming continuous data to it using the Nominal SDK for Core in Python. To do this in the Nominal app instead, see the [quickstart: See your data in Nominal](https://docs.nominal.io/core/documentation/getting-started/quickstart).

Commands on this page use these placeholders. Replace each with your own value as you go:

| Placeholder | Value |
|---|---|
| `<WORKSPACE_RID>` | the Nominal Core workspace's resource ID (`ri.organization.workspace....`) |
| `<NOMINAL_API_KEY>` | your Nominal API key (`nominal_api_...`) |
| `<API_URL>` | the API base URL for your Nominal Core app (for example `https://api.gov.nominal.io/api`) |
| `<PROFILE>` | a short name for your saved credentials (for example `default`) |
| `<DATASET_FILE_PATH>` | full path to the CSV file you'll upload (`/full/path/to/racecar_dataset.csv.gz`) |

## Gather your Nominal credentials

Copy your workspace RID, API key, and API URL from the Nominal app. You'll use them as `<WORKSPACE_RID>`, `<NOMINAL_API_KEY>` and `<API_URL>` below.

:::{dropdown} Where to find these values

Open your organization's **API keys** page  in Settings:

```{button-link} https://app.gov.nominal.io/settings/org/api-keys
:color: primary

Go to API keys page
```

1. **API key:** Click **Create key**, name it, set an expiration, and click **Generate key**. Copy the key and keep it for `<NOMINAL_API_KEY>`. _You won’t be able to see it again. Store your API key securely to protect your Nominal data._
1. **API URL:** On the same page, click **Copy URL** and keep it for `<API_URL>`.
1. **Workspace RID:** On the same page, find your workspace in the list of all the organization’s workspaces, click **Copy RID**, and keep it for `<WORKSPACE_RID>`.

![Settings API keys page](/guides/images/b5/settings-api-keys-page.png)
:::

## Set up your environment

Pick a short profile name (`<PROFILE>`) to save your Nominal credentials under. Then download the sample CSV you'll upload later in this guide.

Download the `racecar_dataset.csv.gz` sample data file; its full path is `<DATASET_FILE_PATH>` below. It's a gzipped CSV, which Nominal ingests as is, so there's no need to unzip it:

{download}`Download racecar_dataset.csv.gz </guides/data/racecar_dataset.csv.gz>`

Set up a Python virtual environment so the SDK has an isolated place to live:

```{include} /guides/_snippets/python/setup.md
```

## Install the Nominal SDK for Core in Python

```{include} /guides/_snippets/python/installing-python-client.md
```

## Save your credentials as a Nominal profile

In your terminal, run this command to save the credentials you entered above as a Nominal profile:

```shell
nominal config profile add '<PROFILE>' -t '<NOMINAL_API_KEY>' -w '<WORKSPACE_RID>' -u '<API_URL>'
```

Confirm the profile was written:

```shell
nominal config profile list
```

## Build an asset with data in Nominal

```{include} /guides/_snippets/assets-intro.md
```

Run the following script to create an {abbr}`asset (Representation of physical or simulated devices or systems that you're testing (example: vehicles, components, or testbeds).)` and upload the CSV you downloaded earlier:

1. Create a new file named `fsae_asset_upload.py` and copy the following code into it:

    ```{literalinclude} /../../examples/fsae_asset_upload.py
    :language: python
    ```

1. Upload data to Nominal by running the script in your terminal from your virtual environment, passing the profile name and CSV path as flags:

    ```shell
    python fsae_asset_upload.py --profile '<PROFILE>' --file '<DATASET_FILE_PATH>'
    ```

    Run `python fsae_asset_upload.py --help` to see all available flags.

## Verify your data in Nominal

Verify that your Python script successfully created an asset and uploaded data to it:

1. Open the Nominal app, and go to the **Assets** tab in the left sidebar:

    ```{button-link} https://app.gov.nominal.io/assets
    :color: primary

    Go to Assets page
    ```
1. Click on the `FSAE CT8 Vehicle` asset you created and verify the uploaded `racecar_dataset` file appears under the **Data sources** tab:

    ```{video} https://res.cloudinary.com/didkpxvqu/image/upload/v1766096480/Documentation/quickstart/qq2_fast.mp4
    :loop:
    :alt: The uploaded racecar_dataset file under the asset's Data sources tab
    ```

## Stream data to Nominal

You can also stream continuous data to an asset in real time. Run the following script to stream sine, temperature, CPU utilization, and CPU info struct channels to the same `FSAE CT8 Vehicle` asset and dataset you created above:

1. Create a new file named `stream_data_to_dataset.py` and copy the following code into it:

    ```{literalinclude} /guides/_snippets/code/sdk/python/getting_started/stream_data_to_dataset.py
    :language: python
    ```

1. Run the script from your virtual environment:

    ```shell
    python stream_data_to_dataset.py --profile '<PROFILE>'
    ```

    :::{note}

    Keep the script running while you complete the next section. Press `Ctrl+C` to stop streaming when you're done.
    :::

## View your streaming data in Nominal

While the streaming script is still running, create a workbook to watch data arrive in real time:

1. Open the Nominal app, and go to the **Assets** tab in the left sidebar:

    ```{button-link} https://app.gov.nominal.io/assets
    :color: primary

    Go to Assets page
    ```
1. Click the `FSAE CT8 Vehicle` asset, then click **New workbook** in the top right and select **New empty workbook**.
1. Drag `sine_wave`, `temperature`, or `cpu_utilization` from **Channel search** onto the time series chart.
1. `cpu_info` is a struct channel, so it cannot render directly on a time series chart. View it in one of two ways:
    * **Value grid:** Add a **Value grid** panel to the workbook and drag `cpu_info` onto a cell. The grid updates as new points stream in.
    * **Time series chart:** Apply a transform to `cpu_info` to extract a numeric subfield (for example, `frequency_ghz`), then drag the derived series onto the time series chart. See [Transforming variables](https://docs.nominal.io/core/documentation/platform/workbooks/manipulating-time-series#transforming-variables).
1. Click **Go Live** in the bottom left of the workbook to start streaming. New points appear as the script continues streaming:

    ```{video} /guides/images/b5/stream-gif.mp4
    :loop:
    :alt: Streaming data flowing into a Nominal workbook
    ```

::::{dropdown} Running this tutorial alongside other people? Filter the workbook to only your session's data.
:icon: people

When the streaming script runs, it prints a `Tutorial session ID` (a UUID) in your terminal and tags every point it enqueues with `tutorial_session_id = <that UUID>`.

Run the same streaming script as in the previous step:

```shell
python stream_data_to_dataset.py --profile '<PROFILE>'
```

Your terminal shows something like:

```text
Tutorial session ID: c066ff84-89a0-43aa-9d3c-2e14b5e44385
Filter by tag 'tutorial_session_id' = 'c066ff84-89a0-43aa-9d3c-2e14b5e44385' in Nominal
to isolate this stream.

Asset: FSAE CT8 Vehicle, RID: ri.scout.cerulean-staging.asset.4a5ce8bd-7425-478f-b279-1bfc742c2bde
Dataset: FSAE CT8 Vehicle dataset, RID: ri.catalog.cerulean-staging.dataset.13007ff7-ad41-4ac0-a5b5-ed0b922b97ba
Streaming all set up and queueing has started! Press ctrl+c to terminate streaming.
```

Copy the UUID from the output, then paste it into a workbook tag filter:

1. In the workbook's **Inputs** panel (left sidebar), click **+**.
1. Set **Type** to **Tag filter**.
1. Set **Tag key** to `tutorial_session_id`.
1. Paste your UUID into **Tag values**, then click **Save input**.

![Adding a tutorial_session_id tag filter in the workbook Inputs panel](/guides/images/b5/workbook-tutorial-session-filter.png)

The chart now shows only the points this streaming session has uploaded.

If you drag additional channels onto the chart after adding the filter, click the **+** next to the `tutorial_session_id` input and select **Apply to all variables** so the new channels also respect the filter.

::::

Congrats! You have successfully created an asset, uploaded a CSV dataset, and streamed continuous data to it using the Nominal SDK for Core in Python.

## Clean up

```{include} /guides/_snippets/quickstart-cleanup.md
```

You can also archive the asset using the Python SDK:

```{code-block} python
:caption: archive_asset.py
my_asset.archive()
```

## Next steps

* [Create a workbook to visualize this data in Nominal](https://docs.nominal.io/core/documentation/platform/workbooks/overview#creating-a-workbook).
* [Query this data with SQL](https://docs.nominal.io/developers/sql/overview) to pull it into a notebook or your own tooling.
* [Explore additional streaming patterns with the Nominal SDK](/guides/streaming/overview.md).
* [Start building with the Nominal SDK reference](/reference/toplevel.md).
* [Securely store your API key by creating a profile stored on disk](/guides/authentication.md#using-the-api-key).

:::{dropdown} New to Python? IDE recommendations

**Jupyter** is a free, beginner-friendly analysis environment ideal for notebooks, charts, and annotated workflows. Try [JupyterLab](https://jupyter.org/) if you’re new to Python.

**VSCode** is a lightweight editor suited for scripts and automation. Download it from [here](https://code.visualstudio.com/).
:::
