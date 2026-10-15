---
myst:
  html_meta:
    description: "Retrieve decimated data points in Python and display them in a time series"
---

# Retrieve decimated data points in Python

{.lead}
Retrieve decimated data points in Python and display them in a time series

When exploring large datasets, it is often useful to decimate the data so it can be faster to download and visualize.
Decimation is an optimization that reduces the number of transmitted and plotted points when visualizing high-volume data. Nominal uses decimation to maintain performance on time series and scatter plots by bucketing points when datasets exceed a large threshold. Zooming in sufficiently reveals full-resolution data within the selected range.

The Nominal platform has a fast decimation service that retains details in the signal. You can see this in
action on the Nominal platform when viewing a timeseries chart in a workbook. This chart shows a decimated signal while
retaining details, retrieving higher resolution data points as you zoom in.

This guide demonstrates how to retrieve decimated data points from the Nominal platform in a Jupyter notebook using
Python and display them in an interactive time series chart. The chart will update to show higher-resolution data as you
zoom in, similar to the behavior in the Nominal workbook.

```{video} https://res.cloudinary.com/didkpxvqu/video/upload/v1732190709/decimate_zoom_lqicxl.mov
```

## Prerequisites

Make sure you have the following Python packages installed:
 - Nominal
 - jupyterlab
 - pandas
 - plotly
 - ipywidgets

You can install them all using:
```shell
pip3 install nominal jupyterlab pandas plotly ipywidgets
```

## Generate a sample dataset

Generate a sample dataset with 720,000 rows of random data:

```python
import datetime

import numpy as np
import pandas as pd
from dateutil import parser

dataset_name = "1h_5ms"
start = parser.isoparse("2024-10-22T00:00:00Z")
end = parser.isoparse("2024-10-22T01:00:00Z")
resolution = datetime.timedelta(milliseconds=5)

dt = end - start
n = int((dt.total_seconds() * 1e6 + dt.microseconds) / resolution.microseconds)

np.random.seed(2)
cols = ["value"]

# Generate a fake signal
signal = np.random.normal(0, 0.3, size=n).cumsum() + 50

# Generate many noisy samples from the signal
noise = lambda var, bias, n: np.random.normal(bias, var, n)

data = {c: signal + noise(1, 10 * (np.random.random() - 0.5), n) for c in cols}

# Pick a few samples from the first line and really blow them out
locs = np.random.choice(n, 10)
data["value"][locs] *= 2

data["Time"] = [(start + resolution * i).isoformat() for i in range(n)]

df_gen = pd.DataFrame(data)
df_gen
```

This should display a Pandas DataFrame with a `Time` column and a `value` column with 720,000 rows.

## Upload the dataset to Nominal

Before uploading the data, ensure you're connected to Nominal.

```{include} /guides/_snippets/python/auth.md
```

Upload the dataset to Nominal using the `upload_dataframe()` method:

```python
from nominal.core import NominalClient
from nominal.thirdparty.pandas import upload_dataframe

client = NominalClient.from_profile("default")

dataset = upload_dataframe(
    client, df_gen, dataset_name, "Time", timestamp_type="iso_8601"
)
dataset
```

This should display the dataset metadata.

:::{note}

You can access this dataset later on by it's rid. You can find the rid in the output of the code above or look it
up on the Nominal platform in the "Datasources" section and enter it in the `client.get_dataset()` function
:::

## Retrieve decimated data points

With the dataset uploaded, retrieve decimated data points for a {abbr}`Channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` using `channel_to_dataframe_decimated()` from `nominal.thirdparty.pandas`.

```python
from nominal.thirdparty.pandas import channel_to_dataframe_decimated

channel = dataset.get_channel("value")
df_decimated = channel_to_dataframe_decimated(channel, start, end, buckets=2000)
df_decimated
```

This should display a Pandas DataFrame with the 2000 decimated data points.

## Display the data in a time series chart

Display the data in a time series chart using Plotly:

```python
import plotly.graph_objects as go

if "value" in df_decimated.columns:
    # range had less than 1000 points
    fig = go.Figure(
        [
            go.Scatter(
                x=df_decimated.index,
                y=df_decimated["value"],
                mode="lines",
                # line=dict(width=0),
                showlegend=False,
                # line_shape="vh"
            ),
        ]
    )
else:
    fig = go.Figure(
        [
            go.Scatter(
                x=df_decimated.index,
                y=df_decimated["max"],
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                line_shape="vh",
            ),
            go.Scatter(
                x=df_decimated.index,
                y=df_decimated["min"],
                mode="lines",
                fill="tonexty",
                fillcolor="blue",
                line=dict(width=0),
                showlegend=False,
                line_shape="vh",
            ),
        ]
    )

fig.update_layout(xaxis_title="Time", yaxis_title="Value")

fig.show()
```

This will display a time series chart with the decimated data points. Note the outliers in the signal that are preserved.

![Decimated chart](/guides/images/b4/decimated_lwyjmn.png)

## Increase resolution when zooming in

When you zoom in on the chart, the resolution of the data points will not increase.

![Decimated chart zoom low](/guides/images/b4/decimated_zoom_low_k2txen.png)

This section adds an event handler and requests higher resolution data points when zooming in.

```python
def on_range_change(layout, range_x):
    start = parser.isoparse(range_x[0] + "Z")
    end = parser.isoparse(range_x[1] + "Z")

    df = channel_to_dataframe_decimated(channel, start, end, buckets=2000)

    # Create a new figure with the new data
    fig = make_fig(df)
    fig.update_layout(xaxis={"range": range_x})

    # Update the existing figure widget with the new figure
    fig_widget.layout = fig.layout
    fig_widget.layout.on_change(on_range_change, "xaxis.range")
    fig_widget.data = []
    fig_widget.add_traces(fig.data)


def make_fig(df):
    if "value" in df.columns:
        # range had less than 1000 points
        fig = go.Figure(
            [
                go.Scatter(
                    x=df.index,
                    y=df["value"],
                    mode="lines",
                    showlegend=False,
                ),
            ]
        )
    else:
        fig = go.Figure(
            [
                go.Scatter(
                    x=df.index,
                    y=df["max"],
                    mode="lines",
                    line=dict(width=0),
                    showlegend=False,
                    line_shape="vh",
                ),
                go.Scatter(
                    x=df.index,
                    y=df["min"],
                    mode="lines",
                    fill="tonexty",
                    fillcolor="blue",
                    line=dict(width=0),
                    showlegend=False,
                    line_shape="vh",
                ),
            ]
        )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Value",
    )
    return fig


fig_range = make_fig(channel_to_dataframe_decimated(channel, start, end, buckets=2000))
fig_widget = go.FigureWidget(fig_range)

fig_widget.layout.on_change(on_range_change, "xaxis.range")

fig_widget
```

This will display a time series chart with the decimated data points. When you zoom in, the resolution of the data
increases.

![Decimated chart zoom high](/guides/images/b4/decimated_zoom_high_ieuvxm.png)
