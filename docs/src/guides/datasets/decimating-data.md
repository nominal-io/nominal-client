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

```{literalinclude} /guides/_snippets/code/data_retrieval/retrieve_decimated_1.py
:language: python
```

This should display a Pandas DataFrame with a `Time` column and a `value` column with 720,000 rows.

## Upload the dataset to Nominal

Before uploading the data, ensure you're connected to Nominal.

```{include} /guides/_snippets/python/auth.md
```

Upload the dataset to Nominal using the `upload_dataframe()` method:

```{literalinclude} /guides/_snippets/code/data_retrieval/retrieve_decimated_2.py
:language: python
```

This should display the dataset metadata.

:::{note}

You can access this dataset later on by it's rid. You can find the rid in the output of the code above or look it
up on the Nominal platform in the "Datasources" section and enter it in the `client.get_dataset()` function
:::

## Retrieve decimated data points

With the dataset uploaded, retrieve decimated data points for a {abbr}`Channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` using `channel_to_dataframe_decimated()` from `nominal.thirdparty.pandas`.

```{literalinclude} /guides/_snippets/code/data_retrieval/retrieve_decimated_3.py
:language: python
```

This should display a Pandas DataFrame with the 2000 decimated data points.

## Display the data in a time series chart

Display the data in a time series chart using Plotly:

```{literalinclude} /guides/_snippets/code/data_retrieval/retrieve_decimated_4.py
:language: python
```

This will display a time series chart with the decimated data points. Note the outliers in the signal that are preserved.

![Decimated chart](/guides/images/b4/decimated_lwyjmn.png)

## Increase resolution when zooming in

When you zoom in on the chart, the resolution of the data points will not increase.

![Decimated chart zoom low](/guides/images/b4/decimated_zoom_low_k2txen.png)

This section adds an event handler and requests higher resolution data points when zooming in.

```{literalinclude} /guides/_snippets/code/data_retrieval/retrieve_decimated_5.py
:language: python
```

This will display a time series chart with the decimated data points. When you zoom in, the resolution of the data
increases.

![Decimated chart zoom high](/guides/images/b4/decimated_zoom_high_ieuvxm.png)
