---
myst:
  html_meta:
    description: "Load a CSV file in Python and upload it to the Nominal platform"
---

# Load a CSV file in Python

{.lead}
Load a CSV file in Python and upload it to the Nominal platform

This guide describes an automatable pipeline for uploading CSV files to Nominal. 

[CSV](https://en.wikipedia.org/wiki/Comma-separated_values) (Comma Separated Values) is a widely used format for logging flight, sensor, and machine data. 
It is human-readable, editable with spreadsheets, and compatible with practically any data analysis tool.

## Prepare your CSV file

Nominal expects CSV uploads to use standard comma-delimited formatting:

- **Delimiter:** Separate values with commas (`,`).
- **Decimal separator:** Write numeric values with a period (`.`), such as `3.14`.

Semicolon-delimited files are not supported. These exports are common in locales that use commas as decimal separators, for example `3,14`.

If your source data uses semicolons or comma decimals, convert it before upload. Prefer exporting the data again with comma delimiters and period decimal separators from Microsoft Excel, Google Sheets, or the source system. If you convert with a script, parse the original file with its existing delimiter and decimal settings, then write a new CSV with comma delimiters and period decimal separators.

After conversion, inspect a few rows before upload to confirm that numeric columns use `.` decimals and columns remain aligned.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Download sample data

For convenience and educational purposes, Nominal hosts test sample data on the [Nominal Hugging Face](https://huggingface.co/nominal-io) account.

Download 1000 rows of data generated in the [X-Plane](https://www.x-plane.com/) flight simulator.

Use the popular [Polars](https://pola.rs/) library to download and inspect the data. Polars is installed automatically as part of the `nominal` Python library.

```python

df = pl.read_csv('hf://datasets/nominal-io/frosty-flight/frosty_flight_1k_rows.csv')
df.write_csv('frosty_flight_1k_rows.csv')

df.head().select(df.columns[:6])
```

<table border="1" class="dataframe"><thead><tr><th>source_time</th><th>f_act_sec</th><th>f_sim_sec</th><th>frame_time</th><th>cpu_time</th><th>gpu_time</th></tr><tr><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;2024-06-08T05:58:42.000Z&quot;</td><td>0.27645</td><td>19.9</td><td>3.61728</td><td>0.0022</td><td>0.00078</td></tr><tr><td>&quot;2024-06-08T05:58:51.000Z&quot;</td><td>0.23154</td><td>19.9</td><td>4.31888</td><td>1.63362</td><td>0.13793</td></tr><tr><td>&quot;2024-06-08T05:58:52.000Z&quot;</td><td>0.26435</td><td>32.30079</td><td>3.78289</td><td>3.77245</td><td>0.00089</td></tr><tr><td>&quot;2024-06-08T05:58:52.000Z&quot;</td><td>0.29929</td><td>19.9</td><td>3.34123</td><td>3.77245</td><td>0.00089</td></tr><tr><td>&quot;2024-06-08T05:58:52.000Z&quot;</td><td>0.34157</td><td>30.36099</td><td>2.9277</td><td>3.77245</td><td>0.00089</td></tr></tbody></table>

## Upload your data to Nominal

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

dataset = client.create_dataset('Frosty Flight')
dataset.add_tabular_data(
    'frosty_flight_1k_rows.csv',
    timestamp_column = 'source_time',
    timestamp_type = 'iso_8601',
)

print('Uploaded dataset:', dataset.rid)
```

Equivalently, since your CSV is already loaded in the Polars dataframe `df`, you can upload it with the `upload_dataframe()` method:

```python
from nominal.thirdparty.pandas import upload_dataframe

dataset = upload_dataframe(
    client,
    df.to_pandas(),
    "name",
    timestamp_column='source_time',
    timestamp_type='iso_8601'
)

print('Uploaded dataset:', dataset.rid)
```

After upload, navigate to Nominal's [Datasets](https://app.gov.nominal.io/data-sources?sidebar=allDatasets) page (login required). You'll see your CSV at the top!

```{include} /guides/_snippets/timestamp-types.md
```
