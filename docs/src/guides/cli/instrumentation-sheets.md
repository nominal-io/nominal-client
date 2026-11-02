---
myst:
  html_meta:
    description: "Set channel descriptions and units on a dataset from an instrumentation sheet with nom mis."
---

# Instrumentation sheets

{.lead}
`nom mis` reads an instrumentation sheet, also called a signal or {term}`channel` list, and sets the description and {term}`unit` of each matching channel in a {term}`dataset`.

You need a {term}`profile` from [API keys setup](/guides/api-keys-setup.md).
Excel files need the [`mis` extra](/guides/install.md#extras), and `.xls` files also need `pip install xlrd`.
To set units from Python instead, see [Channels and data types](/guides/datasets/channels.md#set-units).

## Prepare the sheet

The sheet is a CSV or Excel file with a `Channel` column, and optional `Description` and `UCUM Unit` columns.
Column names are case-insensitive, and other columns are ignored.

```text
Channel,Description,UCUM Unit
RPM_ENGINE_1,Engine 1 rotational speed,/min
TEMP_EGT_1,Engine 1 exhaust gas temperature,Cel
PRESS_FUEL_1,Engine 1 fuel pressure,bar
SPEED_IAS,Indicated airspeed,m/s
```

Units are [UCUM](https://ucum.org/) symbols, such as `Cel` for degrees Celsius.
To convert your own unit names, list the units Nominal knows and map yours to them, for example with `XLOOKUP` in Excel:

```bash
nom mis list-units --profile default --output units.csv --format csv
```

The file has a `UCUM` column with the symbol and a `Unit Name` column.

## Check the units

```bash
nom mis validate --profile default mis.csv
```

It lists units that Nominal doesn't recognize.
Those still display in Nominal, but {term}`workbooks <workbook>` can't convert them.

## Apply the sheet

```bash
nom mis process --profile default mis.csv --dataset-rid <DATASET_RID>
```

Channels match by exact, case-sensitive name. The command warns about sheet rows with no matching channel.
By default, it skips a channel whose existing description or unit differs from the sheet, with a warning. Pass `--override-channel-info` to replace them.

For an Excel file with several sheets, pick one with `--sheet "Instrumentation"`. `validate` takes the same option.
