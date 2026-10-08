---
myst:
  html_meta:
    description: "Bulk process channel descriptions and units from an instrumentation sheet"
---

# Instrumentation Sheet Handling

{.lead}
Bulk process channel descriptions and units from an instrumentation sheet

The Nominal CLI provides tools to process Master Instrumentation Sheets (MIS) and apply {abbr}`channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` descriptions and units to your datasets in bulk. This is particularly useful for test programs where you may have hundreds or thousands of channels to document and this information already exists in an external document.

## What is a Master Instrumentation Sheet?

A Master Instrumentation Sheet (MIS) is a comprehensive reference document that catalogs all measurement channels in a test program. It typically includes:

- **Channel names** - The identifier for each measurement point
- **Descriptions** - Human-readable explanations of what each channel measures
- **Units** - The engineering units for each measurement (e.g., `psia`, `Cel`, `m/s`)
- **Sample frequency** - The sample frequency for each measurement (e.g., `100 Hz`, `1000 Hz`)

And many others. In the case of Nominal the first 3 are most germane.

### Other Common Names

Depending on your organization, this document may also be called:
- **Signal List**
- **Channel List**
- **Instrumentation List**
- **Parameter List**

## File Format Requirements

Your instrumentation sheet must be in CSV or Excel format (`.csv`, `.xlsx`, `.xls`) with exactly three columns:

| Column Name | Description | Example |
|------------|-------------|---------|
| `Channel` | Channel identifier | `RPM_ENGINE_1` |
| `Description` | Human-readable description | `Engine 1 Rotational Speed` |
| `UCUM Unit` | Unit in UCUM format | `rpm` |

:::{note}

Column names are case-insensitive - `channel`, `Channel`, and `CHANNEL` are all acceptable.
:::

### Example Sheet

```text
Channel,Description,UCUM Unit
RPM_ENGINE_1,Engine 1 Rotational Speed,rpm
TEMP_EGT_1,Engine 1 Exhaust Gas Temperature,Cel
PRESS_FUEL_1,Engine 1 Fuel Pressure,psia
ALT_BARO,Barometric Altitude,[ft_i]
SPEED_IAS,Indicated Airspeed,[kn_i]
```

## Converting Your Existing Sheet

Many organizations already have instrumentation sheets, but they may not be in the required format. Here's how to convert your existing sheet:

### Step 1: Get Available Units

First, export the list of units that Nominal supports:

```bash
nom mis list-units --output nominal_units.csv --format csv
```

This creates a CSV file with two columns:
- `UCUM` - The UCUM symbol to use (e.g., `Cel`, `psia`, `rpm`)
- `Unit Name` - The full name of the unit (e.g., `degree Celsius`, `pound per square inch absolute`)

Add this new sheet to your existing instrumentation sheet.

### Step 2: Create a unit mapping

In the same sheet as above add a new column for your companies units. Map each of your company's units to the Nominal unit. Here is a short example:
```text
Unit,UCUM Unit
psia,psia
c,Cel
DegF,[degF]
rpm,rpm
```

### Step 3: Apply the Mapping

Once you have your unit mapping table from Step 2, create a new sheet or tab with the three required columns and combine all the information:

1. **Copy your channel names** into the `Channel` column
2. **Copy your descriptions** into the `Description` column
3. **Use XLOOKUP to convert your units to UCUM format**:

```text
=XLOOKUP(D2, unit_mapping[Unit], unit_mapping[UCUM Unit], "NOT_FOUND", 0)
```

Where:
- `D2` contains your company's original unit (e.g., "c", "DegF", "psia") on a different sheet (likely one that is unmodified)
- `unit_mapping` is a table reference to the mapping you created in Step 2
- The formula searches for your company's unit and returns the corresponding UCUM symbol
- If no match is found, it returns "NOT_FOUND" so you can add missing units to your mapping

:::{tip}

If you don't have XLOOKUP (Excel 2019 or older), use VLOOKUP instead:
```text
=IFERROR(VLOOKUP(D2, unit_mapping!A:B, 2, FALSE), "NOT_FOUND")
```
Note: The `2` in VLOOKUP returns the second column (UCUM Unit).
:::

### Step 4: Validate Display-Only Units

Some units may not have UCUM equivalents. Nominal will still display these units, but they won't work with the Unit Conversion transform. To identify these units before uploading:

```bash
nom mis validate path/to/your/mis.csv
```

This command will report any units that are not recognized:

```
WARNING: Found 2 display only units in the MIS file:
  - deg_F
  - meters_per_second
WARNING: The listed units will still show in Nominal but will not work with 
the 'Unit Conversion' transform. You can use the 'list-units' command to see 
all available units.
```

You can then decide whether to correct these units or accept them as display-only.

## CLI Commands

### Process MIS File

Apply channel descriptions and units from your instrumentation sheet to a dataset:

```bash
nom mis process path/to/your/mis.csv --dataset-rid ri.nominal.dataset.abc123
```

**For Excel files with a single sheet:**
```bash
nom mis process path/to/your/mis.xlsx --dataset-rid ri.nominal.dataset.abc123
```

The CLI will automatically use the only sheet in the file.

**For Excel files with multiple sheets:**
```bash
nom mis process path/to/your/mis.xlsx --dataset-rid ri.nominal.dataset.abc123 --sheet "Instrumentation"
```

You must specify which sheet contains your MIS data using the `--sheet` option.

### Validate Units

Check if all units in your MIS file are supported by Nominal before processing:

```bash
nom mis validate path/to/your/mis.csv
```

For Excel files:
```bash
nom mis validate path/to/your/mis.xlsx --sheet "Instrumentation"
```

This will report:
- ✅ All units are valid, or
- ⚠️ Which units are display-only (won't work with Unit Conversion)

### List Available Units

Export all units supported by Nominal:

**As a table (default):**
```bash
nom mis list-units
```

**As a CSV file:**
```bash
nom mis list-units --output units.csv --format csv
```

**As a table file:**
```bash
nom mis list-units --output units.txt --format table
```

## Complete Workflow Example

Here's a complete workflow for applying instrumentation data to a dataset:

### 1. Export Nominal's unit list
```bash
nom mis list-units --output nominal_units.csv --format csv
```

### 2. Prepare your MIS file
- Open your existing instrumentation sheet in Excel
- Create a new sheet with columns: `Channel`, `Description`, `UCUM Unit`
- Use XLOOKUP to map your units to Nominal's UCUM format
- Save as CSV or keep as Excel

### 3. Validate your units
```bash
nom mis validate ~/instrumentation/flight_test_mis.csv
```

Review any warnings about display-only units and correct if needed.

### 4. Process the MIS file
```bash
nom mis process ~/instrumentation/flight_test_mis.csv --dataset-rid ri.nominal.dataset.abc123
```

The CLI will:
- Read your MIS file
- Match channels by name to your dataset
- Update the description and unit for each matched channel
- Warn you about any channels in the MIS that don't exist in the dataset

### 5. Verify in Nominal
Open your dataset in Nominal and confirm that channel descriptions and units have been updated.

## Troubleshooting

### "Channel X not found in dataset"
This warning means a channel in your MIS file doesn't exist in your dataset. Common causes:
- Typo in channel name
- Channel name has different casing (Nominal is case-sensitive)
- MIS file includes channels from a different test or configuration

### "Excel file has multiple sheets"
If you see this error, you need to specify which sheet contains your MIS data:
```bash
nom mis process file.xlsx --dataset-rid ri.nominal.dataset.abc123 --sheet "Sheet1"
```

### "MIS file must have columns: channel, description, ucum unit"
Your file is missing one or more required columns. Ensure your file has exactly these three columns (case-insensitive).

### "Error parsing MIS file"
The file format is not recognized. Ensure your file is:
- A valid CSV file, or
- A valid Excel file with `.xlsx` or `.xls` extension

## Common UCUM Unit mappings

Here are some commonly used UCUM units in aerospace testing:

| Measurement Type | UCUM Symbol | Full Name |
|-----------------|-------------|-----------|
| Temperature | `Cel` | degree Celsius |
| Temperature | `[degF]` | degree Fahrenheit |
| Pressure | `bar` | bar |
| Pressure | `psia` | pound per square inch absolute |
| Pressure | `psig` | pound per square inch gauge |
| Speed | `m/s` | meter per second |
| Speed | `[kn_i]` | knot (international) |
| Speed | `mph` | mile per hour |
| Angular Speed | `rpm` | revolutions per minute |
| Angular Speed | `deg/s` | degree per second |
| Altitude | `m` | meter |
| Altitude | `[ft_i]` | foot (international) |
| Acceleration | `m/s^2` | meter per second squared |
| Acceleration | `[g]` | standard acceleration of gravity |
| Force | `N` | newton |
| Force | `[lbf_av]` | pound-force (avoirdupois) |
| Voltage | `V` | volt |
| Current | `A` | ampere |
| Frequency | `Hz` | hertz |
| Angle | `deg` | degree |
| Angle | `rad` | radian |
| Mass Flow | `kg/s` | kilogram per second |

:::{tip}

Use `nom mis list-units` to see the complete list of available units with their exact UCUM symbols.
:::

## Best Practices

1. **Validate before processing** - Always run `validate` before `process` to catch unit issues early
2. **Keep your MIS current** - Update your instrumentation sheet as your test program evolves
3. **Use version control** - Store your MIS files in version control (Git) alongside your test configurations
4. **Standardize early** - Establish UCUM unit conventions at the start of your program
5. **Document custom units** - If you must use display-only units, document why and what they represent
6. **Test with a subset** - For large datasets, test your MIS file on a small test dataset first

## Related Commands

- [Dataset CLI](/guides/datasets/overview.md) - Other dataset management commands
- [Authentication](/guides/authentication.md) - Setting up API access
