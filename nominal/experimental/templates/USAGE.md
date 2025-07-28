# YAML Template Configuration Usage

This document describes how to properly structure YAML template files for creating multi-panel data visualization workbooks.

## General Structure

All YAML templates must follow this top-level structure:

```yaml
version: 0                    # Required: Template version number
title: "Your Template Name"   # Required: Display name for the template  
labels: ["tag1", "tag2"]      # Required: Array of descriptive labels
tabs:                         # Required: Dictionary of tab definitions
  "Tab Name 1":
    panels: [...]             # Required: Array of panel definitions
  "Tab Name 2": 
    panels: [...]
```

### Required Fields
- **`version`**: Integer version number (currently must be `0`)
- **`title`**: String name that will be displayed for this template
- **`labels`**: Array of strings used for categorizing/searching templates
- **`tabs`**: Dictionary where keys are tab names and values contain panel definitions

### Optional Fields
- None at the top level - all fields shown above are required

## Panel Types

Each panel within a tab must specify a `type` field. The following panel types are supported:

### TIMESERIES Panels

Creates time-series line plots with optional comparison runs.

```yaml
- type: "TIMESERIES"
  rows:                           # Required: Dictionary of row definitions
    "Row Display Name":
      channel_name_1:             # Channel identifier
        - "#FF69B4"               # Required: Hex color code
        - "Axis Label"            # Required: Y-axis label text
        - 0                       # Required: Axis side (0=right, 1=left)
      channel_name_2:
        - "#32A467"
        - "Another Axis Label" 
        - 1
  comparison_runs:                # Optional: Comparison run definitions
    "Run Name":
      - "#0000FF"                 # Required: Hex color for comparison
      - "run.identifier.string"   # Required: Run identifier
```

**TIMESERIES Required Fields:**
- `type`: Must be `"TIMESERIES"`
- `rows`: Dictionary of row definitions, each containing channel specifications
- For each channel: `[color, axis_label, axis_side]`

**TIMESERIES Optional Fields:**
- `comparison_runs`: Dictionary of comparison runs (only supported for TIMESERIES panels)

### SCATTER Panels

Creates scatter plots with X and Y axis channel mappings.

```yaml
- type: "SCATTER"
  plots:                          # Required: Plot configuration
    x_axis:                       # Required: X-axis definition
      - channel_name_x            # Required: Channel for X-axis
      - "X Axis Label"            # Required: X-axis display label
    y_axis:                       # Required: Y-axis channels
      channel_name_y1:
        - "#FF69B4"               # Required: Hex color code
        - "Y Axis Label"          # Required: Y-axis label
        - 0                       # Required: Axis side (0=right, 1=left)
      channel_name_y2:
        - "#32A467"
        - "Another Y Label"
        - 1
  comparison_runs: {}             # Optional: Empty dict if no comparisons
```

**SCATTER Required Fields:**
- `type`: Must be `"SCATTER"`
- `plots`: Dictionary containing `x_axis` and `y_axis` definitions
- `x_axis`: Array of `[channel_name, axis_label]`
- `y_axis`: Dictionary of channels with `[color, axis_label, axis_side]`

**SCATTER Optional Fields:**
- `comparison_runs`: Currently not fully supported, use empty dict `{}`

### HISTOGRAM Panels

Creates histogram distributions of channel data.

```yaml
- type: "HISTOGRAM"
  channels:                       # Required: Array of channel definitions
    - [channel_name_1, "#FF69B4"] # Required: [channel, color] pairs
    - [channel_name_2, "#32A467"]
  bucket_strat:                   # Required: Bucketing strategy
    type: "COUNT"                 # Required: "COUNT" or "WIDTH"
    # For COUNT type, optionally include:
    # num_buckets: 20             # Optional for COUNT: Number of buckets (auto-chosen if omitted)
    # For WIDTH type, also include:
    # bucket_width: 0.03          # Required for WIDTH: Bucket width value
    # offset: 0                   # Optional for WIDTH: Offset value
```

**HISTOGRAM Required Fields:**
- `type`: Must be `"HISTOGRAM"`
- `channels`: Array of `[channel_name, hex_color]` pairs
- `bucket_strat`: Dictionary with bucketing configuration
  - `type`: Either `"COUNT"` or `"WIDTH"`
  - For `"COUNT"` type: `num_buckets` (integer) is optional - auto-chosen if omitted
  - For `"WIDTH"` type: `bucket_width` (number) is required
  - For `"WIDTH"` type: `offset` (number) is optional

### GEOMAP Panels

Creates geographic map visualizations with lat/long coordinate plotting.

```yaml
- type: "GEOMAP"
  plots:                          # Required: Plot definitions
    "Plot Name 1":
      - lat_channel_name          # Required: Latitude channel
      - lon_channel_name          # Required: Longitude channel  
      - "#FF69B4"                 # Required: Hex color code
    "Plot Name 2":
      - lat_channel_2
      - lon_channel_2
      - "#32A467"
  tile_type: "STREET"             # Optional: "STREET" or "SATELLITE"
  geopoints:                      # Optional: Static coordinate points
    - [37.7749, -122.4194]        # [latitude, longitude] pairs
    - [40.7128, -74.0060]
```

**GEOMAP Required Fields:**
- `type`: Must be `"GEOMAP"`
- `plots`: Dictionary of plot definitions with `[lat_channel, lon_channel, color]`

**GEOMAP Optional Fields:**
- `tile_type`: Map style, either `"STREET"` or `"SATELLITE"` (defaults to `"STREET"`)
- `geopoints`: Array of `[latitude, longitude]` coordinate pairs for static markers

## Data Types and Formatting

### Colors
All colors must be specified as hex color codes starting with `#`:
- Valid: `"#FF69B4"`, `"#32A467"`, `"#0000FF"`
- Invalid: `"red"`, `"rgb(255,0,0)"`, `"FF69B4"`

### Axis Sides
Axis side values must be integers:
- `0`: Right side axis
- `1`: Left side axis

### Channel Names
Channel names must be strings that **exactly match** the channel names as they exist in your Nominal data source. Channel name matching is case-sensitive and must be precise - any mismatch will result in the channel not being found.

## Example Complete Template

```yaml
version: 0
title: "Multi-Panel Analysis Dashboard"
labels: ["analysis", "comparison", "geographic"]
tabs:
  "Performance":
    panels:
      - type: "TIMESERIES"
        rows:
          "Speed Tracking":
            vehicle_speed:
              - "#3633ff"
              - "Speed (mph)"
              - 0
        comparison_runs:
          Baseline:
            - "#ff3333"
            - "run.baseline.123"
      - type: "HISTOGRAM"
        channels:
          - [vehicle_speed, "#FFC0CB"]
        bucket_strat:
          type: "COUNT"
  "Location":
    panels:
      - type: "SCATTER"
        plots:
          x_axis:
            - gps_lat
            - "Latitude"
          y_axis:
            gps_lon:
              - "#FF69B4"
              - "Longitude"
              - 0
      - type: "GEOMAP"
        plots:
          Track:
            - gps_lat
            - gps_lon
            - "#32A467"
        tile_type: "SATELLITE"
```

## Notes and Limitations

- **Comparison Runs**: Currently only supported for TIMESERIES panels
- **Tab Names**: Must be unique within a template
- **Panel Ordering**: Panels are displayed in the order they appear in the YAML
- **Channel Validation**: Channel names must exactly match those in your Nominal data source (case-sensitive)