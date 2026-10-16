---
myst:
  html_meta:
    description: "Overview of the Nominal CLI"
---

# Command line interface

{.lead}
Overview of the Nominal CLI

A subset of the functionality of the Python API is made available via a command-line interface (CLI).

The CLI executable is called `nom`, and installed when you do `pip install nominal`.

```sh
nom --version
```

Before using the CLI, save a connection profile following the [Authentication guide](/guides/authentication.md). Commands that access Nominal require `--profile default` (replace `default` with your profile name).

The CLI provides commands for managing connection profiles and containerized extractors, downloading data, processing instrumentation sheets, and migrating resources between tenants:

```sh
nom --help
```

See the [generated CLI reference](/sdk/cli.md) for current command options and defaults.

## Available Commands

### `config`
Manage authentication profiles for connecting to Nominal. Add, remove, list, and migrate profiles that store your API tokens and workspace settings.

### `container`
Work with containerized extractors and the container images they run. An extractor carries identity; its execution contract (inputs, parameters, output format, timestamp defaults) lives on the container images registered against it, exactly one of which is active.

### `download`
Interactive data browsing and download wizard. Select an asset and dataset through an interactive terminal UI, choose channels and time bounds, and export data to CSV or Parquet. See the [Downloading Data](/guides/cli/downloading-data.md) guide for a detailed walkthrough.

### `migrate`
Copy resources between Nominal tenants. Use `prep` to count in-scope resources and generate a migration plan, `copy` to perform the copy, and `summary` to summarize a migration as a markdown table.

### `mis`
Process Master Instrumentation Sheets (MIS) to bulk-update channel descriptions and units. Validate units against Nominal's UCUM catalog and list available units. See the [Instrumentation Sheet Handling](/guides/cli/instrumentation-sheets.md) guide for detailed documentation.

## Using Commands

To find out more about each command, run it with `--help`:

```sh
nom config --help
```

```sh
nom config profile add --help
```

## Next Steps

- **Authentication**: Set up your API tokens with `nom config profile add` - see the [Authentication guide](/guides/authentication.md)
- **Download Data**: Learn how to use the interactive download wizard - see the [Downloading Data](/guides/cli/downloading-data.md) guide
- **Instrumentation Sheets**: Learn how to bulk-update channel metadata - see the [Instrumentation Sheet Handling](/guides/cli/instrumentation-sheets.md) guide
- **API Reference**: For programmatic access, check out the [Python SDK documentation](/sdk/index.md)
