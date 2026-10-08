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
$ nom --version
nom, version 1.167.0
```

Before using the CLI, ensure that it is authenticated following the instructions in the [Authentication section](/guides/authentication.md).

Currently, the CLI has commands for manipulating attachments, datasets, runs, containerized extractors, instrumentation sheets, and cross-tenant migrations:

```sh
$ nom --help
Usage: nom [OPTIONS] COMMAND [ARGS]...

Options:
  --version   Show the version and exit.
  -h, --help  Show this message and exit.

Commands:
  attachment
  config
  container   Work with containerized extractors and the container images...
  dataset
  download    Browse assets, pick a dataset, filter channels by exact...
  migrate     Commands to migrate resources between Nominal tenants.
  mis         This CLI processes an MIS and turns it into unit...
  run
```

## Available Commands

### `attachment`
Upload, download, and retrieve attachments by RID. Useful for managing documentation, images, and other files associated with your test data.

### `config`
Manage authentication profiles for connecting to Nominal. Add, remove, list, and migrate profiles that store your API tokens and workspace settings.

### `container`
Work with containerized extractors and the container images they run. An extractor carries identity; its execution contract (inputs, parameters, output format, timestamp defaults) lives on the container images registered against it, exactly one of which is active.

### `dataset`
Upload CSV files to create new datasets, retrieve dataset information by RID, and summarize dataset schemas including {abbr}`channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` names, units, and metadata.

### `download`
Interactive data browsing and download wizard. Browse assets, runs, and datasets through an interactive terminal UI, select channels, and export data to disk in various formats. See the [Downloading Data](/guides/cli/downloading-data.md) guide for a detailed walkthrough.

### `migrate`
Copy resources between Nominal tenants. Use `prep` to count in-scope resources and generate a migration plan, `copy` to perform the copy, and `summary` to summarize a migration as a markdown table.

### `mis`
Process Master Instrumentation Sheets (MIS) to bulk-update channel descriptions and units. Validate units against Nominal's UCUM catalog and list available units. See the [Instrumentation Sheet Handling](/guides/cli/instrumentation-sheets.md) guide for detailed documentation.

### `run`
Create and retrieve runs. Define time-bounded segments of test data with properties and labels for organizing your test campaigns.

## Using Commands

To find out more about each command, run it with `--help`:

```sh
$ nom attachment --help
Usage: nom attachment [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --help  Show this message and exit.

Commands:
  download  Download an attachment with the given RID to the specified...
  get       Get an attachment by its RID
  upload    Upload attachment from a local file with a given name and...
```

```sh
$ nom attachment download --help
Usage: nom attachment download [OPTIONS]

  Download an attachment with the given RID to the specified location on disk.

Options:
  -r, --rid TEXT           [required]
  -o, --output TEXT        full path to write the attachment to (not just the
                           directory)  [required]
  --profile TEXT           If provided, use the given named config profile for
                           instantiating a Nominal Client. This is the
                           preferred mechanism for instantiating a client
                           today-- see `nom config profile add` to create a
                           configuration profile. If provided, takes
                           precedence over --token, --token-path, and --base-
                           url.  [required]
  --trust-store-path FILE  Path to a trust store CA root file to initiate SSL
                           connections. If not provided, defaults to certifi's
                           trust store.
  --no-color               If provided, don't color terminal log output
  -v, --verbose            Verbosity to use within the CLI. Pass -v to allow
                           info-level logs, or -vv for debug-level.  [default:
                           0]
  -h, --help               Show this message and exit.
```

## Next Steps

- **Authentication**: Set up your API tokens with `nom config profile add` - see the [Authentication guide](/guides/authentication.md)
- **Download Data**: Learn how to use the interactive download wizard - see the [Downloading Data](/guides/cli/downloading-data.md) guide
- **Instrumentation Sheets**: Learn how to bulk-update channel metadata - see the [Instrumentation Sheet Handling](/guides/cli/instrumentation-sheets.md) guide
- **API Reference**: For programmatic access, check out the [Python SDK documentation](/reference/toplevel.md)
