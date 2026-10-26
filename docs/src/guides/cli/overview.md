---
myst:
  html_meta:
    description: "What the nom command line interface does, and where each command is documented."
---

# Command line interface

{.lead}
`nom` is the client's command line interface. It installs with `nominal` and covers profiles, extractors, downloads, instrumentation sheets and migrations.

Commands that call Nominal take `--profile <name>`, using a profile from [API keys setup](/guides/api-keys-setup.md).
Add `--help` to any command for its options, or see the [CLI reference](/sdk/cli.md).

| Command | Use it to | Guide |
| --- | --- | --- |
| `nom config` | Add, list, remove and migrate profiles | [API keys setup](/guides/api-keys-setup.md) |
| `nom container` | Register and manage file extractors and their images | [File extractors](/guides/extractors/overview.md#from-the-cli) |
| `nom download` | Pick channels and a time range, and save them as CSV or Parquet | [Download data](/guides/cli/downloading-data.md) |
| `nom mis` | Set channel units and descriptions from an instrumentation sheet | [Instrumentation sheets](/guides/cli/instrumentation-sheets.md) |
| `nom migrate` | Copy resources between Nominal tenants | [CLI reference](/sdk/cli.md) |
