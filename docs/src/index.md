# Nominal Python SDK: Function Reference

The main components of the SDK are:

- An object-oriented client interface (`nominal.core.NominalClient`)
  for interacting with the Nominal platform
- The `nom` CLI for performing common operations from the terminal
- Extensions for working with thirdparty components (`nominal.thirdparty.pandas`, etc.). Pandas and polars are included by default; TDMS is available with the `tdms` extra.

Use the navigation bar above to see examples and reference documentation.

If you are new to Nominal and the Python client, we recommend that you read our [Quickstart guide](https://docs.nominal.io/core/sdk/python-client/quickstart).

Thereafter, navigate to the [API reference manual](./reference/toplevel.md).

```{toctree}
:hidden:
:caption: Home
:maxdepth: 1

Overview <self>
Changelog <changelog>
License <license>
```

```{toctree}
:hidden:
:caption: Reference
:maxdepth: 1

High-level SDK <reference/toplevel>
Timestamps <reference/ts>
Core SDK <reference/core>
Exceptions <reference/exceptions>
nom cli <reference/nom-cli>
matlab <reference/thirdparty/matlab>
pandas <reference/thirdparty/pandas>
tdms <reference/thirdparty/tdms>
Compute <reference/experimental/compute>
Extractors <reference/experimental/extractors>
Ingestion <reference/experimental/ingest>
Logging <reference/experimental/logging>
Video Processing <reference/experimental/video_processing>
```

```{toctree}
:hidden:
:caption: Guides
:maxdepth: 1

Networking & TLS <networking-tls>
```

```{toctree}
:hidden:
:caption: Development
:maxdepth: 1

Contributing <contributing>
```
