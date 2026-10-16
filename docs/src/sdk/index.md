# Nominal Python SDK

{.lead}
The API reference for the `nominal` package. For installation, quickstarts, and how-to guides,
see the [guides](/index.md).

The main components of the SDK are:

- **[`nominal.core`](core.md)**: the object-oriented client for the Nominal core platform.
  Start from {py:class}`~nominal.core.NominalClient`.
- **[`nominal.ts`](ts.md)**: timestamp types and formats used across the Nominal ecosystem.
  Start with the {ref}`timestamp format examples <examples>`.
- **[`nominal.exceptions`](exceptions.md)**: an inventory of errors raised by the client.
- **[`nominal.thirdparty`](thirdparty/index.md)**: integrations for working with
  [pandas](thirdparty/pandas.md), [Polars](thirdparty/polars.md), and [MATLAB](thirdparty/matlab.md).
  pandas and Polars are installed by default. For [TDMS](thirdparty/tdms.md) files,
  install `nominal[tdms]`, which provides `nominal.tdms`.
- **[`nominal.ibis`](ibis.md)**: an Ibis backend for querying the Nominal SQL API.
  Install with `nominal[ibis]`.
- **[`nominal.experimental`](experimental/index.md)**: cutting-edge experiments for working with the core platform,
  including [compute expressions and results](experimental/compute.md),
  [derived datasets](experimental/derived_datasets.md), [extractor authoring](experimental/extractors.md),
  [the ingestion builder](experimental/ingest.md), [cross-tenant migration](experimental/migration.md),
  [spatial data](experimental/spatial.md), [live video streaming](experimental/video.md),
  and [video processing](experimental/video_processing.md). These APIs may change between releases.

For command-line operations, see the [`nom` CLI reference](cli.md).
