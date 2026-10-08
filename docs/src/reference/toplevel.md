# Nominal Python SDK

{.lead}
The API reference for the `nominal` package. For installation, quickstarts and how-to guides, see the [guides](/index.md).

The main components of the SDK are:

- **[Core](core.md)**: the object-oriented client for the Nominal platform. Start from
  {py:class}`~nominal.core.NominalClient`.
- **[Timestamps](ts.md)**: how timestamps in your data are encoded, for uploads and streams.
- **[Exceptions](exceptions.md)**: errors raised by the client.
- **[`nom` CLI](nom-cli.md)**: common operations from the terminal.
- **Third-party integrations**: [pandas](thirdparty/pandas.md) and [polars](thirdparty/polars.md)
  (installed by default), [MATLAB](thirdparty/matlab.md), and [TDMS](thirdparty/tdms.md)
  (installed with `nominal-tdms`).
- **Experimental**: [compute](experimental/compute.md), [containerized extractors](experimental/extractors.md),
  [multi-file ingestion](experimental/ingest.md), [logging](experimental/logging.md), and
  [video processing](experimental/video_processing.md). These APIs may change without notice.
