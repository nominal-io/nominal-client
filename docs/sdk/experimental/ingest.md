# Multi-file ingestion

Use {py:class}`~nominal.experimental.ingest.IngestBuilder` to upload files and submit them as one
ingestion job. Choose `add_csv` for CSV row settings, `add_parquet` for Parquet files or archives,
or `add_tabular_data` to infer either format while using their shared options.

The [package walkthrough](https://github.com/nominal-io/nominal-client/blob/main/nominal/experimental/ingest/README.md)
covers building a batch, tracking ingestion, and declaring channel units.

## Channel units and CSV rows

Pass `units={"pressure": "Pa"}` to declare channel units for tabular or Avro data.
For CSV files with a units record, use `add_csv(..., units_row=2, data_row=3)`;
explicit unit mappings override the record per channel. Row numbers are one-based,
and the backend owns their defaults and validation. The client does not convert values.

When a container produces these files, declare units through the
[extractor output context](extractors.md#output-contexts) instead. Those units travel
in the output manifest, rather than the image's registration contract.

## Ingestion builder

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.experimental.ingest.IngestBuilder
   ~nominal.experimental.ingest.MultipartUploader
```

## Defaults

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.experimental.ingest.DEFAULT_FILE_RETRY_TIMEOUT_S
   ~nominal.experimental.ingest.DEFAULT_MAX_STORAGE_WORKERS
   ~nominal.experimental.ingest.DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES
   ~nominal.experimental.ingest.MAX_SMALL_FILE_ROUTE_BYTES
   ~nominal.experimental.ingest.NOMINAL_MAX_CONCURRENCY
```
