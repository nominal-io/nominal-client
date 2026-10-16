# Ingestion builder

```{include} ../../../../nominal/experimental/ingest/README.md
:start-line: 1
```

When a container produces these files, declare units through the
[extractor output context](extractors.md#output-contexts) instead. Those units travel
in the output manifest, rather than the image's registration contract.

## Reference

```{eval-rst}
.. include:: ../_summaries/nominal.experimental.ingest.rst

.. autodata:: nominal.experimental.ingest.DEFAULT_FILE_RETRY_TIMEOUT_S

.. autodata:: nominal.experimental.ingest.DEFAULT_MAX_STORAGE_WORKERS

.. autodata:: nominal.experimental.ingest.DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES

.. autodata:: nominal.experimental.ingest.MAX_SMALL_FILE_ROUTE_BYTES

.. autodata:: nominal.experimental.ingest.NOMINAL_MAX_CONCURRENCY
```
