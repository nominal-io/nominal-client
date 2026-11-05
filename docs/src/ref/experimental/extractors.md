# Extractor authoring

```{include} ../../../../nominal/experimental/extractor/README.md
:start-line: 1
```

## Per-output units

`ManifestExtractorContext.add_tabular` and `add_avro_stream` accept `units=` maps from channel
names to unit symbols. Each declaration copies its map into that output's manifest entry;
units are output metadata, not image-registration settings. For direct file uploads, use
the [ingestion builder](ingest.md#channel-units-and-csv-rows).

## Typed arguments

{py:meth}`nominal.core.Dataset.add_containerized` and the ingestion builder accept string,
boolean, integer, and finite float arguments. Booleans become `"true"`/`"false"`; numbers
use `str`/`repr`, and strings are unchanged. Non-finite floats raise `ValueError` before upload.

## API Reference

### Decorators

```{eval-rst}
.. include:: ../_summaries/nominal.experimental.extractor.decorators.rst
```

### Output contexts

```{eval-rst}
.. include:: ../_summaries/nominal.experimental.extractor.context.rst
```

### Parameter types

```{eval-rst}
.. include:: ../_summaries/nominal.experimental.extractor.types.rst
```

### Execution and registration

```{eval-rst}
.. include:: ../_summaries/nominal.experimental.extractor.runner.rst
```
