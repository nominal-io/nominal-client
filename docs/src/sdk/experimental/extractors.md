# Extractor authoring

```{include} ../../../../nominal/experimental/extractor/README.md
:start-line: 1
```

## Decorators

```{eval-rst}
.. automodule:: nominal.experimental.extractor.decorators
```

## Output contexts

`ManifestExtractorContext.add_tabular` and `add_avro_stream` accept `units=` maps from channel
names to unit symbols. Each declaration copies its map into that output's manifest entry;
units are output metadata, not image-registration settings. For direct file uploads, use
the [ingestion builder](ingest.md#channel-units-and-csv-rows).

```{eval-rst}
.. automodule:: nominal.experimental.extractor.context
```

## Parameter types

```{eval-rst}
.. automodule:: nominal.experimental.extractor.types
```

## Execution and registration

```{eval-rst}
.. automodule:: nominal.experimental.extractor.runner
```
