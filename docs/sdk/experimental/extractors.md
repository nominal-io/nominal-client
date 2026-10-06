# Containerized extractors

Declare files and settings with decorators, receive them as callback arguments, and use the
context to declare outputs. The same declarations describe the image for registration.

The [package walkthrough](https://github.com/nominal-io/nominal-client/blob/main/nominal/experimental/extractor/README.md)
covers authoring, local execution, outputs, and publishing. The modules below provide the API
reference. Their public objects are also available directly from `nominal.experimental.extractor`.

## Decorators

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.experimental.extractor.decorators.single_file_extractor
   ~nominal.experimental.extractor.decorators.manifest_extractor
   ~nominal.experimental.extractor.decorators.input
   ~nominal.experimental.extractor.decorators.parameter
   ~nominal.experimental.extractor.decorators.error
```

## Output contexts

{py:meth}`ManifestExtractorContext.add_tabular <nominal.experimental.extractor.context.ManifestExtractorContext.add_tabular>`
and {py:meth}`~nominal.experimental.extractor.context.ManifestExtractorContext.add_avro_stream`
accept `units=` maps from channel names to unit symbols. Each declaration copies its map into that
output's manifest entry; units are output metadata, not image-registration settings. For direct
file uploads, use the [ingestion builder](ingest.md#channel-units-and-csv-rows).

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.experimental.extractor.context.ExtractorContext
   ~nominal.experimental.extractor.context.SingleFileExtractorContext
   ~nominal.experimental.extractor.context.ManifestExtractorContext
```

## Parameter types

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.experimental.extractor.types.Choice
   ~nominal.experimental.extractor.types.IntRange
   ~nominal.experimental.extractor.types.FloatRange
   ~nominal.experimental.extractor.types.BadParameter
```

## Execution and registration

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.experimental.extractor.runner.Extractor
```
