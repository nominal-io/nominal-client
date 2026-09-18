# Containerized extractors

Declare files and settings with decorators, receive them as callback arguments, and use the
context to declare outputs. The same declarations describe the image for registration.

The [package walkthrough](https://github.com/nominal-io/nominal-client/blob/main/nominal/experimental/extractor/README.md)
covers authoring, local execution, outputs, and publishing. The modules below provide the API
reference. Their public objects are also available directly from `nominal.experimental.extractor`.

## Decorators

::: nominal.experimental.extractor.decorators

## Output contexts

`ManifestExtractorContext.add_tabular` and `add_avro_stream` accept `units=` maps from channel
names to unit symbols. Each declaration copies its map into that output's manifest entry.

::: nominal.experimental.extractor.context

## Parameter types

::: nominal.experimental.extractor.types

## Execution and registration

::: nominal.experimental.extractor.runner
