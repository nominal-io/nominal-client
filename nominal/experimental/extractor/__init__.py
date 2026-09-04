"""Runtime helpers for authoring Nominal Hosted containerized extractors.

A containerized extractor is a Docker image Nominal runs during ingest: it mounts the
uploaded input file(s), runs your code, and ingests whatever your code writes to the
output directory. The contract is environment-driven:

- each input file is placed in the input mount (``/input``), and its path is also exposed
  in the environment variable declared for that input at registration time;
- output goes to the directory named by ``$OUTPUT_DIR``;
- declared parameters arrive as environment variables (values are always strings).

The image's registered output format fixes which of two output contracts the ingest
pipeline applies, and this module provides one decorator per contract:

- :func:`single_file_extractor` -- the pipeline ingests exactly one output file, parsed
  according to the registered format (``PARQUET``, ``CSV``, ...). Your function declares
  that file with :meth:`SingleFileExtractorContext.set_output`.
- :func:`manifest_extractor` -- for images registered with the ``MANIFEST`` output format.
  The pipeline reads a ``manifest.json`` describing every output file; your function
  declares each file it wrote with the method for that file's format --
  :meth:`ManifestExtractorContext.add_tabular`, :meth:`~ManifestExtractorContext.add_avro_stream`,
  :meth:`~ManifestExtractorContext.add_journal_json`, :meth:`~ManifestExtractorContext.add_video`
  -- and :meth:`Extractor.run` writes the manifest automatically. A manifest extractor may emit
  telemetry, videos, or both.

  Video outputs require a recent version of the Nominal platform: an older ingest pipeline
  ignores them, and rejects a manifest whose only outputs are videos.

Both decorators turn ``def fn(ctx) -> None`` into a container entrypoint: ``ctx`` resolves
inputs and parameters from the environment, and :meth:`Extractor.run` finalizes the outputs
and turns any failure into a non-zero exit so the ingest job fails cleanly. Nominal describes
the extractor's registered contract to the container through ``_NOMINAL_*`` environment
variables -- the registered output format (``_NOMINAL_OUTPUT_FORMAT``), the mounted inputs
(``_NOMINAL_INPUTS``), and the declared parameters (``_NOMINAL_PARAMETERS``). When the
registered output format is injected and disagrees with the decorator you used, the run fails
at startup with a clear error rather than emitting output the pipeline will reject; when it
is absent (a local run) the decorator's word is law.

Newer ingest pipelines additionally inject system metadata -- the ingest job and dataset RIDs,
the resolved job-level timestamp metadata, and the ingest request's tags -- exposed through
:attr:`ExtractorContext.ingest_job_rid`, :attr:`ExtractorContext.dataset_rid`,
:attr:`ExtractorContext.job_timestamp_metadata`, and :attr:`ExtractorContext.additional_tags`.
All are optional: None/empty when not injected (e.g. local runs).

The manifest document is emitted through the generated ``nominal_api.ingest_manifest`` types, so
its schema tracks the platform contract instead of being hand-mirrored here. Format I/O (pyarrow,
etc.) remains the author's own dependency. Registering the built image with Nominal is a separate
step (see the Nominal Hosted extractor APIs); this module is only the in-container runtime.
"""

from nominal.core.container_image import TimestampMetadata
from nominal.core.exceptions import ExtractorError
from nominal.experimental.extractor._context import (
    ExtractorContext,
    ManifestExtractorContext,
    SingleFileExtractorContext,
)
from nominal.experimental.extractor._runner import Extractor, manifest_extractor, single_file_extractor

__all__ = [
    "Extractor",
    "ExtractorContext",
    "ExtractorError",
    "ManifestExtractorContext",
    "SingleFileExtractorContext",
    "TimestampMetadata",
    "manifest_extractor",
    "single_file_extractor",
]
