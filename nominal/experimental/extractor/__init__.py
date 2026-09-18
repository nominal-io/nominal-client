"""Declare, run, and register containerized extractors.

Public decorators, contexts, converters, and the runner are re-exported here.
See the package README for the authoring walkthrough and registration examples.
"""

from nominal.core.container_image import TimestampMetadata
from nominal.core.exceptions import ExtractorError
from nominal.experimental.extractor.context import (
    ExtractorContext,
    ManifestExtractorContext,
    SingleFileExtractorContext,
)
from nominal.experimental.extractor.decorators import error, input, manifest_extractor, parameter, single_file_extractor
from nominal.experimental.extractor.runner import Extractor
from nominal.experimental.extractor.types import BadParameter, Choice, FloatRange, IntRange

__all__ = [
    "BadParameter",
    "Choice",
    "FloatRange",
    "IntRange",
    "error",
    "Extractor",
    "ExtractorContext",
    "ExtractorError",
    "ManifestExtractorContext",
    "SingleFileExtractorContext",
    "TimestampMetadata",
    "input",
    "parameter",
    "manifest_extractor",
    "single_file_extractor",
]
