"""Resolve declared file inputs and scalar settings into callback arguments."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, ParamSpec

from nominal.core.container_image import FileExtractionInput, FileExtractionParameter
from nominal.core.exceptions import ExtractorError
from nominal.experimental.extractor._definition import _MISSING, _declare, _DeclaredCallback
from nominal.experimental.extractor.context import ExtractorContext
from nominal.experimental.extractor.types import BadParameter

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")


@dataclass(frozen=True)
class _Input:
    """Bind one callback argument to a registered file input.

    The same declaration attaches to the callback, supplies registration metadata, and resolves
    a mounted file at runtime. It owns optional-file handling and rejects supplied non-file paths.
    """

    argument: str
    spec: FileExtractionInput

    def resolve(self, ctx: ExtractorContext) -> object:
        path = ctx._resolve_input(self.spec.environment_variable, optional=not self.spec.required, envvar_only=True)
        if path is not None and not path.is_file():
            raise ExtractorError(f"input {self.argument!r} ({self.spec.environment_variable}) is not a file")
        logger.debug("input %s is %s", self.argument, "omitted" if path is None else "resolved")
        return path

    def __call__(self, fn: Callable[_P, None]) -> _DeclaredCallback[_P]:
        callback = _declare(fn)
        callback.arguments = (self, *callback.arguments)
        return callback


@dataclass(frozen=True)
class _Parameter:
    """Bind one callback argument to a registered scalar setting and its conversion policy.

    Requiredness, an already-converted default, and the converter travel with the registration
    spec. Resolution distinguishes absence from an empty value and sanitizes ValueError/TypeError
    before the runner applies error mappings.
    """

    argument: str
    spec: FileExtractionParameter
    converter: Callable[[str], object]
    default: object

    def resolve(self, ctx: ExtractorContext) -> object:
        raw = ctx._resolve_param(self.spec.environment_variable, envvar_only=True)
        if raw is None:
            if self.default is _MISSING:
                raise ExtractorError(
                    f"required parameter {self.argument!r} ({self.spec.environment_variable}) is not set"
                )
            logger.debug("using default for parameter %s", self.argument)
            return self.default
        try:
            logger.debug("converting supplied parameter %s", self.argument)
            return self.converter(raw)
        except BadParameter as error:
            raise ExtractorError(
                f"cannot convert parameter {self.argument!r} ({self.spec.environment_variable}): {error}"
            ) from None
        except (ValueError, TypeError):
            # Converter messages may contain credentials or other private parameter values.
            raise ExtractorError(
                f"cannot convert parameter {self.argument!r} ({self.spec.environment_variable})"
            ) from None

    def __call__(self, fn: Callable[_P, None]) -> _DeclaredCallback[_P]:
        callback = _declare(fn)
        callback.arguments = (self, *callback.arguments)
        return callback
