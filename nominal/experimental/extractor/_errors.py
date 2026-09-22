"""Structured failure reporting for extractor entrypoints."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, ParamSpec

from nominal.experimental.extractor._definition import _declare, _DeclaredCallback

logger = logging.getLogger(__name__)

_MAX_TERMINATION_BYTES = 4096
_P = ParamSpec("_P")


@dataclass(frozen=True)
class _ErrorMapping:
    """Report a matching exception with ``code``, its message, and a nonzero exit status.

    ``code`` should match the extractor's registered catalog error code. ``retryable``
    describes whether retrying the ingest may succeed without changing the input. The code
    must fit the 4,096-byte termination JSON envelope; long messages are shortened to fit.
    Mapped failures retain their full traceback on stderr.
    """

    exception_type: type[Exception]
    code: str
    exit_code: int
    retryable: bool = False
    message: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.exception_type, type) or not issubclass(self.exception_type, Exception):
            raise TypeError("error exception_type must be an Exception subclass")
        if not isinstance(self.code, str) or not self.code.strip():
            raise ValueError("code must be a non-empty string")
        if type(self.exit_code) is not int or not 1 <= self.exit_code <= 255:
            raise ValueError("exit_code must be an integer between 1 and 255")
        if not isinstance(self.retryable, bool):
            raise ValueError("retryable must be a bool")
        if len(self._serialize("").encode("utf-8")) > _MAX_TERMINATION_BYTES:
            raise ValueError("code must fit within the 4096-byte termination message envelope")

    def __call__(self, fn: Callable[_P, None]) -> _DeclaredCallback[_P]:
        callback = _declare(fn)
        if self.exception_type in callback.errors:
            raise ValueError(f"duplicate error declaration for {self.exception_type.__name__}")
        callback.errors[self.exception_type] = self
        return callback

    def _serialize(self, message: str) -> str:
        return json.dumps({"code": self.code, "message": message, "retryable": self.retryable})

    def report(self, error: BaseException, termination_log_path: str | Path) -> None:
        """Write bounded JSON to the termination log and stderr, preserving error identity."""
        message = str(error)
        low, high = 0, min(len(message), _MAX_TERMINATION_BYTES)
        # Keep the longest prefix that fits after JSON escaping, without serializing an
        # unbounded message or truncating the resulting JSON document.
        while low < high:
            middle = (low + high + 1) // 2
            if len(self._serialize(message[:middle]).encode("utf-8")) <= _MAX_TERMINATION_BYTES:
                low = middle
            else:
                high = middle - 1
        payload = self._serialize(message[:low])
        try:
            Path(termination_log_path).write_text(payload, encoding="utf-8")
        except OSError:
            # Best effort: the registered exit-code mapping remains the platform fallback.
            logger.debug("termination log write failed; retaining stderr and exit-code reporting")
        else:
            logger.debug("wrote termination log (%d bytes)", len(payload.encode("utf-8")))
        print(payload, file=sys.stderr)


def _resolve_error(errors: Mapping[type[Exception], _ErrorMapping], error: BaseException) -> _ErrorMapping | None:
    """Use the closest mapped Exception class; never map process-control BaseExceptions."""
    if isinstance(error, Exception):
        for exception_type in type(error).__mro__:
            if (mapping := errors.get(exception_type)) is not None:
                return mapping
    return None
