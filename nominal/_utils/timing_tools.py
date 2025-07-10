from __future__ import annotations

import contextlib
import logging
import time
from types import TracebackType
from typing import Type

from typing_extensions import Self

logger = logging.getLogger(__name__)


class LogTiming(contextlib.ContextDecorator):
    """Timing manager that provides logged timing information for code within a context manager.

    This may be used as a method level decorator to log execution time of the method when called,
    or it may be used directly as a context manager.

    Examples:
        Using as a context manager to time a block of code.

        >>> with LogTiming("Processed 100 records", level=logging.INFO):
        ...     pass
        Processed 100 records (... seconds)

        Using as a decorator to time an entire function.

        @LogTiming("Function `my_function` finished", level=logging.INFO)
        def my_function(name):
            pass

    """

    def __init__(self, message: str, level: int = logging.DEBUG):
        """Initialize and validate the log message and its arguments.

        Args:
            message: Message template string to prefix log statement with
            level: Log level to print timing information with
        """
        self._message = message
        self._log_level = level

        self._start_time = 0.0
        self._end_time = 0.0

    def __enter__(self) -> Self:
        """Track start time of the context manager."""
        self._start_time = time.time()
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Track end time of the context manager and print a log message with timing details."""
        self._end_time = time.time()
        logging.log(self._log_level, "%s (%f seconds)", self._message, self._end_time - self._start_time)
