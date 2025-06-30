from __future__ import annotations

import contextlib
import logging
import time
from types import TracebackType
from typing import Any, Type

from typing_extensions import Self

logger = logging.getLogger(__name__)


class LogTiming(contextlib.ContextDecorator):
    """Timing manager that provides logged timing information for code within a context manager.

    This may be used as a method level decorator to log execution time of the method when called,
    or it may be used directly as a context manager.

    Examples:
        Using as a context manager to time a block of code.

        >>> with LogTiming("Processed %d records", 100, level=logging.INFO):
        ...     pass
        Processed 100 records (... seconds)

        Using as a decorator to time an entire function.

        @LogTiming("Function `my_function` finished", level=logging.INFO)
        def my_function(name):
            pass

        Incorrect usage will raise an error immediately.

        >>> try:
        ...     timer = LogTiming("Mismatched formatter %s %d", "hello")
        ... except TypeError as e:
        ...     print("Caught expected error!")
        Caught expected error!

    """

    def __init__(self, message: str, *log_args: Any, level: int = logging.DEBUG):
        """Initialize and validate the log message and its arguments.

        Args:
            message: Message template string to prefix log statement with (use standard logging format strings)
            *log_args: Log arguments to interpolate into the provided `message` much like you would use with `logging`
            level: Log level to print timing information with
        """
        self._message = message
        self._log_args = log_args
        self._log_level = level

        # Ensure user provided args / message are compatible
        self._validate_log_format()

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
        logging.log(
            self._log_level, f"{self._message} (%f seconds)", *self._log_args, self._end_time - self._start_time
        )

    def _validate_log_format(self) -> None:
        """Validate that the provided log message prefix and arguments would properly format.

        Throws:
            TypeError: Message would fail to format
        """
        try:
            # We create a dummy LogRecord to use the logging module's own
            # formatting logic. This is the most reliable way to check.
            # The getMessage() method will raise TypeError or ValueError
            # if the message and args don't match.
            record = logging.LogRecord(
                name="validation",
                level=self._log_level,
                pathname="",
                lineno=0,
                msg=self._message,
                args=self._log_args,
                exc_info=None,
            )

            # Ensures there aren't too many args or incorrectly typed args
            record.getMessage()

            # Ensures there's enough args
            _ = self._message % self._log_args
        except (TypeError, ValueError) as e:
            # Re-raise with a more user-friendly message, chaining the original exception.
            raise TypeError(f"Log message {self._message!r} and arguments {self._log_args} are not compatible.") from e
