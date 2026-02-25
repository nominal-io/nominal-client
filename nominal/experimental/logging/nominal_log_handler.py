from __future__ import annotations

import datetime
import logging
from types import TracebackType
from typing import Mapping, Type

from nominal.core.dataset import Dataset
from nominal.core.log import LogPoint


class NominalLogHandler(logging.Handler):
    """A custom logging handler that batches log records and sends them to Nominal in a background thread.

    NOTE: to log custom args from a `logger.log(...)` statement, you can pass args as a dictionary via `extras`
          Example:
            logger.info("infotainment logs", extra={"nominal_args": {"country": "america", "count": 1234}})
          This would allow users to see the custom log args within the Nominal log panel.
    """

    def __init__(
        self,
        dataset: Dataset,
        log_channel: str = "logs",
        max_batch_size: int = 50_000,
        flush_interval: datetime.timedelta = datetime.timedelta(seconds=1),
        default_args: Mapping[str, str] | None = None,
    ):
        """Initializes the handler.

        Args:
            dataset: The dataset object with a `write_logs` method.
            log_channel: The channel within the dataset to send logs to
            max_batch_size: The maximum number of records to hold in the queue before flushing.
            flush_interval: The maximum time to wait before flushing the queue.
            default_args: Default key-value pairs to use as arg in all log messages
        """
        super().__init__()
        self._log_stream = dataset.get_log_stream(
            batch_size=max_batch_size,
            max_wait=flush_interval,
        )
        self._log_channel = log_channel
        self._default_args = default_args or {}

    def emit(self, record: logging.LogRecord) -> None:
        """Puts a log record into the queue"""
        if not self.filter(record):
            return

        extra_data = getattr(record, "nominal_args") if hasattr(record, "nominal_args") else {}
        args = {
            "level": record.levelname,
            "filename": record.filename,
            "function": record.funcName,
            "line": str(record.lineno),
            **self._default_args,
            **{str(k): str(v) for k, v in extra_data.items()},
        }
        log_entry = LogPoint(int(record.created * 1e9), message=self.format(record), args=args)

        try:
            self._log_stream.enqueue(self._log_channel, log_entry.timestamp, log_entry.message, log_entry.args)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        """Shutoff log handler from sending logs to Nominal"""
        self._log_stream.close()

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the stream and close out any used system resources."""
        self.close()


class ModuleFilter(logging.Filter):
    """A logging filter that excludes records from specified modules."""

    def __init__(self, excluded_modules: set[str]):
        """Initializes the filter with a set of module names to exclude.

        Args:
            excluded_modules: A set of module prefixes to exclude from logging.
        """
        super().__init__()
        self.excluded_modules = excluded_modules

    def filter(self, record: logging.LogRecord) -> bool:
        """Determines if a log record should be processed.

        Args:
            record: The log record to be checked.

        Returns:
            bool: True if the record should be logged, False otherwise.
        """
        # The logger's name is often the module's name.
        # We check if the logger's name starts with any of the excluded module names.
        return not any(record.name.startswith(mod) for mod in self.excluded_modules)


def install_nominal_log_handler(
    dataset: Dataset,
    *,
    log_channel: str = "logs",
    level: int = logging.INFO,
    logger: logging.Logger | None = None,
    default_args: Mapping[str, str] | None = None,
) -> NominalLogHandler:
    """Install and configure a NominalLogHandler on the provided logger instance.

    Args:
        dataset: Nominal dataset to send logs to
        log_channel: Nominal channel to send logs to within the provided dataset
        level: Minimum log level to send to Nominal
        logger: Logger instance to attach the log handler to. Attaches to the root logger by default
        default_args: Key-value arguments to apply to all log messages by default

    Returns:
        Attached NominalLogHandler
    """
    if logger is None:
        logger = logging.getLogger()

    handler = NominalLogHandler(dataset, log_channel=log_channel, default_args=default_args)
    handler.setLevel(level)
    # Logs from urllib3 while uploading logs result in an infinite loop of producing logs
    # while uploading logs to Nominal. They are typically pretty spammy logs anyways, so
    # not particularly relevant to see in a log panel within Nominal.
    handler.addFilter(ModuleFilter(set(["urllib3.connectionpool"])))

    logger.addHandler(handler)
    return handler
