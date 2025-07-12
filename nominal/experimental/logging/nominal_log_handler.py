from __future__ import annotations

import atexit
import datetime
import logging
import queue
import threading
import time

from nominal.core.dataset import Dataset
from nominal.core.log import LogPoint

DEFAULT_LOG_CHANNEL = "logs"
DEFAULT_LOG_BATCH_SIZE = 1000
DEFAULT_LOG_FLUSH_INTERVAL = datetime.timedelta(seconds=1)


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
        log_channel: str = DEFAULT_LOG_CHANNEL,
        max_batch_size: int = DEFAULT_LOG_BATCH_SIZE,
        flush_interval: datetime.timedelta = DEFAULT_LOG_FLUSH_INTERVAL,
        max_queue_size: int = 0,
    ):
        """Initializes the handler.

        Args:
        dataset: The dataset object with a `write_logs` method.
        log_channel: The channel within the dataset to send logs to
        max_batch_size: The maximum number of records to hold in the queue before flushing.
        flush_interval: The maximum time to wait before flushing the queue.
        max_queue_size: Maximum size of the internal log message queue. Set to 0 for unbounded size.
        """
        super().__init__()
        self.dataset = dataset
        self.log_channel = log_channel
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval

        self.queue: queue.Queue[LogPoint] = queue.Queue(maxsize=max_queue_size)

        self._worker_thread: threading.Thread | None = None
        self._last_flush_time = 0.0

        # Coordinate notifications to worker thread
        self._condition = threading.Condition()
        self._should_shutdown = False

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
            **{str(k): str(v) for k, v in extra_data.items()},
        }
        log_entry = LogPoint(int(record.created * 1e9), message=self.format(record), args=args)

        try:
            self.queue.put(log_entry, block=False)

            # Notify worker that a new log point is available
            with self._condition:
                self._condition.notify()
        except queue.Full:
            # Handle the case where the queue is full by logging a warning on the root logger
            logging.warning("Nominal Log queue is full, dropping new log messages.")
        except Exception:
            self.handleError(record)

    def _get_batch(self) -> list[LogPoint]:
        """Retrieves a batch of log records from the queue."""
        batch: list[LogPoint] = []
        while not self.queue.empty() and len(batch) < self.max_batch_size:
            try:
                batch.append(self.queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _flush_batch(self, batch: list[LogPoint]) -> None:
        """Flush a batch of logs to Nominal."""
        if batch:
            try:
                self.dataset.write_logs(batch, channel_name=self.log_channel, batch_size=self.max_batch_size)
                self._last_flush_time = time.monotonic()
            except Exception:
                logging.exception("Error writing logs to Nominal")

    def _time_until_flush_due(self) -> float:
        """Time in seconds until the next flush should happen

        NOTE: may be negative if we have not flushed since longer than the configured interval
        """
        time_since_last_flush = time.monotonic() - self._last_flush_time
        return self.flush_interval.total_seconds() - time_since_last_flush

    def _should_flush(self) -> bool:
        """Whether or not we should flush immediately based on time or batch size."""
        flush_due = self._time_until_flush_due() <= 0
        batch_full = self.queue.qsize() >= self.max_batch_size
        return batch_full or (flush_due and not self.queue.empty())

    def _worker(self) -> None:
        """The background thread that processes the log queue."""
        while not self._should_shutdown:
            batch = None
            with self._condition:
                # If the queue is empty and we aren't shutting down, await new logs
                if self.queue.empty() and not self._should_shutdown:
                    self._condition.wait()

                # If a shutdown is requested, exit
                if self._should_shutdown:
                    break

                # If we have logs, but not a full batch worth, we wait up until the
                # flush interval for more logs, or until a new log has been enqueued
                if not self._should_flush():
                    self._condition.wait(self._time_until_flush_due())

                # If our flushing conditions have been satisfied, extract a batch to push to nominal
                if self._should_flush():
                    batch = self._get_batch()

            # If a batch was extracted, flush the batch now that the condition lock has been released
            if batch:
                self._flush_batch(batch)
                batch = None

        # On shutdown, flush any remaining items in the queue
        batch = self._get_batch()
        while batch:
            self._flush_batch(batch)
            batch = self._get_batch()

    def start(self) -> None:
        """Start background workers for uploading logs to Nominal."""
        # Already started
        if self._worker_thread is not None:
            return

        self._last_flush_time = time.monotonic()
        self._should_shutdown = False

        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()

        # Shut down this logger before exiting
        atexit.register(self.shutdown)

    def shutdown(self) -> None:
        """Flushes any remaining logs in the queue before the application exits."""
        # Not started
        if self._worker_thread is None:
            return

        # Signal worker to shutdown, wake it up in case it was awaiting logs
        with self._condition:
            self._should_shutdown = True
            self._condition.notify_all()

        # Await worker to finish processing
        self._worker_thread.join()
        self._worker_thread = None

        try:
            atexit.unregister(self.shutdown)
        except Exception:
            pass


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
    dataset: Dataset, *, log_channel: str = "logs", level: int = logging.INFO, logger: logging.Logger | None = None
) -> NominalLogHandler:
    """Install and configure a NominalLogHandler on the provided logger instance.

    Args:
        dataset: Nominal dataset to send logs to
        log_channel: Nominal channel to send logs to within the provided dataset
        level: Minimum log level to send to Nominal
        logger: Logger instance to attach the log handler to. Attaches to the root logger by default

    Returns:
        Attached NominalLogHandler
    """
    if logger is None:
        logger = logging.getLogger()

    handler = NominalLogHandler(dataset, log_channel=log_channel)
    handler.setLevel(level)
    # Logs from urllib3 while uploading logs result in an infinite loop of producing logs
    # while uploading logs to Nominal. They are typically pretty spammy logs anyways, so
    # not particularly relevant to see in a log panel within Nominal.
    handler.addFilter(ModuleFilter(set(["urllib3.connectionpool"])))
    handler.start()

    logger.addHandler(handler)
    return handler
