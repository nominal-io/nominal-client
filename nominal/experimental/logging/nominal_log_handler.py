import atexit
import datetime
import logging
import queue
import threading
import time

from nominal.core.dataset import Dataset
from nominal.core.log import LogPoint

DEFAULT_LOG_BATCH_SIZE = 1000
DEFAULT_LOG_FLUSH_INTERVAL = datetime.timedelta(seconds=5)


class NominalLogHandler(logging.Handler):
    """A custom logging handler that batches log records and sends them to Nominal in a background thread.

    NOTE: to log custom args from a `logger.log(...)` statement, you can pass args as a dictionary via `extras`
          Example:
            logger.info("infotainment logs", extra={"nominal_args": {"country": "america", "count": 1234}})
          This would allow users to see the custom log args within the Nominal log panel.

    NOTE: it is recommended to NOT install this on the root logger, as otherwise, logs that occur during
          log uploading (e.g. in urllib) will result in an infinite chain of logs being produced to the
          dataset.
    """

    def __init__(
        self,
        dataset: Dataset,
        max_batch_size: int = DEFAULT_LOG_BATCH_SIZE,
        flush_interval: datetime.timedelta = DEFAULT_LOG_FLUSH_INTERVAL,
        max_queue_size: int = 0,
    ):
        """Initializes the handler.

        Args:
        dataset: The dataset object with a `write_logs` method.
        max_batch_size: The maximum number of records to hold in the queue before flushing.
        flush_interval: The maximum time to wait before flushing the queue.
        max_queue_size: Maximum size of the internal log message queue. Set to 0 for unbounded size.
        """
        super().__init__()
        self.dataset = dataset
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval

        self.queue = queue.Queue(maxsize=max_queue_size)
        self.last_flush_time = time.monotonic()

        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

        atexit.register(self.shutdown)

    def emit(self, record: logging.LogRecord) -> None:
        """Puts a log record into the queue"""
        try:
            extra_data = getattr(record, "nominal_args") if hasattr(record, "nominal_args") else {}
            args = {
                "level": record.levelname,
                "filename": record.filename,
                "function": record.funcName,
                "line": str(record.lineno),
                **{str(k): str(v) for k, v in extra_data.items()},
            }

            log_entry = LogPoint(int(record.created * 1e9), message=self.format(record), args=args)
            self.queue.put(log_entry, block=False)
        except queue.Full:
            # Handle the case where the queue is full by logging a warning on the root logger
            logging.warning("Nominal Log queue is full, dropping new log messages.")
        except Exception:
            self.handleError(record)

    def _get_batch(self) -> list[LogPoint]:
        """Retrieves a batch of log records from the queue."""
        batch = []
        while not self.queue.empty() and len(batch) < self.max_batch_size:
            try:
                batch.append(self.queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _worker(self):
        """The background thread that processes the log queue."""
        while True:
            time_since_last_flush = time.monotonic() - self.last_flush_time
            if self.queue.qsize() >= self.max_batch_size or (
                self.queue.qsize() > 0 and time_since_last_flush >= self.flush_interval.total_seconds()
            ):
                batch = self._get_batch()
                if batch:
                    try:
                        self.dataset.write_logs(batch)
                        self.last_flush_time = time.monotonic()
                    except Exception:
                        # Handle exceptions from the backend here.
                        logging.exception("Error writing logs to Nominal")
            else:
                # Sleep for a short duration to avoid busy-waiting.
                time.sleep(0.1)

    def shutdown(self):
        """Flushes any remaining logs in the queue before the application exits."""
        # Signal the worker to flush everything
        remaining_logs = self._get_batch()
        if remaining_logs:
            try:
                self.dataset.write_logs(remaining_logs)
            except Exception:
                logging.exception("Error flushing logs to Nominal at program shutdown")
        self.queue.join()
