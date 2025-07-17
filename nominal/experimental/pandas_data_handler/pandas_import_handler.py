from __future__ import annotations

import abc
import gzip
import json
import logging
import multiprocessing
import os
import queue
import signal
import time
from multiprocessing.managers import SyncManager
from multiprocessing.synchronize import Event
from threading import Condition, Thread
from typing import Generic, Iterator, Mapping, Self, TypeVar, cast

import pandas as pd
import pebble
import requests
from pandas._typing import DtypeObj

from nominal._utils import SharedCounter
from nominal.core.datasource import DataSource
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


DEFAULT_NUM_RETRIES = 3
DEFAULT_COMPRESSION_LEVEL = 6
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_NUM_ENCODE_WORKERS = 8
DEFAULT_NUM_UPLOAD_WORKERS = 16
DEFAULT_ENCODE_QUEUE_SIZE = 256
DEFAULT_UPLOAD_QUEUE_SIZE = 2048


def _to_api_json_timestamp(timestamp: IntegralNanosecondsUTC) -> dict[str, int]:
    return {
        "seconds": int(timestamp / 1e9),
        "nanos": int(timestamp % 1e9),
    }


def _to_api_dtype(dtype: DtypeObj) -> str:
    # The linter prefers using `is`, `is not`, or `isinstance` for the following checks,
    # but they don't actually work without much more specific types being used unless
    # `==` is used for the following checks
    if dtype == object:  # noqa: E721
        return "strings"
    elif dtype == int:  # noqa: E721
        return "ints"
    elif dtype == float:  # noqa: E721
        return "doubles"
    else:
        raise ValueError(f"Unknown datatype for streaming data: {dtype}")


def _extract_batches_from_dataframe(
    df: pd.DataFrame, timestamp_column: str, max_batch_size: int
) -> Iterator[tuple[str, pd.DataFrame]]:
    if timestamp_column not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_column}' not found in DataFrame.")
    elif len(df.columns) != len(set(df.columns)):
        raise ValueError(f"Dataframe has duplicate columns: {df.columns}")

    valid_df = df[df[timestamp_column].notna()]
    for col_name in valid_df.columns:
        if col_name == timestamp_column:
            continue

        # 1. Select the current data column and the timestamp column
        # 2. Filter out rows where the current data column is null
        #    This ensures we only process pairs where both value and timestamp are valid
        filtered_df = valid_df[valid_df[col_name].notna()][[col_name, timestamp_column]]

        # If no non-null pairs exist for this column, skip it
        if filtered_df.empty:
            continue

        # Iterate through the column in batches using slicing
        num_rows_filtered = filtered_df.shape[0]
        for offset in range(0, num_rows_filtered, max_batch_size):
            df_slice = filtered_df.iloc[offset : min(offset + max_batch_size, num_rows_filtered)]
            if df_slice.empty:
                continue

            yield col_name, df_slice


class _StopWorking:
    """Sentinel value to tell task workers to stop working."""


T = TypeVar("T")


class StoppableQueue(Generic[T]):
    def __init__(
        self,
        queue: multiprocessing.Queue[T | _StopWorking],  # queue.Queue[T | _StopWorking],
        stop_flag: Event,
        interrupt_flag: Event,
    ):
        self._queue = queue
        self.stop_flag = stop_flag
        self.interrupt_flag = interrupt_flag

    @classmethod
    def from_manager(
        cls,
        manager: SyncManager,
        queue_size: int = 0,
    ):
        # stop_flag = manager.Event()
        # interrupt_flag = manager.Event()
        stop_flag = multiprocessing.Event()
        interrupt_flag = multiprocessing.Event()

        return cls(
            # manager.Queue(maxsize=queue_size),
            multiprocessing.Queue(maxsize=queue_size),
            # manager.Condition(lock),
            # manager.Condition(lock),
            stop_flag,
            interrupt_flag,
        )

    def stop(self) -> None:
        """Immediately stop all processes blocking on the queue and stop future enqueues or dequeues."""
        self.stop_flag.set()

    def interrupt(self, num_stops: int | None = None) -> None:
        """Prevent new items from being added to the queue, useful during shutdown operations"""
        self.interrupt_flag.set()
        if num_stops:
            for _ in range(num_stops):
                self.put(_StopWorking())

    def wait(self) -> None:
        """Blocks until all tasks are completed within the queue.

        Should be called after using interrupt(), but unecessary after a stop()
        """
        while not self.stop_flag.is_set():
            if self._queue.empty():
                return

            time.sleep(0.25)

    def get(self) -> T | None:
        """Block until stop is signalled or data is received and return the oldest member of the queue."""
        while not self.stop_flag.is_set():
            try:
                item = self._queue.get(timeout=0.1)
                if isinstance(item, _StopWorking):
                    return None
                else:
                    return item
            except queue.Empty:
                continue

        return None

    def put(self, item: T | _StopWorking) -> None:
        """Block until stop is signalled or space is available and insert an element in the queue"""
        # If stop flag is set, or the interrupt flag is set and the item isn't a stop work order,
        # stop trying to add to the queue
        while not (self.stop_flag.is_set() or (self.interrupt_flag.is_set() and not isinstance(item, _StopWorking))):
            try:
                self._queue.put(item, timeout=0.1)
                return
            except queue.Full:
                continue


InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class _Worker(abc.ABC, Generic[InputT]):
    def __init__(self, *, logger: logging.Logger | None = None):
        self._input_queue = None
        self._logger = logger

    @property
    @abc.abstractmethod
    def name(self) -> str: ...

    @abc.abstractmethod
    def process(self, task_input: InputT) -> bool: ...

    @property
    def logger(self) -> logging.Logger:
        """Logger instance to use for logging"""
        if self._logger is None:
            self._logger = logging.getLogger(__name__)

            # If we are in a subprocess, use the handlers as setup by the multiprocessing library
            if multiprocessing.parent_process() is not None:
                self._logger.handlers = multiprocessing.get_logger().handlers

        return self._logger

    @property
    def input_queue(self) -> StoppableQueue[InputT]:
        if self._input_queue is None:
            raise RuntimeError("Cannot access input queue... not running!")

        return self._input_queue

    def get_input(self) -> InputT | None:
        start = time.monotonic()
        maybe_task = self.input_queue.get()
        if maybe_task is None:
            return None

        end = time.monotonic()
        diff = end - start
        if diff >= 1.0:
            self.logger.warning("Waited %fs to retrieve task for %s", diff, self.name)

        return maybe_task

    def run(self, input_queue: StoppableQueue[InputT], *, exit_on_exception: bool = False) -> None:
        # Reset logging for task
        self._logger = None
        self._input_queue = input_queue

        while True:
            task_input = self.get_input()
            if task_input is None:
                logger.info("Worker signaled to stop... exiting!")
                return

            try:
                if not self.process(task_input):
                    self.logger.warning("Processing task signalled for worker shutdown... exiting!")
                    return
            except KeyboardInterrupt:
                self.logger.info("User signalled shutdown... exiting!")
                return
            except Exception:
                self.logger.exception("Failed to perform task...")

                # If we should stop work upon any exception... stop working!
                if exit_on_exception:
                    return


class _BiWorker(_Worker[InputT], Generic[InputT, OutputT]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
    ):
        super().__init__(logger=logger)
        self._output_queue = None

    @property
    def output_queue(self) -> StoppableQueue[OutputT]:
        if self._output_queue is None:
            raise RuntimeError("Cannot access output queue... not running!")

        return self._output_queue

    def put_output(self, output: OutputT) -> None:
        start = time.monotonic()
        self.output_queue.put(output)
        end = time.monotonic()
        diff = end - start
        if diff >= 1.0:
            self.logger.warning("Waited %fs to enqueue data for %s", diff, self.name)

    def run(
        self,
        input_queue: StoppableQueue[InputT],
        output_queue: StoppableQueue[OutputT],
        *,
        exit_on_exception: bool = False,
    ) -> None:
        self._output_queue = output_queue
        super().run(input_queue, exit_on_exception=exit_on_exception)


class _EncodeWorker(_BiWorker[pd.DataFrame, bytes]):
    """Worker process to encode dataframes into batched streaming requests"""

    def __init__(
        self,
        datasource_rid: str,
        timestamp_column: str,
        tags: Mapping[str, str] | None,
        compression_level: int,
        batch_size: int,
        points_encoded: SharedCounter,
    ):
        super().__init__()

        self._datasource_rid = datasource_rid
        self._tags = tags or {}
        self._timestamp_column = timestamp_column

        self._compression_level = compression_level
        self._batch_size = batch_size

        self._points_encoded = points_encoded

        # Progress tracking
        self._reset_progress()

    @property
    def name(self) -> str:
        return "Encode Worker"

    def _reset_progress(self) -> None:
        self._task_points = 0
        self._task_encode_time = 0.0
        self._task_compress_time = 0.0
        self._task_enqueue_time = 0.0
        self._task_time = 0.0

    def _log_timing(self) -> None:
        self.logger.info(
            "Spent %fs encoding data with %d points (%f/s) [encoding: %fs] [compressing: %fs] [enqueueing: %fs]",
            self._task_time,
            self._task_points,
            float("inf") if self._task_time == 0 else self._task_points / self._task_time,
            self._task_encode_time,
            self._task_compress_time,
            self._task_enqueue_time,
        )

    def _encode_batch(self, df_slice: pd.DataFrame, value_col: str) -> bytes:
        start = time.monotonic()

        values = df_slice[value_col].to_list()
        timestamps = df_slice[self._timestamp_column].apply(_to_api_json_timestamp).to_list()
        assert len(timestamps) == len(values)

        dtype_str = _to_api_dtype(df_slice[value_col].dtype)
        request = {
            "batches": [
                {
                    "channel": value_col,
                    "timestamps": timestamps,
                    "tags": self._tags,
                    "values": {"type": dtype_str, dtype_str: values},
                },
            ],
            "dataSourceRid": self._datasource_rid,
        }
        encoded = json.dumps(request).encode("utf-8")

        end = time.monotonic()
        diff = end - start
        self._task_encode_time += diff

        num_values = len(values)
        self._task_points += num_values
        self._points_encoded.increment(num_values)

        return encoded

    def _compress_batches(self, encoded_batches: bytes) -> bytes:
        start = time.monotonic()
        compressed_data = gzip.compress(encoded_batches, compresslevel=self._compression_level)
        end = time.monotonic()
        diff = end - start
        self._task_compress_time += diff

        return compressed_data

    def _enqueue_batch_data(self, encoded_data: bytes) -> None:
        start = time.monotonic()
        self.put_output(encoded_data)
        end = time.monotonic()
        diff = end - start
        self._task_enqueue_time += diff

    def process(self, task_input: pd.DataFrame) -> bool:
        start = time.monotonic()
        for column_name, df_slice in _extract_batches_from_dataframe(
            task_input, timestamp_column=self._timestamp_column, max_batch_size=self._batch_size
        ):
            encoded_batch = self._encode_batch(df_slice, column_name)
            compressed_batch = self._compress_batches(encoded_batch)
            self._enqueue_batch_data(compressed_batch)

        end = time.monotonic()
        diff = end - start
        self._task_time += diff

        self._log_timing()
        self._reset_progress()
        return True


class _UploadWorker(_Worker[bytes]):
    """Worker process to upload encoded requests of streaming data to Nominal"""

    def __init__(
        self,
        auth_header: str,
        api_base_url: str,
        num_retries: int,
        bytes_uploaded: SharedCounter,
    ):
        super().__init__()

        # self._input_queue = input_queue
        self._num_retries = num_retries

        self._task_uploading_time = 0.0
        self._bytes_uploaded = bytes_uploaded

        self._headers = {
            "Authorization": auth_header,
            "Content-type": "application/json",
            "Content-Encoding": "gzip",
        }
        self._url = f"{api_base_url}/storage/writer/v1/columnar"

    @property
    def name(self) -> str:
        return "Upload Worker"

    def _upload_data(self, data: bytes) -> bool:
        try_count = 0
        for _ in range(self._num_retries):
            try_count += 1
            req_start = time.monotonic()
            try:
                resp = requests.post(self._url, headers=self._headers, data=data)
            except requests.exceptions.RequestException:
                self.logger.exception("Failed to make request (%d/%d)", try_count, self._num_retries)
                continue
            finally:
                req_end = time.monotonic()
                req_diff = req_end - req_start
                self.logger.debug("Posted batch in %fs (%d/%d)", req_diff, try_count, self._num_retries)

            try:
                resp.raise_for_status()
            except requests.HTTPError:
                self.logger.exception(
                    "Error making request (%d/%d): %s", try_count, self._num_retries, resp.content.decode()
                )
                continue

            return True
        return False

    def process(self, task_input: bytes) -> bool:
        start = time.monotonic()
        success = self._upload_data(task_input)
        end = time.monotonic()
        diff = end - start
        self._task_uploading_time += diff

        byte_count = len(task_input)
        self._bytes_uploaded.increment(byte_count)

        if success:
            self.logger.debug("Successfully uploaded %d bytes in %fs", byte_count, diff)
        else:
            self.logger.error(
                "Failed to upload %d bytes in %d tries! Check Nominal for data integrity",
                byte_count,
                self._num_retries,
            )

        return True


class PandasImportHandler:
    """Manages streaming data into Nominal using pandas dataframes.

    There are two key parts of the import pipeline that occur:
        - Encoding: Converting dataframes into gzipped requests to send to the backend
          with data to ingest.
          - Completely CPU bound task, handled using a pool of subprocesses.
        - Publishing: Sending requests to the backend to kick off streaming ingest.
          - Completely IO bound task, handled using a pool of threads.

    Ingest is exposed both as a instance method and via direct access to a queue.
    There is no difference between using the instance method or the ingest queue, but
    users publishing via a PandasImportHandler from another background pool should prefer
    direct queue access from subprocesses to avoid pickling the import handler to background
    processes.

    There is additionally an queue between encoder workers and publisher workers,
    though, this is not intended for direct use.

    Over the course of operation, the various subprocesses and threads will log if they have waited
    a prolonged period to be able to retrieve (or push) tasks from their respective queues. These may
    be utilized to help tune the number of relative workers between any external extraction pool, the
    encoding pool, and the publishing pool. This will allow for the optimal publishing rate from your
    application.
    """

    def __init__(
        self,
        datasource_rid: str,
        timestamp_column: str,
        auth_header: str,
        api_base_url: str,
        tags: Mapping[str, str] | None = None,
        num_retries: int = DEFAULT_NUM_RETRIES,
        compression_level: int = DEFAULT_COMPRESSION_LEVEL,
        batch_size: int = DEFAULT_BATCH_SIZE,
        num_encode_workers: int = DEFAULT_NUM_ENCODE_WORKERS,
        num_upload_workers: int = DEFAULT_NUM_UPLOAD_WORKERS,
        encode_queue_size: int = DEFAULT_ENCODE_QUEUE_SIZE,
        upload_queue_size: int = DEFAULT_UPLOAD_QUEUE_SIZE,
    ):
        """Initialize PandasImportHandler

        Args:
            datasource_rid: RID of the datasource to stream to
            timestamp_column: Name of the timestamp column of dataframes to upload
            auth_header: Auth header for use with the Nominal API
            api_base_url: Base URL of the Nominal API
            num_retries: Number of retries to use when uploading to the Nominal API
            tags: Key-value pairs to tag data with
            compression_level: Level of compression to use with GZIP
            batch_size: Maximum number of points per request to Nominal
            num_encode_workers: Number of background processes to use for encoding dataframes to backend requests.
            num_upload_workers: Number of background threads to use for sending requests to the Nominal backend.
            encode_queue_size: Size of the queue for dataframes to be ingested into Nominal
            upload_queue_size: Size of the queue for requests to be sent to the Nominal backend.
        """
        self._datasource_rid = datasource_rid
        self._timestamp_column = timestamp_column
        self._auth_header = auth_header
        self._api_base_url = api_base_url
        self._num_retries = num_retries
        self._tags = tags
        self._compression_level = compression_level
        self._batch_size = batch_size

        self._started = False

        # Used to manage shared state between background processes
        self._manager: SyncManager | None = None

        # None queued values are used to signal background processes to stop processing
        self._encode_queue: StoppableQueue[pd.DataFrame] | None = None
        self._encode_queue_size = encode_queue_size

        self._encode_pool: pebble.ProcessPool | None = None
        self._encode_pool_size = num_encode_workers

        # None queued values are used to signal background threads to stop processing
        self._upload_queue: StoppableQueue[bytes] | None = None
        self._upload_queue_size = upload_queue_size

        self._upload_pool: pebble.ThreadPool | None = None
        self._upload_pool_size = num_upload_workers

        self._points_encoded: SharedCounter | None = None
        self._bytes_uploaded: SharedCounter | None = None

    @classmethod
    def from_datasource(  # type: ignore[no-untyped-def]
        cls,
        datasource: DataSource,
        timestamp_column: str,
        **kwargs,
    ) -> Self:
        return cls(
            datasource_rid=datasource.rid,
            timestamp_column=timestamp_column,
            auth_header=datasource._clients.auth_header,
            api_base_url=datasource._clients.channel_metadata._uri,
            **kwargs,
        )

    @property
    def ingest_queue(self) -> StoppableQueue[pd.DataFrame]:
        """Queue for directly scheduling data to be published to Nominal.

        May be used within subprocesses.
        """
        if self._encode_queue is None:
            raise RuntimeError("Cannot access ingest queue-- import handler has not been started")

        return self._encode_queue

    @property
    def upload_queue(self) -> StoppableQueue[bytes]:
        """Queue for directly scheduling requests to be published to the Nominal backend.

        May be used within subprocesses.
        """
        if self._upload_queue is None:
            raise RuntimeError("Cannot access upload queue-- import handler has not been started")

        return self._upload_queue

    @property
    def points_encoded(self) -> float:
        return 0.0 if self._points_encoded is None else self._points_encoded.value()

    @property
    def bytes_uploaded(self) -> float:
        return 0.0 if self._bytes_uploaded is None else self._bytes_uploaded.value()

    def ingest(self, data: pd.DataFrame) -> None:
        """Ingest data to Nominal."""
        self.ingest_queue.put(data)

    def start(self) -> None:
        """Start background processes and prepare the handler for import."""
        if self._started:
            logger.warning("Import handler already started-- not starting.")
            return

        self._manager = multiprocessing.Manager()

        # Start background pools and workers
        self._encode_queue = StoppableQueue.from_manager(
            self._manager,
            queue_size=self._encode_queue_size,
        )
        self._upload_queue = StoppableQueue.from_manager(
            self._manager,
            queue_size=self._upload_queue_size,
        )
        self._encode_pool = pebble.ProcessPool(max_workers=self._encode_pool_size)
        self._upload_pool = pebble.ThreadPool(max_workers=self._upload_pool_size)

        self._points_encoded = SharedCounter.from_manager(self._manager)
        self._bytes_uploaded = SharedCounter.from_manager(self._manager)

        self._encode_workers = [
            _EncodeWorker(
                datasource_rid=self._datasource_rid,
                timestamp_column=self._timestamp_column,
                tags=self._tags,
                compression_level=self._compression_level,
                batch_size=self._batch_size,
                points_encoded=self._points_encoded,
            )
            for _ in range(self._encode_pool_size)
        ]
        self._encode_processes = [
            multiprocessing.Process(
                target=worker.run,
                args=(
                    self._encode_queue,
                    self._upload_queue,
                ),
                daemon=True,
            )
            for worker in self._encode_workers
        ]
        for proc in self._encode_processes:
            proc.start()
        # self._encode_futures = [
        #     self._encode_pool.schedule(
        #         worker.run,
        #         args=[
        #             self._encode_queue,
        #             self._upload_queue,
        #         ],
        #     )
        #     for worker in self._encode_workers
        # ]

        self._upload_workers = [
            _UploadWorker(
                auth_header=self._auth_header,
                api_base_url=self._api_base_url,
                num_retries=self._num_retries,
                bytes_uploaded=self._bytes_uploaded,
            )
            for _ in range(self._upload_pool_size)
        ]
        self._upload_threads = [
            Thread(
                target=worker.run,
                args=(self._upload_queue,),
                daemon=True,
            )
            for worker in self._upload_workers
        ]
        for thread in self._upload_threads:
            thread.start()
        # self._upload_futures = [
        #     self._upload_pool.schedule(worker.run, args=[self._upload_queue]) for worker in self._upload_workers
        # ]

        self._started = True

    def teardown(self) -> None:
        """Immediately terminate background processes and shutdown handler."""
        if not self._started:
            logger.warning("Import handler not started-- not tearing down!")
            return

        if self._encode_queue:
            self._encode_queue.stop()
            self._encode_queue = None

        if self._upload_queue:
            self._upload_queue.stop()
            self._upload_queue = None

        for proc in self._encode_processes:
            proc.terminate()

        if self._encode_pool:
            self._encode_pool.stop()  # type: ignore[no-untyped-call]
            self._encode_pool.join()
            self._encode_pool = None

        if self._upload_pool:
            self._upload_pool.stop()  # type: ignore[no-untyped-call]
            self._upload_pool.join()
            self._upload_pool = None

        if self._manager:
            print("Shutting down manager")
            self._manager.shutdown()
            self._manager = None

        self._started = False

    def stop(self) -> None:
        """Gracefully stops background processes.

        Call teardown() after to free resources
        """
        if not self._started:
            logger.warning("Import handler not started-- not stopping.")
            return

        logger.info("Scheduling stop requests for encode workers")
        self.ingest_queue.interrupt(num_stops=self._encode_pool_size)
        logger.info("Awaiting encode tasks to finish")
        self.ingest_queue.wait()
        for proc in self._encode_processes:
            proc.join()

        logger.info("Scheduling stop requests for upload workers")
        self.upload_queue.interrupt(num_stops=self._upload_pool_size)
        logger.info("Awaiting upload tasks to finish")
        self.upload_queue.wait()
        for thread in self._upload_threads:
            thread.join()
