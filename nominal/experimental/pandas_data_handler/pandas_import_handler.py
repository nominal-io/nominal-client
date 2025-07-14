from __future__ import annotations

import abc
import gzip
import json
import logging
import multiprocessing
import queue
import time
from multiprocessing.managers import SyncManager
from typing import Iterator, Mapping, cast

import pandas as pd
import pebble
import requests
from pandas._typing import DtypeObj

from nominal.core.datasource import DataSource
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


def _to_api_json_timestamp(timestamp: IntegralNanosecondsUTC) -> dict[str, int]:
    return {
        "seconds": int(timestamp / 1e9),
        "nanos": int(timestamp % 1e9),
    }


def _to_api_dtype(dtype: DtypeObj) -> str:
    if dtype == object:
        return "strings"
    elif dtype == int:
        return "ints"
    elif dtype == float:
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


class _TaskWorker(abc.ABC):
    def __init__(self):
        self.logger = multiprocessing.get_logger()

    @abc.abstractmethod
    def _run_task(self) -> bool:
        """Run a single task.

        Returns:
            True if should continue to accept more tasks, or False if not.
        """

    def run(self) -> None:
        while True:
            try:
                if not self._run_task():
                    self.logger.info("Stop request received! Shutting down worker...")
                    return
            except KeyboardInterrupt:
                self.logger.info("User requested shutdown...")
                return
            except Exception:
                self.logger.exception("Failed to perform task")


class _EncodeWorker(_TaskWorker):
    def __init__(
        self,
        input_queue: queue.Queue[pd.DataFrame | None],
        output_queue: queue.Queue[bytes | None],
        datasource_rid: str,
        timestamp_column: str,
        tags: Mapping[str, str] | None,
        compression_level: int,
        batch_size: int,
    ):
        super().__init__()

        self._input_queue = input_queue
        self._output_queue = output_queue

        self._datasource_rid = datasource_rid
        self._tags = tags or {}
        self._timestamp_column = timestamp_column

        self._compression_level = compression_level
        self._batch_size = batch_size

        # Progress tracking
        self._reset_progress()

    def _reset_progress(self) -> None:
        self._total_points = 0
        self._task_points = 0
        self._task_encode_time = 0.0
        self._task_compress_time = 0.0
        self._task_enqueue_time = 0.0
        self._task_time = 0.0

    def _log_timing(self) -> None:
        self.logger.debug(
            "Spent %fs encoding data with %d points (%f/s) [encoding: %fs] [compressing: %fs] [enqueueing: %fs]",
            self._task_time,
            self._task_points,
            float("inf") if self._task_time == 0 else self._task_points / self._task_time,
            self._task_encode_time,
            self._task_compress_time,
            self._task_enqueue_time,
        )

    def _retrieve_df(self) -> pd.DataFrame | None:
        start = time.monotonic()
        df = self._input_queue.get()
        end = time.monotonic()

        diff = end - start
        if diff >= 1.0:
            self.logger.warning("Waited %fs to retrieve encode task", diff)

        return df

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
        self._task_points += len(values)
        self._total_points += len(values)

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
        self._output_queue.put(encoded_data)
        end = time.monotonic()

        diff = end - start
        self._task_encode_time += diff
        if diff >= 1.0:
            self.logger.warning("Waited %fs to enqueue encoded data", diff)

    def _run_task(self) -> bool:
        df = self._retrieve_df()
        if df is None:
            return False

        start = time.monotonic()
        for column_name, df_slice in _extract_batches_from_dataframe(
            df, timestamp_column=self._timestamp_column, max_batch_size=self._batch_size
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


class _UploadWorker(_TaskWorker):
    def __init__(self, input_queue: queue.Queue[bytes | None], auth_header: str, api_base_url: str, num_retries: int):
        super().__init__()

        self._input_queue = input_queue
        self._num_retries = num_retries

        self._task_uploading_time = 0.0
        self._bytes_uploaded = 0.0

        self._headers = {
            "Authorization": auth_header,
            "Content-type": "application/json",
            "Content-Encoding": "gzip",
        }
        self._url = f"{api_base_url}/storage/writer/v1/columnar"

    def _retrieve_request(self) -> bytes | None:
        start = time.monotonic()
        data = self._input_queue.get()
        end = time.monotonic()
        diff = end - start
        if diff >= 1.0:
            self.logger.warning("Waited %fs to retrieve upload task", diff)

        return data

    def _upload_data(self, batch_data: bytes) -> bool:
        byte_count = len(batch_data)
        success = False
        try_count = 0
        start = time.monotonic()
        for _ in range(self._num_retries):
            try_count += 1
            req_start = time.monotonic()
            try:
                resp = requests.post(self._url, headers=self._headers, data=batch_data)
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

            success = True
            break

        end = time.monotonic()
        diff = end - start
        self._task_uploading_time += diff
        self._bytes_uploaded += byte_count

        if success:
            self.logger.debug("Successfully uploaded %d bytes in %fs (%d attempts)", byte_count, diff, try_count)
        else:
            self.logger.error("Failed to upload %d bytes in %d tries!", byte_count, self._num_retries)

        return success

    def _run_task(self) -> bool:
        req = self._retrieve_request()
        if req is None:
            return False

        if not self._upload_data(req):
            self.logger.error("Some data failed to upload! Check Nominal to ensure data integrity!")

        return True


DEFAULT_NUM_RETRIES = 3
DEFAULT_COMPRESSION_LEVEL = 6
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_NUM_ENCODE_WORKERS = 4
DEFAULT_NUM_UPLOAD_WORKERS = 16
DEFAULT_ENCODE_QUEUE_SIZE = 256
DEFAULT_UPLOAD_QUEUE_SIZE = 2048


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
        self._encode_queue: queue.Queue[pd.DataFrame | None] | None = None
        self._encode_queue_size = encode_queue_size

        self._encode_pool: pebble.ProcessPool | None = None
        self._encode_pool_size = num_encode_workers

        # None queued values are used to signal background threads to stop processing
        self._upload_queue: queue.Queue[bytes | None] | None = None
        self._upload_queue_size = upload_queue_size

        self._upload_pool: pebble.ThreadPool | None = None
        self._upload_pool_size = num_upload_workers

    @classmethod
    def from_datasource(
        cls,
        datasource: DataSource,
        timestamp_column: str,
        **kwargs,
    ):
        return cls(
            datasource_rid=datasource.rid,
            timestamp_column=timestamp_column,
            auth_header=datasource._clients.auth_header,
            api_base_url=datasource._clients.channel_metadata._uri,
            **kwargs,
        )

    @property
    def ingest_queue(self) -> queue.Queue[pd.DataFrame]:
        """Queue for directly scheduling data to be published to Nominal.

        May be used within subprocesses.
        """
        if self._encode_queue is None:
            raise RuntimeError("Cannot access ingest queue-- import handler has not been started")

        return cast(queue.Queue[pd.DataFrame], self._encode_queue)

    def ingest(self, data: pd.DataFrame) -> None:
        """Ingest data to Nominal."""
        self.ingest_queue.put(data)

    @property
    def upload_queue(self) -> queue.Queue[bytes]:
        """Queue for directly scheduling requests to be published to the Nominal backend.

        May be used within subprocesses.
        """
        if self._upload_queue is None:
            raise RuntimeError("Cannot access upload queue-- import handler has not been started")

        return cast(queue.Queue[bytes], self._upload_queue)

    def start(self) -> None:
        """Start background processes and prepare the handler for import."""
        if self._started:
            logger.warning("Import handler already started-- not starting.")
            return

        self._manager = multiprocessing.Manager()

        # Start background pools and workers
        self._encode_queue = self._manager.Queue(self._encode_queue_size)
        self._encode_pool = pebble.ProcessPool(max_workers=self._encode_pool_size)

        self._upload_queue = self._manager.Queue(self._upload_queue_size)
        self._upload_pool = pebble.ThreadPool(max_workers=self._upload_pool_size)

        self._encode_workers = [
            _EncodeWorker(
                input_queue=self._encode_queue,
                output_queue=self._upload_queue,
                datasource_rid=self._datasource_rid,
                timestamp_column=self._timestamp_column,
                tags=self._tags,
                compression_level=self._compression_level,
                batch_size=self._batch_size,
            )
            for _ in range(self._encode_pool_size)
        ]
        self._encode_futures = [self._encode_pool.schedule(worker.run) for worker in self._encode_workers]

        self._upload_workers = [
            _UploadWorker(
                input_queue=self._upload_queue,
                auth_header=self._auth_header,
                api_base_url=self._api_base_url,
                num_retries=self._num_retries,
            )
            for _ in range(self._upload_pool_size)
        ]
        self._upload_futures = [self._upload_pool.schedule(worker.run) for worker in self._upload_workers]

        self._started = True

    def _teardown(self) -> None:
        if not self._started:
            return

        if self._manager:
            self._manager.shutdown()
            self._manager = None

        if self._encode_pool:
            self._encode_pool.stop()
            self._encode_pool.join()
            self._encode_pool = None

        if self._upload_pool:
            self._upload_pool.stop()
            self._upload_pool.join()
            self._upload_pool = None

        self._started = False

    def stop(self) -> None:
        """Gracefully signal stops to background processes and shutdown handler."""
        if not self._started:
            logger.warning("Import handler not started-- not stopping.")
            return

        # Should not be None if the handler has been started-- just for type checking
        assert self._encode_queue is not None
        logger.info("Scheduling stop requests for encode workers")
        for _ in range(self._encode_pool_size):
            self._encode_queue.put(None)

        # Should not be None if handler has been started-- just for type checking
        assert self._encode_pool is not None
        logger.info("Waiting gracefully for encode workers to shutdown")
        self._encode_pool.close()
        self._encode_pool.join()

        # Should not be None if the handler has been started-- just for type checking
        assert self._upload_queue is not None
        logger.info("Scheduling stop requests for upload workers")
        for _ in range(self._upload_pool_size):
            self._upload_queue.put(None)

        # Should not be None if handler has been started-- just for type checking
        assert self._upload_pool is not None
        logger.info("Waiting gracefully for upload workers to shutdown")
        self._upload_pool.close()
        self._upload_pool.join()

        logger.info("Fully tearing down import handler")
        self._teardown()

    def terminate(self) -> None:
        """Immediately terminate background processes and shutdown handler."""
        if not self._started:
            logger.warning("Import handler not started-- not terminating.")
            return

        logger.info("Forcefully tearing down import handler")
        self._teardown()
