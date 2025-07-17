from __future__ import annotations

import gzip
import json
import logging
import multiprocessing
import time
from threading import Thread
from typing import Mapping, Self

import pandas as pd
import requests

from nominal._utils import SharedCounter
from nominal._utils.threading_tools import StoppableQueue
from nominal.core.datasource import DataSource
from nominal.experimental.pandas_data_handler._utils import (
    BiWorker,
    Worker,
    extract_batches_from_dataframe,
    to_api_dtype,
    to_api_json_timestamp,
)

logger = logging.getLogger(__name__)


DEFAULT_NUM_RETRIES = 3
DEFAULT_COMPRESSION_LEVEL = 3
DEFAULT_BATCH_SIZE = 100_000
DEFAULT_NUM_ENCODE_WORKERS = 4
DEFAULT_NUM_UPLOAD_WORKERS = 32
DEFAULT_ENCODE_QUEUE_SIZE = 128
DEFAULT_UPLOAD_QUEUE_SIZE = 2048


class _EncodeWorker(BiWorker[pd.DataFrame, bytes]):
    """Worker process to encode dataframes into batched streaming requests"""

    def __init__(
        self,
        input_queue: StoppableQueue[pd.DataFrame],
        output_queue: StoppableQueue[bytes],
        datasource_rid: str,
        timestamp_column: str,
        tags: Mapping[str, str] | None,
        compression_level: int,
        batch_size: int,
        points_encoded: SharedCounter,
        log_stats: bool,
    ):
        super().__init__(input_queue=input_queue, output_queue=output_queue)

        self._datasource_rid = datasource_rid
        self._tags = tags or {}
        self._timestamp_column = timestamp_column

        self._compression_level = compression_level
        self._batch_size = batch_size

        self._points_encoded = points_encoded
        self._log_stats = log_stats

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
        if self._log_stats:
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

        dtype_str = to_api_dtype(df_slice[value_col].dtype)
        batch = {
            "channel": value_col,
            "timestamps": df_slice[self._timestamp_column].apply(to_api_json_timestamp).to_list(),
            "tags": self._tags,
            "values": {"type": dtype_str, dtype_str: df_slice[value_col].to_list()},
        }
        request = {
            "batches": [batch],
            "dataSourceRid": self._datasource_rid,
        }

        encoded = json.dumps(request).encode("utf-8")

        end = time.monotonic()
        diff = end - start
        self._task_encode_time += diff

        num_values = len(batch["timestamps"])
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
        for column_name, df_slice in extract_batches_from_dataframe(
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


def run_encoder(
    input_queue: StoppableQueue[pd.DataFrame],
    output_queue: StoppableQueue[bytes],
    datasource_rid: str,
    timestamp_column: str,
    tags: Mapping[str, str] | None,
    compression_level: int,
    batch_size: int,
    points_encoded: SharedCounter,
    log_stats: bool = False,
) -> None:
    encoder = _EncodeWorker(
        input_queue=input_queue,
        output_queue=output_queue,
        datasource_rid=datasource_rid,
        timestamp_column=timestamp_column,
        tags=tags,
        compression_level=compression_level,
        batch_size=batch_size,
        points_encoded=points_encoded,
        log_stats=log_stats,
    )
    encoder.run()


class _UploadWorker(Worker[bytes]):
    """Worker process to upload encoded requests of streaming data to Nominal"""

    def __init__(
        self,
        input_queue: StoppableQueue[bytes],
        auth_header: str,
        api_base_url: str,
        num_retries: int,
        bytes_uploaded: SharedCounter,
    ):
        super().__init__(input_queue=input_queue)

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


def run_uploader(
    input_queue: StoppableQueue[bytes],
    auth_header: str,
    api_base_url: str,
    num_retries: int,
    bytes_uploaded: SharedCounter,
) -> None:
    uploader = _UploadWorker(
        input_queue=input_queue,
        auth_header=auth_header,
        api_base_url=api_base_url,
        num_retries=num_retries,
        bytes_uploaded=bytes_uploaded,
    )
    uploader.run()


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

        self.encode_queue: StoppableQueue[pd.DataFrame] = StoppableQueue.from_size(queue_size=encode_queue_size)
        self._num_encode_workers = num_encode_workers
        self._encode_workers: list[multiprocessing.Process] = []

        self.upload_queue: StoppableQueue[bytes] = StoppableQueue.from_size(queue_size=upload_queue_size)
        self._num_upload_workers = num_upload_workers
        self._upload_workers: list[Thread] = []

        self._points_encoded = SharedCounter.from_value(0.0)
        self._bytes_uploaded = SharedCounter.from_value(0.0)

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
    def points_encoded(self) -> float:
        return self._points_encoded.value()

    @property
    def bytes_uploaded(self) -> float:
        return self._bytes_uploaded.value()

    def ingest(self, data: pd.DataFrame) -> None:
        """Ingest data to Nominal."""
        self.encode_queue.put(data)

    def start(self) -> None:
        """Start background processes and prepare the handler for import."""
        if self._started:
            logger.warning("Import handler already started-- not starting.")
            return

        for _ in range(self._num_encode_workers):
            proc = multiprocessing.Process(
                target=run_encoder,
                kwargs=dict(
                    input_queue=self.encode_queue,
                    output_queue=self.upload_queue,
                    datasource_rid=self._datasource_rid,
                    timestamp_column=self._timestamp_column,
                    tags=self._tags,
                    compression_level=self._compression_level,
                    batch_size=self._batch_size,
                    points_encoded=self._points_encoded,
                ),
                daemon=True,
            )
            proc.start()
            self._encode_workers.append(proc)

        for _ in range(self._num_upload_workers):
            thread = Thread(
                target=run_uploader,
                kwargs=dict(
                    input_queue=self.upload_queue,
                    auth_header=self._auth_header,
                    api_base_url=self._api_base_url,
                    num_retries=self._num_retries,
                    bytes_uploaded=self._bytes_uploaded,
                ),
                daemon=True,
            )
            thread.start()
            self._upload_workers.append(thread)

        self._started = True

    def stop(self) -> None:
        """Gracefully stops background processes.

        Call teardown() after to free resources
        """
        if not self._started:
            logger.warning("Import handler not started-- not stopping.")
            return

        logger.info("Scheduling stop requests for encode workers")
        self.encode_queue.interrupt(num_stops=self._num_encode_workers)
        logger.info("Awaiting encode tasks to finish")
        self.encode_queue.wait()
        for proc in self._encode_workers:
            proc.join()

        logger.info("Scheduling stop requests for upload workers")
        self.upload_queue.interrupt(num_stops=self._num_upload_workers)
        logger.info("Awaiting upload tasks to finish")
        self.upload_queue.wait()
        for thread in self._upload_workers:
            thread.join()

    def teardown(self) -> None:
        """Immediately terminate background processes and shutdown handler."""
        if not self._started:
            logger.warning("Import handler not started-- not tearing down!")
            return

        self.encode_queue.stop()
        self.upload_queue.stop()

        for proc in self._encode_workers:
            proc.terminate()

        self._started = False
