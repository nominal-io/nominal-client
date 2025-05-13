from __future__ import annotations

import concurrent.futures
import logging
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Callable, Iterable, Mapping, Protocol, Type
from types import TracebackType

import pandas as pd
from typing_extensions import Self

from nominal.core._batch_processor_proto import SerializedBatchV2
from nominal.core._clientsbunch import HasScoutParams, ProtoWriteService
from nominal.core._queueing import BatchV2
from nominal.core.write_stream_base import WriteStreamBase
from nominal.experimental.stream_v2._serializer import BatchSerializer
from nominal.experimental.stream_v2._utils import prepare_df_for_upload, split_into_chunks

logger = logging.getLogger(__name__)

# Thread-local storage for requests sessions
thread_local = threading.local()

def get_thread_local_session() -> requests.Session:
    """Get thread-local session to ensure thread safety"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

@dataclass(frozen=True)
class WriteStreamV3:
    _write_pool: ThreadPoolExecutor
    _serializer: BatchSerializer
    _clients: _Clients
    _nominal_data_source_rid: str

    class _Clients(HasScoutParams, Protocol):
        @property
        def proto_write(self) -> ProtoWriteService: ...

    @classmethod
    def create(
        cls,
        clients: _Clients,
        serializer: BatchSerializer,
        nominal_data_source_rid: str,
        max_write_thread_workers: int | None = None,
    ) -> Self:
        write_pool = ThreadPoolExecutor(max_workers=max_write_thread_workers)

        return cls(
            _write_pool=write_pool,
            _serializer=serializer,
            _clients=clients,
            _nominal_data_source_rid=nominal_data_source_rid,
        )

    def enqueue_dataframes(
        self,
        timestamp_column: str,
        dataframes: Iterable[pd.DataFrame],
        points_per_batch: int = 50000,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        prepared_dataframes = []
        for df in dataframes:
            try:
                channel_data, _discarded_total = prepare_df_for_upload(df, timestamp_column)
                if channel_data:
                    prepared_dataframes.append((channel_data))
            except Exception as e:
                logger.error(f"Dataframe preparation error: {e}", exc_info=True)
                continue

        all_chunks = []
        for channel_data in prepared_dataframes:
            try:
                chunks = split_into_chunks(channel_data, points_per_batch, tags)
                all_chunks.extend(list(chunks))
            except Exception as e:
                logger.error(f"Chunking error: {e}", exc_info=True)
                continue

        total_points = 0
        for chunk in all_chunks:
            for batch_item in chunk:
                try:
                    batch = BatchV2(
                        channel_name=batch_item[0],
                        seconds=batch_item[1],
                        nanos=batch_item[2],
                        values=batch_item[3],
                        tags=batch_item[4],
                    )
                    total_points += len(batch_item[3])
                    callback = partial(
                        _write_serialized_batch_v2, self._write_pool, self._clients, self._nominal_data_source_rid
                    )
                    future = self._serializer.serialize_v2(batch)
                    future.add_done_callback(callback)
                except Exception as e:
                    logger.error(f"Batch processing error: {e}", exc_info=True)

        logger.info(f"Created {len(all_chunks)} chunks from {len(prepared_dataframes)} dataframes, sending {total_points} data points")
    def __enter__(self) -> WriteStreamV3:
        """Create the stream as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close(wait=exc_type is None)

    def close(self, wait: bool = True) -> None:
        """Close the write stream and clean up resources.
        
        Args:
            wait: If True, wait for all pending writes to complete before returning
        """
        logger.debug("Closing write stream (wait=%s)", wait)
        self._serializer.close(cancel_futures=not wait)
        self._write_pool.shutdown(wait=wait, cancel_futures=not wait)


def direct_write_nominal_batches(
    auth_header: str,
    data_source_rid: str,
    data: bytes,
    base_uri: str
) -> None:
    """
    Send data directly using thread-local sessions instead of using the connection pool
    from the conjure_python_client's RequestsClient.
    """
    try:
        session = get_thread_local_session()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-protobuf",
            "Authorization": auth_header,
        }
        url = f"{base_uri}/storage/writer/v1/nominal/{data_source_rid}"
        response = session.post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error in direct_write_nominal_batches: {e}", exc_info=True)
        raise


def _write_serialized_batch_v2(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV3._Clients,
    nominal_data_source_rid: str,
    future: concurrent.futures.Future[SerializedBatchV2],
) -> None:
    # Check if the future has completed with an exception
    if future.exception() is not None:
        logger.error(f"Error in serialization: {future.exception()}", exc_info=future.exception())
        return
        
    try:
        serialized = future.result()
        # Use direct write method instead of ProtoWriteService
        # Extract base URI from the client
        base_uri = clients.proto_write._uri
        _write_future = pool.submit(
            direct_write_nominal_batches,
            clients.auth_header,
            nominal_data_source_rid,
            serialized.data,
            base_uri
        )
    except Exception as e:
        logger.error(f"Error in _write_serialized_batch_v2: {e}", exc_info=True)
        raise
