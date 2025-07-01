from __future__ import annotations

import abc
import concurrent.futures
import datetime
import logging
import multiprocessing
from multiprocessing.managers import ValueProxy
from typing import BinaryIO, Generic, Iterable, Mapping, Sequence, TypeVar, cast

from nominal._utils import LogTiming
from nominal.core.channel import Channel
from nominal.core.read_stream_base import ExportJob, ReadStreamBase, TimeRange
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC, _AnyNativeTimestampType

logger = logging.getLogger(__name__)

ExportType = TypeVar("ExportType")


class ExportStream(abc.ABC, ReadStreamBase, Generic[ExportType]):
    """Exports in-memory chunks of data from Nominal."""

    def export(
        self,
        channels: Sequence[Channel],
        start: IntegralNanosecondsUTC,
        end: IntegralNanosecondsUTC,
        tags: Mapping[str, str] | None = None,
        batch_duration: datetime.timedelta | None = None,
        timestamp_type: _AnyNativeTimestampType = "epoch_seconds",
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
    ) -> Iterable[ExportType]:
        """Given a list of channels, a time range, and other assorted configuration details, export batches of
        data in-memory.

        Args:
            channels: Channels to export data for
            start: Starting timestamp of data to export in nanoseconds
            end: Ending timestamp of data to export in nanoseconds
            tags: Key-value pairs to filter data being exported with
            batch_duration: If provided, manually set the duration of each batch of data to export in-memory
                NOTE: if not provided, this is computed based on sampled data rates for each
                      channel and the configured request / batch point maximums.
            timestamp_type: Timestamp format to export data with
            buckets: Number of buckets to decimate data into within each exported batch of data
                NOTE: may not be used alongside `resolution`
            resolution: Resolution, in nanoseconds, between decimated points.
                NOTE: may not be used alongside `buckets`

        Yields:
            Yields batches of data in-memory
        """
        tags = tags or {}

        ###############################
        # Step 0: Check preconditions #
        ###############################

        # Ensure user has selected channels to export
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return

        # Ensure user has not selected incompatible decimation options
        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")

        #######################################
        # Step 1: Determine download schedule #
        #######################################

        with LogTiming("Built export jobs"):
            download_batches = self._build_download_queue(
                channels,
                time_range=TimeRange(start, end),
                timestamp_type=timestamp_type,
                tags=tags,
                batch_duration=batch_duration,
                buckets=buckets,
                resolution=resolution,
            )

        ##############################
        # Step 2: Kick off downloads #
        ##############################

        with (
            LogTiming(f"Downloaded {len(download_batches)} batches"),
            concurrent.futures.ProcessPoolExecutor(max_workers=self._num_workers) as pool,
            multiprocessing.Manager() as manager,
        ):
            for batch_idx, batch in enumerate(download_batches.items()):
                slice, tasks = batch

                logger.info(
                    "Starting to download data for slice %s (batch %d/%d)", slice, batch_idx + 1, len(download_batches)
                )
                with LogTiming(f"Downloaded data for slice {slice} ({batch_idx + 1} / {len(download_batches)})"):
                    futures = [pool.submit(self._extract_batch, manager.Value("o", task)) for task in tasks]
                    results = []
                    for idx, future in enumerate(concurrent.futures.as_completed(futures)):
                        ex = future.exception()
                        if ex is not None:
                            logger.error("Failed to extract batch", exc_info=ex)
                            continue

                        res = future.result()
                        logger.info("Finished extracting batch %d/%d", idx + 1, len(tasks))
                        results.append(res)

                with LogTiming(f"Merged {len(results)} exports"):
                    yield self._merge_exports(results)

    @classmethod
    def _extract_batch(cls, task_proxy: ValueProxy[ExportJob]) -> ExportType:
        """Extract a single batch of data based on task configurations.

        NOTE: intended to be used as a task in a multiprocessing setup.
        """
        task = task_proxy.value
        if not task.channels:
            raise ValueError("No channels to extract!")

        dataexport = task.channels[0]._clients.dataexport
        auth_header = task.channels[0]._clients.auth_header

        resp = dataexport.export_channel_data(auth_header, task.export_request())
        return cls._stream_export(cast(BinaryIO, resp), task)

    @classmethod
    @abc.abstractmethod
    def _stream_export(cls, stream: BinaryIO, task: ExportJob) -> ExportType:
        """Given a binary gzipped stream of CSV data for an export batch, extract the in-memory representation."""

    @classmethod
    @abc.abstractmethod
    def _merge_exports(cls, exports: Sequence[ExportType]) -> ExportType:
        """Given a sequence of in-memory representations of data in a batch, combine into a single representation."""
