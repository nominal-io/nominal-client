from __future__ import annotations

import queue

import pandas as pd


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

    There is additionally an internal queue between encoder workers and publisher workers,
    though, this is not directly exposed to users and should be treated as an implementation detail.
    """

    def start(self) -> None:
        """Start background processes and prepare the handler for import."""

    def stop(self) -> None:
        """Gracefully signal stops to background processes and shutdown handler."""

    def terminate(self) -> None:
        """Immediately terminate background processes and shutdown handler."""

    @property
    def ingest_queue(self) -> queue.Queue[pd.DataFrame]:
        """"""

    def ingest(self, data: pd.DataFrame) -> None:
        """"""
