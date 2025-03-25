from __future__ import annotations

from threading import Thread
from typing import BinaryIO

import polars as pl
from nominal.io import ts
from nominal.io._utils import reader_writer
from nominal.io.core.client import NominalClient
from nominal.io.core.dataset import Dataset
from nominal.io.core.filetype import FileTypes


def upload_polars(
    client: NominalClient,
    df: pl.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    channel_name_delimiter: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a polars.DataFrame.

    If `wait_until_complete=True` (the default), this function waits until the dataset has completed ingestion before
        returning. If you are uploading many datasets, set `wait_until_complete=False` instead and call
        `wait_until_ingestions_complete()` after uploading all datasets to allow for parallel ingestion.
    """

    def write_and_close(df: pl.DataFrame, w: BinaryIO) -> None:
        df.write_csv(w)
        w.close()

    with reader_writer() as (reader, writer):
        # write the dataframe to CSV in another thread
        t = Thread(target=write_and_close, args=(df, writer))
        t.start()
        dataset = client.create_dataset_from_io(
            reader,
            name,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=FileTypes.CSV,
            description=description,
            prefix_tree_delimiter=channel_name_delimiter,
        )
        t.join()
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset
