from __future__ import annotations

import itertools
from typing import Sequence, cast

from nominal_api import storage_writer_api

from nominal.core._utils import _to_api_batch_key
from nominal.core.stream import BatchItem
from nominal.ts import _SecondsNanos


def make_points(api_batch: Sequence[BatchItem]) -> storage_writer_api.Points:
    if isinstance(api_batch[0].value, str):
        return storage_writer_api.Points(
            string=[
                storage_writer_api.StringPoint(
                    timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                    value=cast(str, item.value),
                )
                for item in api_batch
            ]
        )
    if isinstance(api_batch[0].value, float):
        return storage_writer_api.Points(
            double=[
                storage_writer_api.DoublePoint(
                    timestamp=_SecondsNanos.from_flexible(item.timestamp).to_api(),
                    value=cast(float, item.value),
                )
                for item in api_batch
            ]
        )
    raise ValueError("only float and string are supported types for value")


def process_batch_legacy(
    batch: Sequence[BatchItem],
    nominal_data_source_rid: str,
    auth_header: str,
    storage_writer: storage_writer_api.NominalChannelWriterService,
) -> None:
    api_batched = itertools.groupby(sorted(batch, key=_to_api_batch_key), key=_to_api_batch_key)

    api_batches = [list(api_batch) for _, api_batch in api_batched]
    request = storage_writer_api.WriteBatchesRequest(
        data_source_rid=nominal_data_source_rid,
        batches=[
            storage_writer_api.RecordsBatch(
                channel=api_batch[0].channel_name,
                points=make_points(api_batch),
                tags=api_batch[0].tags or {},
            )
            for api_batch in api_batches
        ],
    )
    storage_writer.write_batches(
        auth_header,
        request,
    )
