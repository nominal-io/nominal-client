from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from nominal_api import scout_catalog
from typing_extensions import Self

from nominal.core.bounds import Bounds
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class DatasetFile:
    id: str
    dataset_rid: str
    name: str
    bounds: Bounds | None
    uploaded_at: IntegralNanosecondsUTC
    ingested_at: IntegralNanosecondsUTC | None

    @classmethod
    def _from_conjure(cls, dataset_file: scout_catalog.DatasetFile) -> Self:
        upload_time = _SecondsNanos.from_datetime(datetime.fromisoformat(dataset_file.uploaded_at)).to_nanoseconds()
        ingest_time = (
            None
            if dataset_file.ingested_at is None
            else _SecondsNanos.from_datetime(datetime.fromisoformat(dataset_file.ingested_at)).to_nanoseconds()
        )
        return cls(
            id=dataset_file.id,
            dataset_rid=dataset_file.dataset_rid,
            name=dataset_file.name,
            bounds=None if dataset_file.bounds is None else Bounds._from_conjure(dataset_file.bounds),
            uploaded_at=upload_time,
            ingested_at=ingest_time,
        )
