from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from nominal_api import scout_catalog
from typing_extensions import Self

from nominal.core.bounds import Bounds

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetFile:
    id: str
    dataset_rid: str
    name: str
    bounds: Bounds | None
    uploaded_at: datetime
    ingested_at: datetime | None


    @classmethod
    def _from_conjure(cls, dataset_file: scout_catalog.DatasetFile) -> Self:
        return cls(
            id = dataset_file.id,
            dataset_rid = dataset_file.dataset_rid,
            name=dataset_file.name,
            bounds=Bounds._from_conjure(dataset_file.bounds),
            uploaded_at=datetime.fromisoformat(dataset_file.uploaded_at),
            ingested_at=None if dataset_file.ingested_at is None else datetime.fromisoformat(dataset_file.ingested_at),
        )
