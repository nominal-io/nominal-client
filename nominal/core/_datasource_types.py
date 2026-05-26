from __future__ import annotations

from enum import Enum

from nominal_api import api


class DataSourceType(Enum):
    """Categorization for Nominal Core datasources"""

    DATASET = "DATASET"
    CONNECTION = "CONNECTION"
    VIDEO = "VIDEO"
    SPATIAL = "SPATIAL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _from_conjure(cls, datasource_type: api.DataSourceType) -> DataSourceType:
        match datasource_type.value:
            case "DATASET":
                return cls.DATASET
            case "CONNECTION":
                return cls.CONNECTION
            case "VIDEO":
                return cls.VIDEO
            case "SPATIAL":
                return cls.SPATIAL
            case "UNKNOWN":
                return cls.UNKNOWN
            case _:
                raise ValueError(f"Unknown datasource type: {datasource_type.name}")

    def _to_conjure(self) -> api.DataSourceType:
        match self.value:
            case "DATASET":
                return api.DataSourceType.DATASET
            case "CONNECTION":
                return api.DataSourceType.CONNECTION
            case "VIDEO":
                return api.DataSourceType.VIDEO
            case "SPATIAL":
                return api.DataSourceType.SPATIAL
            case "UNKNOWN":
                return api.DataSourceType.UNKNOWN
