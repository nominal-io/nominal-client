from __future__ import annotations

from enum import Enum

from nominal.protos.types import types_pb2


class DataSourceType(Enum):
    """A datasource category used by workspace preferred refnames."""

    DATASET = "DATASET"
    CONNECTION = "CONNECTION"
    VIDEO = "VIDEO"
    SPATIAL = "SPATIAL"
    UNKNOWN = "UNKNOWN"
    """An unspecified or unrecognized datasource category."""

    @classmethod
    def _from_proto(cls, datasource_type: types_pb2.DataSourceType.ValueType) -> DataSourceType:
        match datasource_type:
            case types_pb2.DATASET:
                return cls.DATASET
            case types_pb2.CONNECTION:
                return cls.CONNECTION
            case types_pb2.VIDEO:
                return cls.VIDEO
            case types_pb2.SPATIAL:
                return cls.SPATIAL
            case _:
                return cls.UNKNOWN
