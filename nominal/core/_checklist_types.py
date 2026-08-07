from __future__ import annotations

from enum import IntEnum

from nominal.protos.types import common_pb2


class Priority(IntEnum):
    P0 = 0
    P1 = 1
    P2 = 2
    P3 = 3
    P4 = 4

    @classmethod
    def _from_proto(cls, priority: common_pb2.Priority.ValueType) -> Priority | None:
        """None when the priority is unspecified."""
        match priority:
            case common_pb2.P0:
                return cls.P0
            case common_pb2.P1:
                return cls.P1
            case common_pb2.P2:
                return cls.P2
            case common_pb2.P3:
                return cls.P3
            case common_pb2.P4:
                return cls.P4
            case common_pb2.PRIORITY_UNSPECIFIED:
                return None
            case _:
                raise ValueError(
                    f"unknown priority '{common_pb2.Priority.Name(priority)}', expected one of {[p.name for p in cls]}"
                )
