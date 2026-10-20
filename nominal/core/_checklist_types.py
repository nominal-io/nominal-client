from __future__ import annotations

from enum import IntEnum
from typing import assert_never

from nominal.protos.event.v2 import event_pb2


class Priority(IntEnum):
    """Check or event disposition priority, P0 most severe. P0 is 0, so check against None rather than truthiness."""

    P0 = 0
    P1 = 1
    P2 = 2
    P3 = 3
    P4 = 4

    @classmethod
    def _from_proto(cls, priority: event_pb2.Priority.ValueType) -> Priority | None:
        match priority:
            case event_pb2.P0:
                return cls.P0
            case event_pb2.P1:
                return cls.P1
            case event_pb2.P2:
                return cls.P2
            case event_pb2.P3:
                return cls.P3
            case event_pb2.P4:
                return cls.P4
            case _:
                # Open proto enum: a value this client does not know reads as None rather than raising.
                return None

    def _to_proto(self) -> event_pb2.Priority.ValueType:
        match self:
            case Priority.P0:
                return event_pb2.P0
            case Priority.P1:
                return event_pb2.P1
            case Priority.P2:
                return event_pb2.P2
            case Priority.P3:
                return event_pb2.P3
            case Priority.P4:
                return event_pb2.P4
            case _:
                assert_never(self)
