from __future__ import annotations

from enum import IntEnum

from nominal_api import scout_api

from nominal.protos.event.v2 import event_pb2


class Priority(IntEnum):
    """Check priority, P0 most severe. P0 is 0, so check against None rather than truthiness."""

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

    @classmethod
    def _from_conjure(cls, priority: scout_api.Priority) -> Priority:
        match priority.name:
            case "P0":
                return cls.P0
            case "P1":
                return cls.P1
            case "P2":
                return cls.P2
            case "P3":
                return cls.P3
            case "P4":
                return cls.P4
            case _:
                raise ValueError(f"unknown priority '{priority}', expected one of {list(cls)}")

    def _to_conjure(self) -> scout_api.Priority:
        match self:
            case Priority.P0:
                return scout_api.Priority.P0
            case Priority.P1:
                return scout_api.Priority.P1
            case Priority.P2:
                return scout_api.Priority.P2
            case Priority.P3:
                return scout_api.Priority.P3
            case Priority.P4:
                return scout_api.Priority.P4
            case _:
                raise ValueError(f"unknown priority '{self}', expected one of {list(Priority)}")


def _conjure_priority_to_priority(priority: scout_api.Priority) -> Priority:
    return Priority._from_conjure(priority)
