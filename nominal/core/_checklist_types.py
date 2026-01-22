from __future__ import annotations

from enum import IntEnum

from nominal_api import scout_api


class Priority(IntEnum):
    P0 = 0
    P1 = 1
    P2 = 2
    P3 = 3
    P4 = 4

    @classmethod
    def _from_conjure(cls, priority: scout_api.Priority) -> Priority:
        if priority.name == "P0":
            return cls.P0
        elif priority.name == "P1":
            return cls.P1
        elif priority.name == "P2":
            return cls.P2
        elif priority.name == "P3":
            return cls.P3
        elif priority.name == "P4":
            return cls.P4
        else:
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
