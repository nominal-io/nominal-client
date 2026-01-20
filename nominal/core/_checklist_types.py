from __future__ import annotations

from typing import Literal

from nominal_api import scout_api

Priority = Literal[0, 1, 2, 3, 4]

_priority_to_conjure_map: dict[Priority, scout_api.Priority] = {
    0: scout_api.Priority.P0,
    1: scout_api.Priority.P1,
    2: scout_api.Priority.P2,
    3: scout_api.Priority.P3,
    4: scout_api.Priority.P4,
}


def _conjure_priority_to_priority(priority: scout_api.Priority) -> Priority:
    inverted_map = {v: k for k, v in _priority_to_conjure_map.items()}
    if priority in inverted_map:
        return inverted_map[priority]
    raise ValueError(f"unknown priority '{priority}', expected one of {_priority_to_conjure_map.values()}")
