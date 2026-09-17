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
        """The SDK priority for a proto priority, or None if it does not name one.

        Proto enums are open, so a level added after this client was built arrives as an integer with no
        name here. That degrades to None rather than raising, which is what the conjure transport did too:
        its decoder collapsed unrecognized values into `Priority.UNKNOWN`, which callers read as absence.

        The unordered enum wrappers instead degrade to an `UNSPECIFIED` member. This scale is ordered, so
        such a member would take a position on it -- below P4 or above P0 -- and None takes none.
        """
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
            case _:
                return None
