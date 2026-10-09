from __future__ import annotations

from enum import Enum
from typing import Iterable, NamedTuple

from typing_extensions import assert_never

from nominal.protos.event.v2 import event_pb2


class EventType(Enum):
    """Categorization for Nominal Core events."""

    INFO = "INFO"
    """Informational event."""
    FLAG = "FLAG"
    """Event indicating attention is required."""
    ERROR = "ERROR"
    """Event indicating an error occurred."""
    SUCCESS = "SUCCESS"
    """Event indicating a successful outcome."""
    UNKNOWN = "UNKNOWN"
    """Unknown or unrecognized event type.

    Events with this type are invalid and will not be accepted by Nominal Core.
    """

    @classmethod
    def _from_proto(cls, event: event_pb2.EventType.ValueType) -> EventType:
        """Convert a proto event type to a `nominal-core` event type."""
        match event:
            case event_pb2.INFO:
                result = cls.INFO
            case event_pb2.FLAG:
                result = cls.FLAG
            case event_pb2.ERROR:
                result = cls.ERROR
            case event_pb2.SUCCESS:
                result = cls.SUCCESS
            case _:
                result = cls.UNKNOWN
        return result

    def _to_proto(self) -> event_pb2.EventType.ValueType:
        match self:
            case EventType.INFO:
                result = event_pb2.INFO
            case EventType.FLAG:
                result = event_pb2.FLAG
            case EventType.ERROR:
                result = event_pb2.ERROR
            case EventType.SUCCESS:
                result = event_pb2.SUCCESS
            case EventType.UNKNOWN:
                result = event_pb2.EVENT_TYPE_UNSPECIFIED
            case _:
                assert_never(self)
        return result


class EventCreationType(Enum):
    MANUAL = "MANUAL"
    BY_EXTERNAL_RESOURCE = "BY_EXTERNAL_RESOURCE"


class SearchEventOriginType(NamedTuple):
    name: str
    creation_type: EventCreationType

    def _to_proto(self) -> event_pb2.SearchEventOriginType.ValueType:
        """The proto value with this origin type's name.

        Raises:
            ValueError: If no proto value has this name.
        """
        return event_pb2.SearchEventOriginType.Value(self.name)

    @classmethod
    def _from_proto(cls, event: event_pb2.SearchEventOriginType.ValueType) -> SearchEventOriginType:
        """The origin type named by a proto value.

        Raises:
            ValueError: If the value is unspecified or names an origin type this client does not know.
        """
        match event:
            case event_pb2.WORKBOOK:
                return SearchEventOriginTypes.WORKBOOK
            case event_pb2.TEMPLATE:
                return SearchEventOriginTypes.TEMPLATE
            case event_pb2.API:
                return SearchEventOriginTypes.API
            case event_pb2.DATA_REVIEW:
                return SearchEventOriginTypes.DATA_REVIEW
            case event_pb2.PROCEDURE:
                return SearchEventOriginTypes.PROCEDURE
            case event_pb2.STREAMING_CHECKLIST:
                return SearchEventOriginTypes.STREAMING_CHECKLIST
            case _:
                raise ValueError(f"Unexpected Event Origin {event}")

    @classmethod
    def get_manual_origin_types(cls) -> Iterable[SearchEventOriginType]:
        """Return all origin types that are manually created."""
        return [
            origin_type
            for origin_type in SearchEventOriginTypes.__dict__.values()
            if isinstance(origin_type, SearchEventOriginType) and origin_type.creation_type == EventCreationType.MANUAL
        ]


class SearchEventOriginTypes:
    WORKBOOK = SearchEventOriginType("WORKBOOK", EventCreationType.MANUAL)
    TEMPLATE = SearchEventOriginType("TEMPLATE", EventCreationType.MANUAL)
    API = SearchEventOriginType("API", EventCreationType.MANUAL)
    DATA_REVIEW = SearchEventOriginType("DATA_REVIEW", EventCreationType.BY_EXTERNAL_RESOURCE)
    PROCEDURE = SearchEventOriginType("PROCEDURE", EventCreationType.BY_EXTERNAL_RESOURCE)
    STREAMING_CHECKLIST = SearchEventOriginType("STREAMING_CHECKLIST", EventCreationType.BY_EXTERNAL_RESOURCE)
