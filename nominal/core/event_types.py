from __future__ import annotations

from enum import Enum
from typing import Iterable

from nominal_api import event


class EventType(Enum):
    INFO = "INFO"
    FLAG = "FLAG"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_api_event_type(cls, event: event.EventType) -> EventType:
        if event.name == "INFO":
            return cls.INFO
        elif event.name == "FLAG":
            return cls.FLAG
        elif event.name == "ERROR":
            return cls.ERROR
        elif event.name == "SUCCESS":
            return cls.SUCCESS
        else:
            return cls.UNKNOWN

    def _to_api_event_type(self) -> event.EventType:
        if self.name == "INFO":
            return event.EventType.INFO
        elif self.name == "FLAG":
            return event.EventType.FLAG
        elif self.name == "ERROR":
            return event.EventType.ERROR
        elif self.name == "SUCCESS":
            return event.EventType.SUCCESS
        else:
            return event.EventType.UNKNOWN


class EventCreationType(Enum):
    MANUAL = "MANUAL"
    AUTOMATED = "BY_EXTERNAL_RESOURCE"


class SearchEventOriginType(Enum):
    WORKBOOK = ("WORKBOOK", EventCreationType.MANUAL)
    TEMPLATE = ("TEMPLATE", EventCreationType.MANUAL)
    API = ("API", EventCreationType.MANUAL)
    DATA_REVIEW = ("DATA_REVIEW", EventCreationType.AUTOMATED)
    PROCEDURE = ("PROCEDURE", EventCreationType.AUTOMATED)
    STREAMING_CHECKLIST = ("STREAMING_CHECKLIST", EventCreationType.AUTOMATED)

    creation_type: EventCreationType

    def __new__(cls, value: str, creation_type: EventCreationType) -> SearchEventOriginType:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.creation_type = creation_type
        return obj

    @classmethod
    def from_api_origin_type(cls, event: event.SearchEventOriginType) -> SearchEventOriginType:
        if event.name == "WORKBOOK":
            return cls.WORKBOOK
        elif event.name == "TEMPLATE":
            return cls.TEMPLATE
        elif event.name == "API":
            return cls.API
        elif event.name == "DATA_REVIEW":
            return cls.DATA_REVIEW
        elif event.name == "PROCEDURE":
            return cls.PROCEDURE
        elif event.name == "STREAMING_CHECKLIST":
            return cls.STREAMING_CHECKLIST
        else:
            raise ValueError(f"Unexpected Event Origin {event.name}")

    def _to_api_search_event_origin_type(self) -> event.SearchEventOriginType:
        if self.name == "WORKBOOK":
            return event.SearchEventOriginType.WORKBOOK
        elif self.name == "TEMPLATE":
            return event.SearchEventOriginType.TEMPLATE
        elif self.name == "API":
            return event.SearchEventOriginType.API
        elif self.name == "DATA_REVIEW":
            return event.SearchEventOriginType.DATA_REVIEW
        elif self.name == "PROCEDURE":
            return event.SearchEventOriginType.PROCEDURE
        elif self.name == "STREAMING_CHECKLIST":
            return event.SearchEventOriginType.STREAMING_CHECKLIST
        else:
            raise ValueError(f"Unexpected Event Origin {self.name}")

    @classmethod
    def get_manual_origin_types(cls) -> Iterable[SearchEventOriginType]:
        """Return all origin types that are manually created."""
        return [origin_type for origin_type in cls if origin_type.creation_type == EventCreationType.MANUAL]
