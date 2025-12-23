from __future__ import annotations

from enum import Enum
from typing import Iterable, NamedTuple

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
    BY_EXTERNAL_RESOURCE = "BY_EXTERNAL_RESOURCE"


class SearchEventOriginType(NamedTuple):
    name: str
    creation_type: EventCreationType

    @classmethod
    def from_api_origin_type(cls, event: event.SearchEventOriginType) -> SearchEventOriginType:
        if event.name == "WORKBOOK":
            return SearchEventOriginTypes.WORKBOOK
        elif event.name == "TEMPLATE":
            return SearchEventOriginTypes.TEMPLATE
        elif event.name == "API":
            return SearchEventOriginTypes.API
        elif event.name == "DATA_REVIEW":
            return SearchEventOriginTypes.DATA_REVIEW
        elif event.name == "PROCEDURE":
            return SearchEventOriginTypes.PROCEDURE
        elif event.name == "STREAMING_CHECKLIST":
            return SearchEventOriginTypes.STREAMING_CHECKLIST
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
