from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Mapping, Protocol, Sequence

from nominal_api import (
    event,
)
from typing_extensions import Self

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid, update_dataclass
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment
from nominal.core.checklist import _to_api_duration
from nominal.core.connection import Connection
from nominal.core.dataset import Dataset
from nominal.core.log import LogSet
from nominal.core.video import Video
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class Event(HasRid):
    uuid: str
    asset_rids: list[str]
    name: str
    start: IntegralNanosecondsUTC
    duration: timedelta
    type: EventType

    _clients: _Clients = field(repr=False)

    class _Clients(
        Attachment._Clients,
        Asset._Clients,
        Connection._Clients,
        Dataset._Clients,
        LogSet._Clients,
        Video._Clients,
        HasAuthHeader,
        Protocol,
    ):
        @property
        def event(self) -> event.EventService: ...

    def update(
        self,
        *,
        name: str | None = None,
        asset_rids: list[str] | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        duration: timedelta | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        type: EventType | None,
    ) -> Self:
        """Replace event metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in event.labels:
                new_labels.append(old_label)
            run = event.update(labels=new_labels)
        """
        request = event.UpdateEvent(
            uuid=self.uuid,
            asset_rids=asset_rids,
            timestamp=None if start is None else _SecondsNanos.from_flexible(start).to_api(),
            duration=None if duration is None else _to_api_duration(duration),
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            title=name,
            assets=[],
            type=None if type is None else type._to_api_event_type(),
        )
        response = self._clients.event.update_event(self._clients.auth_header, request, self.rid)
        e = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, e, fields=self.__dataclass_fields__)
        return self

    @classmethod
    def _from_conjure(cls, clients: _Clients, event: event.Event) -> Self:
        return cls(
            uuid=event.uuid,
            asset_rids=event.asset_rids,
            name=event.name,
            start=_SecondsNanos.from_api(event.timestamp).to_nanoseconds(),
            duration=timedelta(seconds=event.duration.seconds, microseconds=int(event.timestamp.nanos / 1000)),
            type=EventType.from_api_event_type(event.type),
            _clients=clients,
        )


class EventType(Enum):
    INFO = "INFO"
    FLAG = "FLAG"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_api_event_type(cls, event: event.EventType) -> Self:
        match event.name:
            case "INFO":
                return cls.INFO
            case "FLAG":
                return cls.FLAG
            case "ERROR":
                return cls.ERROR
            case "SUCCESS":
                return cls.SUCCESS
            case _:
                return cls.UNKNOWN

    def _to_api_event_type(self) -> event.EventType:
        match self.name:
            case "INFO":
                return event.EventType.INFO
            case "FLAG":
                return event.EventType.FLAG
            case "ERROR":
                return event.EventType.ERROR
            case "SUCCESS":
                return event.EventType.SUCCESS
            case _:
                return event.EventType.UNKNOWN
