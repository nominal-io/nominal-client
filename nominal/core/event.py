from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Iterable, Mapping, Protocol, Sequence

from nominal_api import event
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import rid_from_instance_or_string, update_dataclass
from nominal.core.asset import Asset
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC, _SecondsNanos, _to_api_duration


@dataclass(frozen=True)
class Event:
    uuid: str
    asset_rids: Sequence[str]
    name: str
    start: IntegralNanosecondsUTC
    duration: IntegralNanosecondsDuration
    properties: Mapping[str, str]
    type: EventType

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def event(self) -> event.EventService: ...

    def update(
        self,
        *,
        name: str | None = None,
        assets: Iterable[Asset | str] | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        duration: timedelta | IntegralNanosecondsDuration | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Iterable[str] | None = None,
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
            event = event.update(labels=new_labels)
        """
        request = event.UpdateEvent(
            uuid=self.uuid,
            asset_rids=None if assets is None else [rid_from_instance_or_string(asset) for asset in assets],
            duration=None if duration is None else _to_api_duration(duration),
            labels=None if labels is None else list(labels),
            name=name,
            properties=None if properties is None else dict(properties),
            timestamp=None if start is None else _SecondsNanos.from_flexible(start).to_api(),
            type=None if type is None else type._to_api_event_type(),
        )
        response = self._clients.event.update_event(self._clients.auth_header, request)
        e = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, e, fields=self.__dataclass_fields__)
        return self

    @classmethod
    def _from_conjure(cls, clients: _Clients, event: event.Event) -> Self:
        if event.duration.picos:
            warnings.warn(
                f"event '{event.name}' ({event.uuid}) has a duration specified in picoseconds: "
                "currently, any sub-nanosecond precision will be truncated in nominal-client",
                UserWarning,
                stacklevel=2,
            )
        return cls(
            uuid=event.uuid,
            asset_rids=tuple(event.asset_rids),
            name=event.name,
            start=_SecondsNanos.from_api(event.timestamp).to_nanoseconds(),
            duration=event.duration.seconds * 1_000_000_000 + event.timestamp.nanos,
            type=EventType.from_api_event_type(event.type),
            properties=event.properties,
            _clients=clients,
        )


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
