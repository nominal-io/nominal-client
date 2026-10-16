from __future__ import annotations

from dataclasses import KW_ONLY, dataclass
from datetime import datetime
from enum import Enum
from typing import Iterable, Mapping, Protocol, Self, Sequence

from nominal_api import scout_checklistexecution_api, scout_integrations_api

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.pagination_tools import (
    list_streaming_checklists_for_asset_paginated,
    list_streaming_checklists_paginated,
)
from nominal.core.integration import Integration
from nominal.core.workbook import Workbook
from nominal.ts import IntegralNanosecondsDuration, _from_api_duration


class _Clients(HasScoutParams, Protocol):
    @property
    def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...


def _iter_list_streaming_checklists(
    clients: _Clients,
    asset_rid: str | None = None,
) -> Iterable[str]:
    if asset_rid is None:
        return list_streaming_checklists_paginated(clients.checklist_execution, clients.auth_header)
    return list_streaming_checklists_for_asset_paginated(clients.checklist_execution, clients.auth_header, asset_rid)


@dataclass(frozen=True)
class StreamingNotificationConfig:
    """A notification destination and its settings for streaming checklist execution.

    Args:
        integration: Existing integration or its RID. Readback contains the RID.
        tags: Tags attached to notifications for this destination.
        workbook: Optional workbook or its RID to link from failure and recovery notifications.
            Readback contains the RID.
        mute_execution_errors: Suppress notifications about execution and channel-resolution errors.
        mute_until: Suppress all notifications until this timezone-aware deadline. ``None`` leaves
            notifications unmuted. Readback uses Python datetime's microsecond precision.
    """

    integration: Integration | str
    _: KW_ONLY
    tags: Sequence[str] = ()
    workbook: Workbook | str | None = None
    mute_execution_errors: bool = False
    mute_until: datetime | None = None

    def _to_conjure(self) -> scout_integrations_api.NotificationConfiguration:
        if self.mute_until is not None and self.mute_until.utcoffset() is None:
            raise ValueError("mute_until must include a timezone")
        return scout_integrations_api.NotificationConfiguration(
            integration_rid=rid_from_instance_or_string(self.integration),
            tags=list(self.tags),
            appended_workbook_rid=None if self.workbook is None else rid_from_instance_or_string(self.workbook),
            notification_filters=[scout_integrations_api.NotificationFilter.EXECUTION_ERROR]
            if self.mute_execution_errors
            else [],
            mute_until=None if self.mute_until is None else self.mute_until.isoformat(),
        )

    @classmethod
    def _from_conjure(cls, config: scout_integrations_api.NotificationConfiguration) -> Self:
        return cls(
            integration=config.integration_rid,
            tags=tuple(config.tags),
            workbook=config.appended_workbook_rid,
            mute_execution_errors=scout_integrations_api.NotificationFilter.EXECUTION_ERROR
            in (config.notification_filters or []),
            mute_until=None if config.mute_until is None else datetime.fromisoformat(config.mute_until),
        )


@dataclass(frozen=True)
class StreamingChecklistConfiguration:
    """Current streaming settings for one asset, returned by `Checklist.get_streaming_configurations`.

    Delays are integer nanoseconds and can be passed directly to `Checklist.execute_streaming`.
    Reapplying these settings replaces the asset's full configuration.
    """

    notifications: Sequence[StreamingNotificationConfig]
    evaluation_delay: IntegralNanosecondsDuration
    recovery_delay: IntegralNanosecondsDuration
    auto_create_events: bool

    @classmethod
    def _from_conjure(cls, config: scout_checklistexecution_api.AssetStreamConfiguration) -> Self:
        return cls(
            notifications=tuple(
                StreamingNotificationConfig._from_conjure(n) for n in config.notification_configurations
            ),
            evaluation_delay=_from_api_duration(config.evaluation_delay),
            recovery_delay=_from_api_duration(config.recovery_delay),
            auto_create_events=config.auto_create_events,
        )


class StreamingChecklistState(str, Enum):
    """Lifecycle state of a checklist running against one asset."""

    RUNNING = "running"
    """Evaluating checks; individual checks may still fail or be skipped."""

    INITIALIZING = "initializing"
    """Preparing execution; check results are not yet available."""

    FAILED = "failed"
    """Execution failed unexpectedly."""


class StreamingCheckState(str, Enum):
    """Latest evaluation state of an individual check."""

    PASS = "pass"
    """The check is passing."""

    FAIL = "fail"
    """The check is failing."""

    INVALID = "invalid"
    """Evaluation failed, for example because the computation was too expensive."""

    SKIPPED = "skipped"
    """Not evaluated, for example because channels are unresolved or streaming features are unsupported."""


@dataclass(frozen=True)
class StreamingCheckStatus:
    """Latest result for a check; parameter_index is zero-based, or None for a non-parameterized check."""

    check_rid: str
    parameter_index: int | None
    state: StreamingCheckState

    @classmethod
    def _from_conjure(cls, result: scout_checklistexecution_api.CheckLiveStatusResponse) -> Self:
        return cls(
            check_rid=result.check_rid,
            parameter_index=result.check_parameter_index,
            state=StreamingCheckState(result.status.type),
        )


@dataclass(frozen=True)
class StreamingChecklistStatus:
    """Live status for one asset, returned by `Checklist.get_streaming_status`.

    The executing commit and individual check results are available only while running.
    A running checklist can still contain failing, invalid, or skipped checks.
    """

    state: StreamingChecklistState
    commit: str | None
    checks: Sequence[StreamingCheckStatus]

    @classmethod
    def _from_conjure(cls, status: scout_checklistexecution_api.ChecklistLiveStatus) -> Self:
        running = status.running
        return cls(
            state=StreamingChecklistState(status.type),
            commit=None if running is None else running.commit_id,
            checks=()
            if running is None
            else tuple(StreamingCheckStatus._from_conjure(c) for c in running.check_results),
        )


class ChannelResolutionState(str, Enum):
    """Whether a channel reference can be resolved for an asset."""

    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"
    UNKNOWN = "UNKNOWN"
    """The channel resolution state is not recognized by this SDK."""


@dataclass(frozen=True)
class ChecklistChannelResolution:
    """Resolution of a channel reference, including its data scope and series tags.

    Optional asset, run, and input identifiers describe any explicit context in the reference.
    """

    channel: str
    data_scope: str
    tags: Mapping[str, str]
    state: ChannelResolutionState
    asset_rid: str | None
    run_rid: str | None
    input_id: str | None

    @classmethod
    def _from_conjure(cls, result: scout_checklistexecution_api.ChannelLocatorWithStatus) -> Self:
        channel = result.channel_locator
        return cls(
            channel=channel.channel,
            data_scope=channel.data_source_ref,
            tags=dict(channel.tags),
            state=ChannelResolutionState(result.resolution_status.value),
            asset_rid=channel.asset,
            run_rid=channel.run,
            input_id=channel.input_id,
        )


@dataclass(frozen=True)
class CheckResolution:
    """Channel resolution and streaming compatibility for an individual check.

    ``parameter_index`` is zero-based, or ``None`` for a non-parameterized check.
    ``unsupported_streaming_features`` contains feature codes such as ``INTEGRAL`` or
    ``TIME_RANGE_FILTER_WITH_FIXED_END``. A resolved channel alone does not establish streaming compatibility.
    """

    check_rid: str
    parameter_index: int | None
    channels: Sequence[ChecklistChannelResolution]
    unsupported_streaming_features: Sequence[str]

    @classmethod
    def _from_conjure(cls, result: scout_checklistexecution_api.ResolvedCheckStatus) -> Self:
        return cls(
            check_rid=result.check_rid,
            parameter_index=result.check_parameter_index,
            channels=tuple(ChecklistChannelResolution._from_conjure(channel) for channel in result.channels),
            unsupported_streaming_features=tuple(feature.value for feature in result.invalid_streaming_compute_nodes),
        )
