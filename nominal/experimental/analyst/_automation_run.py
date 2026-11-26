from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Protocol, Self

from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.experimental.analyst._transport import _AutomationTransport
from nominal.protos.ai.v1 import analyst_automation_pb2 as api
from nominal.ts import IntegralNanosecondsUTC


class AutomationRunStatus(IntEnum):
    """Server-reported lifecycle of an automation run."""

    UNSPECIFIED = 0
    QUEUED = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    CANCELED = 5
    INTERACTION_REQUIRED = 6
    REJECTED = 7

    @classmethod
    def _missing_(cls, value: object) -> AutomationRunStatus:
        return cls.UNSPECIFIED


class AutomationNodeRunStatus(IntEnum):
    """Server-reported lifecycle of one node execution."""

    UNSPECIFIED = 0
    RUNNING = 1
    SUCCEEDED = 2
    FAILED = 3
    CANCELED = 4
    SKIPPED = 5
    INTERACTION_REQUIRED = 6

    @classmethod
    def _missing_(cls, value: object) -> AutomationNodeRunStatus:
        return cls.UNSPECIFIED


@dataclass(frozen=True)
class AutomationNodeRun:
    """Node execution status and its Analyst conversation, if one was created."""

    node_id: str
    iteration: int
    status: AutomationNodeRunStatus
    error_code: str
    conversation_rid: str | None
    conversation_url: str
    trace_id: str
    started_at: IntegralNanosecondsUTC | None
    ended_at: IntegralNanosecondsUTC | None

    @classmethod
    def _from_proto(cls, raw: api.AutomationNodeRun) -> Self:
        return cls(
            node_id=raw.node_id,
            iteration=raw.iteration,
            status=AutomationNodeRunStatus(raw.status),
            error_code=raw.error_code,
            conversation_rid=raw.conversation_rid if raw.HasField("conversation_rid") else None,
            conversation_url=raw.conversation_url,
            trace_id=raw.trace_id,
            started_at=raw.started_at.ToNanoseconds() if raw.HasField("started_at") else None,
            ended_at=raw.ended_at.ToNanoseconds() if raw.HasField("ended_at") else None,
        )


@dataclass(frozen=True)
class AutomationRun(HasRid, RefreshableGrpcMixin[api.AutomationRun]):
    """Snapshot of a queued, active, or completed automation execution.

    Refresh to observe progress. A rejected or failed run is returned as a run, not raised
    as a request error. Cancellation requests do not imply that execution has stopped.
    """

    rid: str
    automation_rid: str
    workspace_rid: str
    revision: int
    status: AutomationRunStatus
    trigger_id: str
    trigger_type: str
    error_code: str
    cancel_requested: bool
    url: str
    created_at: IntegralNanosecondsUTC
    started_at: IntegralNanosecondsUTC | None
    ended_at: IntegralNanosecondsUTC | None
    _clients: _Clients = field(repr=False)

    class _Clients(Protocol):
        @property
        def analyst_automations(self) -> _AutomationTransport: ...

    @property
    def is_finished(self) -> bool:
        """Whether the server reported a known terminal state, including failure or rejection."""
        return self.status in (
            AutomationRunStatus.SUCCEEDED,
            AutomationRunStatus.FAILED,
            AutomationRunStatus.CANCELED,
            AutomationRunStatus.INTERACTION_REQUIRED,
            AutomationRunStatus.REJECTED,
        )

    def _get_latest_api(self) -> api.AutomationRun:
        return self._clients.analyst_automations.call(
            "GetAutomationRun", api.GetAutomationRunRequest(automation_run_rid=self.rid), api.GetAutomationRunResponse
        ).automation_run

    def cancel(self) -> Self:
        """Request cancellation and refresh this snapshot; poll refresh until execution stops."""
        response = self._clients.analyst_automations.call(
            "CancelAutomationRun",
            api.CancelAutomationRunRequest(automation_run_rid=self.rid),
            api.CancelAutomationRunResponse,
        )
        return self._refresh_from_api(response.automation_run)

    def list_node_runs(self) -> Iterator[AutomationNodeRun]:
        """Iterate node executions, fetching subsequent pages lazily."""
        page_token = ""
        while True:
            response = self._clients.analyst_automations.call(
                "ListAutomationNodeRuns",
                api.ListAutomationNodeRunsRequest(automation_run_rid=self.rid, page_token=page_token),
                api.ListAutomationNodeRunsResponse,
            )
            yield from (AutomationNodeRun._from_proto(raw) for raw in response.node_runs)
            page_token = response.next_page_token
            if not page_token:
                return

    @classmethod
    def _from_proto(cls, clients: _Clients, raw: api.AutomationRun) -> Self:
        return cls(
            rid=raw.automation_run_rid,
            automation_rid=raw.automation_rid,
            workspace_rid=raw.workspace_rid,
            revision=raw.revision,
            status=AutomationRunStatus(raw.status),
            trigger_id=raw.trigger_id,
            trigger_type=raw.trigger_type,
            error_code=raw.error_code,
            cancel_requested=raw.cancel_requested,
            url=raw.url,
            created_at=raw.created_at.ToNanoseconds(),
            started_at=raw.started_at.ToNanoseconds() if raw.HasField("started_at") else None,
            ended_at=raw.ended_at.ToNanoseconds() if raw.HasField("ended_at") else None,
            _clients=clients,
        )
