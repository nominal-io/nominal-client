from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Literal, Mapping, Protocol, Sequence

from nominal_api import (
    scout_checklistexecution_api,
    scout_checks_api,
    scout_integrations_api,
    scout_run_api,
)
from typing_extensions import Self

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid, rid_from_instance_or_string
from nominal.core.asset import Asset


@dataclass(frozen=True)
class Checklist(HasRid):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def checklist(self) -> scout_checks_api.ChecklistService: ...
        @property
        def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, checklist: scout_checks_api.VersionedChecklist) -> Self:
        # TODO(ritwikdixit): support draft checklists with VCS
        if not checklist.metadata.is_published:
            raise ValueError("cannot get a checklist that has not been published")

        return cls(
            rid=checklist.rid,
            name=checklist.metadata.title,
            description=checklist.metadata.description,
            properties=checklist.metadata.properties,
            labels=checklist.metadata.labels,
            _clients=clients,
        )

    def execute_streaming(
        self,
        assets: Sequence[Asset | str],
        integration_rids: Sequence[str],
        *,
        evaluation_delay: timedelta = timedelta(),
        recovery_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        """Execute the checklist for the given assets.
        - `assets`: Can be `Asset` instances, or Asset RIDs.
        - `integration_rids`: Checklist violations will be sent to the specified integrations. At least one integration
           must be specified. See https://app.gov.nominal.io/settings/integrations for a list of available integrations.
        - `evaluation_delay`: Delays the evaluation of the streaming checklist. This is useful for when data is delayed.
        - `recovery_delay`: Specifies the minimum amount of time that must pass before a check can recover from a
                            failure. Minimum value is 15 seconds.
        """
        self._clients.checklist_execution.execute_streaming_checklist(
            self._clients.auth_header,
            scout_checklistexecution_api.ExecuteChecklistForAssetsRequest(
                assets=[rid_from_instance_or_string(asset) for asset in assets],
                checklist=self.rid,
                notification_configurations=[
                    scout_integrations_api.NotificationConfiguration(c, tags=[]) for c in integration_rids
                ],
                evaluation_delay=_to_api_duration(evaluation_delay),
                recovery_delay=_to_api_duration(recovery_delay),
            ),
        )

    def stop_streaming(self) -> None:
        """Stop the checklist."""
        self._clients.checklist_execution.stop_streaming_checklist(self._clients.auth_header, self.rid)

    def stop_streaming_for_assets(self, assets: Sequence[Asset | str]) -> None:
        """Stop the checklist for the given assets."""
        self._clients.checklist_execution.stop_streaming_checklist_for_assets(
            self._clients.auth_header,
            scout_checklistexecution_api.StopStreamingChecklistForAssetsRequest(
                assets=[rid_from_instance_or_string(asset) for asset in assets],
                checklist=self.rid,
            ),
        )

    def reload_streaming(self) -> None:
        """Reload the checklist."""
        self._clients.checklist_execution.reload_streaming_checklist(self._clients.auth_header, self.rid)

    def archive(self) -> None:
        """Archive this checklist.
        Archived checklists are not deleted, but are hidden from the UI.
        """
        self._clients.checklist.archive(
            self._clients.auth_header, scout_checks_api.ArchiveChecklistsRequest(rids=[self.rid])
        )

    def unarchive(self) -> None:
        """Unarchive this checklist, allowing it to be viewed in the UI."""
        self._clients.checklist.unarchive(
            self._clients.auth_header, scout_checks_api.UnarchiveChecklistsRequest(rids=[self.rid])
        )


Priority = Literal[0, 1, 2, 3, 4]


_priority_to_conjure_map: dict[Priority, scout_checks_api.Priority] = {
    0: scout_checks_api.Priority.P0,
    1: scout_checks_api.Priority.P1,
    2: scout_checks_api.Priority.P2,
    3: scout_checks_api.Priority.P3,
    4: scout_checks_api.Priority.P4,
}


def _conjure_priority_to_priority(priority: scout_checks_api.Priority) -> Priority:
    inverted_map = {v: k for k, v in _priority_to_conjure_map.items()}
    if priority in inverted_map:
        return inverted_map[priority]
    raise ValueError(f"unknown priority '{priority}', expected one of {_priority_to_conjure_map.values()}")


def _to_api_duration(duration: timedelta) -> scout_run_api.Duration:
    return scout_run_api.Duration(seconds=int(duration.total_seconds()), nanos=duration.microseconds * 1000)
