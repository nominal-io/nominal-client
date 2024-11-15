from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Sequence

from typing_extensions import Self

from nominal._api.combined import scout_checklistexecution_api, scout_integrations_api, scout_run_api
from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid, rid_from_instance_or_string
from nominal.core.asset import Asset


@dataclass(frozen=True)
class StreamingChecklist(HasRid):
    rid: str

    _clients: "_Clients" = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...

    def execute(
        self, assets: Sequence[Asset | str], *, notification_configurations: Sequence[str] | None = None
    ) -> None:
        """Execute the checklist for the given assets.
        `assets` can be `Asset` instances, or asset RIDs.
        `notification_configurations` are Integration RIDs.
        """
        self._clients.checklist_execution.execute_streaming_checklist(
            self._clients.auth_header,
            scout_checklistexecution_api.ExecuteChecklistForAssetsRequest(
                assets=[rid_from_instance_or_string(asset) for asset in assets],
                checklist=self.rid,
                notification_configurations=[
                    scout_integrations_api.NotificationConfiguration(c) for c in notification_configurations or []
                ],
                stream_delay=scout_run_api.Duration(seconds=0, nanos=0),
            ),
        )

    def stop(self) -> None:
        """Stop the checklist."""
        self._clients.checklist_execution.stop_streaming_checklist(self._clients.auth_header, self.rid)

    def stop_for_assets(self, assets: Sequence[Asset | str]) -> None:
        """Stop the checklist for the given assets."""
        self._clients.checklist_execution.stop_streaming_checklist_for_assets(
            self._clients.auth_header,
            scout_checklistexecution_api.StopStreamingChecklistForAssetsRequest(
                assets=[rid_from_instance_or_string(asset) for asset in assets],
                checklist=self.rid,
            ),
        )

    @classmethod
    def _from_conjure(
        cls, clients: _Clients, streaming_checklist: scout_checklistexecution_api.StreamingChecklistInfo
    ) -> Self:
        return cls(
            rid=streaming_checklist.checklist_rid,
            _clients=clients,
        )
