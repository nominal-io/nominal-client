from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Mapping, Protocol, Self, Sequence

from nominal_api import (
    scout_checklistexecution_api,
    scout_checks_api,
)

from nominal._utils.iterator_tools import batched
from nominal.core import run as core_run
from nominal.core._utils.api_tools import HasRid, RefreshableConjureMixin, rid_from_instance_or_string
from nominal.core._utils.frontend_urls import checklist_preview_url, checklist_url
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.asset import Asset
from nominal.core.data_review import DataReview, _get_data_review
from nominal.core.integration import Integration
from nominal.core.streaming_checklist import (
    StreamingChecklistConfiguration,
    StreamingChecklistStatus,
    StreamingNotificationConfig,
)
from nominal.exceptions import NominalChecklistNotPublishedError
from nominal.protos.datareview.v2 import data_review_pb2
from nominal.ts import IntegralNanosecondsDuration, _to_api_duration

_LIVE_STATUS_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class Checklist(HasRid, RefreshableConjureMixin[scout_checks_api.VersionedChecklist]):
    """A set of checks that can evaluate a run or monitor streaming asset data."""

    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    _clients: _Clients = field(repr=False)
    author_rid: str | None = field(default=None, repr=False)
    commit_id: str | None = None
    """Loaded version ID; pass explicitly to execution to pin a review. None for unloaded manual instances."""

    class _Clients(DataReview._Clients, Protocol):
        @property
        def checklist(self) -> scout_checks_api.ChecklistService: ...
        @property
        def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, checklist: scout_checks_api.VersionedChecklist) -> Self:
        # TODO(ritwikdixit): support draft checklists with VCS
        if not checklist.metadata.is_published:
            raise NominalChecklistNotPublishedError(
                f"cannot get checklist {checklist.rid!r}: this version has not been published"
            )

        return cls(
            rid=checklist.rid,
            name=checklist.metadata.title,
            description=checklist.metadata.description,
            properties=checklist.metadata.properties,
            labels=checklist.metadata.labels,
            _clients=clients,
            author_rid=checklist.metadata.author_rid,
            commit_id=checklist.commit.id,
        )

    def _get_latest_api(self) -> scout_checks_api.VersionedChecklist:
        return self._clients.checklist.get(self._clients.auth_header, self.rid)

    def execute(self, run: core_run.Run | str, commit: str | None = None) -> DataReview:
        """Execute a checklist against a run.

        Args:
            run: Run (or its rid) to execute the checklist against
            commit: Commit hash of the version of the checklist to run, or None for the latest version

        Returns:
            Created datareview for the checklist execution
        """
        run_rid = rid_from_instance_or_string(run)

        request = data_review_pb2.BatchInitiateRequest(
            requests=[data_review_pb2.CreateDataReviewRequest(checklist_rid=self.rid, run_rid=run_rid, commit=commit)],
        )
        with translate_grpc_errors():
            response = self._clients.datareview.BatchInitiate(request)
        if len(response.rids) != 1:
            raise RuntimeError(f"Expected exactly one response from BatchInitiate, received {len(response.rids)}")

        return DataReview._from_proto(self._clients, _get_data_review(self._clients, response.rids[0]))

    def execute_streaming(
        self,
        assets: Sequence[Asset | str],
        integration_rids: Sequence[str] | None = None,
        *,
        notifications: Sequence[Integration | str | StreamingNotificationConfig] | None = None,
        evaluation_delay: timedelta | IntegralNanosecondsDuration = timedelta(),
        recovery_delay: timedelta | IntegralNanosecondsDuration = timedelta(seconds=15),
        auto_create_events: bool = False,
    ) -> None:
        """Start streaming or replace the streaming settings for the specified assets.

        Settings for other assets are unchanged. Returning means the request was accepted;
        use `get_streaming_status` to inspect execution. It does not confirm notification delivery.

        Args:
            assets: Assets or their RIDs to run this published checklist against.
            integration_rids: Notification integration RIDs with default settings. Existing positional
                and keyword calls remain supported but emit a deprecation warning. Pass these RIDs
                through ``notifications`` instead.
            notifications: Integrations or their RIDs, optionally wrapped in `StreamingNotificationConfig`
                for tags, workbook links, and mute settings. Cannot be combined
                with ``integration_rids``. Omitting both arguments or passing an empty list disables notifications.
            evaluation_delay: Delay evaluation to accommodate late-arriving data, as a timedelta or integer nanoseconds.
            recovery_delay: Minimum time before a failing check can recover, as a timedelta or integer nanoseconds.
                Must be at least 15 seconds.
            auto_create_events: Create events when checks fail and update them when checks recover.

        Raises:
            ValueError: Both notification arguments are supplied, or a mute deadline lacks a timezone.

        Note:
            This replaces all streaming settings for the specified assets, including existing notification
            destinations. Read `get_streaming_configurations` first to preserve settings when changing them.
        """
        if integration_rids is not None and notifications is not None:
            raise ValueError("Supply either integration_rids or notifications, not both")
        if integration_rids is not None:
            warnings.warn(
                "integration_rids is deprecated; pass integrations or their RIDs through notifications instead",
                UserWarning,
                stacklevel=2,
            )
        if notifications is None:
            notifications = integration_rids or ()
        self._clients.checklist_execution.execute_streaming_checklist(
            self._clients.auth_header,
            scout_checklistexecution_api.ExecuteChecklistForAssetsRequest(
                assets=[rid_from_instance_or_string(asset) for asset in assets],
                checklist=self.rid,
                notification_configurations=[
                    (
                        notification
                        if isinstance(notification, StreamingNotificationConfig)
                        else StreamingNotificationConfig(notification)
                    )._to_conjure()
                    for notification in notifications
                ],
                evaluation_delay=_to_api_duration(evaluation_delay),
                recovery_delay=_to_api_duration(recovery_delay),
                auto_create_events=auto_create_events,
            ),
        )

    def get_streaming_configurations(self) -> Mapping[str, StreamingChecklistConfiguration]:
        """Fetch streaming settings keyed by asset RID.

        Notification destinations and workbooks are returned as RIDs without fetching those resources.
        A checklist with no streaming configuration raises the service's not-found error.
        """
        info = self._clients.checklist_execution.get_streaming_checklist(self._clients.auth_header, self.rid)
        return {
            asset_rid: StreamingChecklistConfiguration._from_conjure(config)
            for asset_rid, config in info.asset_configurations.items()
        }

    def get_streaming_status(self, assets: Sequence[Asset | str]) -> Mapping[str, StreamingChecklistStatus]:
        """Fetch live status and check results keyed by asset RID.

        Args:
            assets: Assets or their RIDs already configured to stream this checklist. An asset without
                a streaming configuration causes the request to fail. An empty sequence returns an empty mapping.

        Returns:
            Status for each returned asset, including the executing commit when running.

        Raises:
            ValueError: The service returns a lifecycle or check-status variant unknown to the
                installed generated bindings. Upgrade the SDK to read newer status variants.

        Note:
            Large reads use multiple requests and are not an atomic snapshot. If any request fails,
            the method raises without returning partial results.
        """
        statuses: dict[str, StreamingChecklistStatus] = {}
        for asset_batch in batched(assets, _LIVE_STATUS_BATCH_SIZE):
            request = scout_checklistexecution_api.BatchChecklistLiveStatusRequest(
                requests=[
                    scout_checklistexecution_api.ChecklistLiveStatusRequest(
                        asset_rid=rid_from_instance_or_string(asset), checklist_rid=self.rid
                    )
                    for asset in asset_batch
                ]
            )
            response = self._clients.checklist_execution.checklist_live_status(self._clients.auth_header, request)
            statuses.update(
                (result.asset_rid, StreamingChecklistStatus._from_conjure(result.status))
                for result in response.checklist_live_status_responses
            )
        return statuses

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

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this checklist in the Nominal app"""
        return checklist_url(self._clients, self.rid)

    def preview_for_run_url(self, run: core_run.Run | str) -> str:
        """Returns a link to the page for previewing this checklist on a given run in the Nominal app"""
        run_rid = rid_from_instance_or_string(run)
        return checklist_preview_url(self._clients, self.rid, run_rid)
