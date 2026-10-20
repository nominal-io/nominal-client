from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from time import sleep
from typing import TYPE_CHECKING, Iterable, Protocol, Self, Sequence

from nominal_api import (
    scout_checklistexecution_api,
    scout_checks_api,
)
from typing_extensions import deprecated

from nominal.core._checklist_types import Priority
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin, rid_from_instance_or_string
from nominal.core._utils.frontend_urls import data_review_events_url, data_review_url
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_data_reviews_paginated
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.event import Event, _get_events
from nominal.core.run import _get_run
from nominal.protos.datareview.v2 import data_review_pb2, data_review_pb2_grpc
from nominal.protos.event.v2 import event_pb2_grpc
from nominal.protos.run.v1 import run_service_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

if TYPE_CHECKING:
    from nominal.core.asset import Asset
    from nominal.core.checklist import Checklist
    from nominal.core.integration import Integration
    from nominal.core.run import Run


@dataclass(frozen=True)
class DataReview(HasRid, RefreshableGrpcMixin[data_review_pb2.DataReview]):
    """The progress and results of a checklist execution."""

    rid: str
    run_rid: str
    checklist_rid: str
    checklist_commit: str
    completed: bool
    created_at: IntegralNanosecondsUTC

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)
    asset_rid: str | None = None
    """Asset evaluated by this review, or None when no asset RID is available."""

    class _Clients(HasScoutParams, Protocol):
        @property
        def datareview(self) -> data_review_pb2_grpc.DataReviewServiceStub: ...
        @property
        def checklist(self) -> scout_checks_api.ChecklistService: ...
        @property
        def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...
        @property
        def event(self) -> event_pb2_grpc.EventServiceStub: ...
        @property
        def run(self) -> run_service_pb2_grpc.RunServiceStub: ...

    @classmethod
    def _from_proto(cls, clients: _Clients, data_review: data_review_pb2.DataReview) -> Self:
        executing_states = [
            check.state.WhichOneof("automatic_check_evaluation_state") in ("pending_execution", "executing")
            for check in data_review.check_evaluations
        ]
        completed = not any(executing_states)
        return cls(
            rid=data_review.rid,
            run_rid=data_review.run_rid,
            checklist_rid=data_review.checklist_ref.rid,
            checklist_commit=data_review.checklist_ref.commit,
            completed=completed,
            created_at=data_review.created_at.ToNanoseconds(),
            _clients=clients,
            created_by_rid=data_review.created_by or None,
            asset_rid=data_review.asset_rid or None,
        )

    def _get_latest_api(self) -> data_review_pb2.DataReview:
        return _get_data_review(self._clients, self.rid)

    def get_checklist(self) -> "Checklist":
        from nominal.core.checklist import Checklist

        return Checklist._from_conjure(
            self._clients,
            self._clients.checklist.get(self._clients.auth_header, self.checklist_rid, commit=self.checklist_commit),
        )

    def get_events(self) -> Sequence[Event]:
        """Retrieves the list of events for the data review."""
        all_event_rids = [
            event_rid
            for check in _get_data_review(self._clients, self.rid).check_evaluations
            if check.state.HasField("generated_alerts")
            for event_rid in check.state.generated_alerts.event_rids
        ]
        return [
            Event._from_proto(self._clients, data_review_event)
            for data_review_event in _get_events(self._clients, all_event_rids)
        ]

    @deprecated("DataReview.reload() is deprecated. Use DataReview.refresh() to update this instance in place.")
    def reload(self) -> Self:
        """Deprecated alias for `refresh`; updates and returns this instance rather than a separate snapshot."""
        return self.refresh()

    def poll_for_completion(self, interval: timedelta = timedelta(seconds=2)) -> DataReview:
        """Refresh this instance until the review completes, then return it."""
        while not self.completed:
            sleep(interval.total_seconds())
            self.refresh()
        return self

    def archive(self) -> None:
        """Archive this data review.
        Archived data reviews are not deleted, but are hidden from the UI.
        """
        with translate_grpc_errors():
            self._clients.datareview.ArchiveDataReview(
                data_review_pb2.ArchiveDataReviewRequest(data_review_rid=self.rid)
            )

    def unarchive(self) -> None:
        """Unarchive this review, allowing it to appear in the UI again."""
        with translate_grpc_errors():
            self._clients.datareview.UnarchiveDataReview(
                data_review_pb2.UnarchiveDataReviewRequest(data_review_rid=self.rid)
            )

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Data Review in the Nominal app"""
        return data_review_url(self._clients, self.run_rid, self.rid)

    @property
    def events_url(self) -> str:
        """Returns a link to the page for events for this Data Review in the Nominal app"""
        return data_review_events_url(self._clients, self.run_rid, self.rid)


@dataclass(frozen=True)
class CheckViolation:
    """A checklist violation with its affected time interval and priority."""

    rid: str
    check_rid: str
    name: str
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    priority: Priority | None


@dataclass(frozen=True)
class DataReviewBuilder:
    """Collect checklist executions and notification destinations before initiating reviews.

    Obtain a builder with `NominalClient.data_review_builder`.

    Example:
        Attach an existing Slack, Opsgenie, or webhook integration:

        ```python
        integration = client.get_integration("integration-rid")
        reviews = (
            client.data_review_builder()
            .execute_checklist("run-rid", "checklist-rid")
            .add_integration(integration)
            .initiate()
        )
        ```
    """

    _integration_rids: list[str]
    _requests: list[data_review_pb2.CreateDataReviewRequest]
    _tags: list[str]
    _clients: DataReview._Clients = field(repr=False)

    def add_integration(self, integration_rid: str | Integration) -> DataReviewBuilder:
        """Add a notification destination for the reviews initiated by this builder.

        Args:
            integration_rid: Integration RID or an `Integration` instance.

        Returns:
            This builder.

        Note:
            Data review notifications support Slack, Opsgenie, generic webhooks, and secure webhooks.
            Teams and PagerDuty are unsupported. This method does not check whether the destination is supported.

            Tags added with `add_tags` apply to every destination, including those already added.
        """
        self._integration_rids.append(rid_from_instance_or_string(integration_rid))
        return self

    def execute_checklist(
        self,
        run: str | Run,
        checklist: str | Checklist,
        *,
        commit: str | None = None,
        asset: str | Asset | None = None,
    ) -> Self:
        """Add a request to create a data review for the given checklist and run.

        Args:
            run: Instance or rid of the Run to run the Checklist on
            checklist: Instance or rid of the checklist to execute on the Run
            commit: Commit hash of the version of the checklist to run, or the latest version if None is provided
            asset: Instance or rid of the asset to run the checklist on within the Run.

                **Note:** Only required for multi-asset runs.

        Returns:
            DataReviewBuilder instance to continue building a data review with

        Raises:
            ValueError: If the given run has multiple associated assets and no asset was specified, or
                if the run has an asset that differs from the provided asset.
        """
        checklist_rid = rid_from_instance_or_string(checklist)
        run_rid = rid_from_instance_or_string(run)
        asset_rid = None if asset is None else rid_from_instance_or_string(asset)

        raw_run = _get_run(self._clients.run, run_rid)
        if len(raw_run.assets) > 1 and asset is None:
            raise ValueError(
                f"Cannot run data review on checklist {checklist_rid} and {run_rid} without specifying `asset_rid`: "
                f"run has {len(raw_run.assets)} assets!"
            )
        elif len(raw_run.assets) == 1 and asset_rid is not None:
            raw_asset_rid = raw_run.assets[0]
            if raw_asset_rid != asset_rid:
                raise ValueError(
                    f"Cannot run data review on checklist {checklist_rid} and {run_rid} with asset {asset_rid}: "
                    f"run has a different asset {raw_asset_rid}!"
                )

        self._requests.append(
            data_review_pb2.CreateDataReviewRequest(
                checklist_rid=checklist_rid,
                run_rid=run_rid,
                asset_rid=asset_rid,
                commit=commit,
            )
        )
        return self

    def add_tags(self, tags: list[str]) -> DataReviewBuilder:
        self._tags.extend(tags)
        return self

    def initiate(self, wait_for_completion: bool = True) -> Sequence[DataReview]:
        """Initiates a batch data review process.

        Args:
            wait_for_completion: If True, waits for the data review process to complete before returning.

        Returns:
            The initiated reviews, which may still be running unless wait_for_completion is True.
            Waiting for review completion does not wait for or confirm notification delivery.
        """
        request = data_review_pb2.BatchInitiateRequest(
            notification_configurations=[
                data_review_pb2.NotificationConfiguration(integration_rid=integration_rid, tags=self._tags)
                for integration_rid in self._integration_rids
            ],
            requests=self._requests,
        )
        with translate_grpc_errors():
            response = self._clients.datareview.BatchInitiate(request)

        data_reviews = [
            DataReview._from_proto(self._clients, _get_data_review(self._clients, rid)) for rid in response.rids
        ]
        if wait_for_completion:
            return poll_until_completed(data_reviews)
        else:
            return data_reviews


def poll_until_completed(
    data_reviews: Sequence[DataReview], interval: timedelta = timedelta(seconds=2)
) -> Sequence[DataReview]:
    return [review.poll_for_completion(interval) for review in data_reviews]


def _iter_search_data_reviews(
    clients: DataReview._Clients,
    assets: Sequence[str] | None = None,
    runs: Sequence[str] | None = None,
    archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
) -> Iterable[DataReview]:
    for review in search_data_reviews_paginated(
        clients.datareview,
        assets=assets,
        runs=runs,
        archive_status=archive_status,
    ):
        yield DataReview._from_proto(clients, review)


def _get_data_review(clients: DataReview._Clients, rid: str) -> data_review_pb2.DataReview:
    """Retrieve the data review with the given RID.

    Raises:
        NominalNotFoundError: If no data review has that RID.
        NominalError: If the retrieval request fails.
    """
    with translate_grpc_errors():
        return clients.datareview.GetDataReview(data_review_pb2.GetDataReviewRequest(data_review_rid=rid)).data_review
