from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from time import sleep
from typing import TYPE_CHECKING, Iterable, Protocol, Sequence

from nominal_api import (
    event as event_api,
)
from nominal_api import (
    scout,
    scout_checklistexecution_api,
    scout_checks_api,
)
from typing_extensions import Self

from nominal.core._checklist_types import Priority
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, rid_from_instance_or_string
from nominal.core._utils.frontend_urls import data_review_events_url, data_review_url
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_data_reviews_paginated
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.event import Event
from nominal.protos.datareview.v2 import data_review_pb2, data_review_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC

if TYPE_CHECKING:
    from nominal.core.asset import Asset
    from nominal.core.checklist import Checklist
    from nominal.core.run import Run


@dataclass(frozen=True)
class DataReview(HasRid):
    rid: str
    run_rid: str
    checklist_rid: str
    checklist_commit: str
    completed: bool
    created_at: IntegralNanosecondsUTC

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def datareview(self) -> data_review_pb2_grpc.DataReviewServiceStub: ...
        @property
        def checklist(self) -> scout_checks_api.ChecklistService: ...
        @property
        def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...
        @property
        def event(self) -> event_api.EventService: ...
        @property
        def run(self) -> scout.RunService: ...

    @classmethod
    def _from_proto(cls, clients: _Clients, data_review: data_review_pb2.DataReview) -> Self:
        still_executing = any(
            check.state.HasField("pending_execution") or check.state.HasField("executing")
            for check in data_review.check_evaluations
        )
        return cls(
            rid=data_review.rid,
            run_rid=data_review.run_rid,
            checklist_rid=data_review.checklist_ref.rid,
            checklist_commit=data_review.checklist_ref.commit,
            completed=not still_executing,
            created_at=data_review.created_at.ToNanoseconds(),
            _clients=clients,
            created_by_rid=data_review.created_by or None,
        )

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
            for check in _get_data_review_proto(self._clients, self.rid).check_evaluations
            if check.state.HasField("generated_alerts")
            for event_rid in check.state.generated_alerts.event_rids
        ]
        event_response = self._clients.event.batch_get_events(self._clients.auth_header, all_event_rids)
        return [Event._from_conjure(self._clients, data_review_event) for data_review_event in event_response]

    def reload(self) -> DataReview:
        """Reloads the data review from the server."""
        return _get_data_review(self._clients, self.rid)

    def poll_for_completion(self, interval: timedelta = timedelta(seconds=2)) -> DataReview:
        """Polls the data review until it is completed."""
        review = self
        while not review.completed:
            sleep(interval.total_seconds())
            review = review.reload()
        return review

    def archive(self) -> None:
        """Archive this data review.
        Archived data reviews are not deleted, but are hidden from the UI.

        NOTE: currently, it is not possible (yet) to unarchive a data review once archived.
        """
        with translate_grpc_errors():
            self._clients.datareview.ArchiveDataReview(
                data_review_pb2.ArchiveDataReviewRequest(data_review_rid=self.rid)
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
    rid: str
    check_rid: str
    name: str
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    priority: Priority | None

    @classmethod
    def _from_proto(cls, check_alert: data_review_pb2.CheckAlert) -> CheckViolation:
        return cls(
            rid=check_alert.rid,
            check_rid=check_alert.check_rid,
            name=check_alert.name,
            start=check_alert.start.ToNanoseconds(),
            end=check_alert.end.ToNanoseconds() if check_alert.HasField("end") else None,
            priority=Priority._from_proto(check_alert.priority),
        )


@dataclass(frozen=True)
class DataReviewBuilder:
    _integration_rids: list[str]
    _requests: list[data_review_pb2.CreateDataReviewRequest]
    _tags: list[str]
    _clients: DataReview._Clients = field(repr=False)

    def add_integration(self, integration_rid: str) -> DataReviewBuilder:
        self._integration_rids.append(integration_rid)
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
            asset: Instance or rid of the asset to run the checklist on within the Run
                NOTE: only required for multi-asset runs

        Returns:
            DataReviewBuilder instance to continue building a data review with

        Raises:
            ValueError: If the given run has multiple associated assets and no asset was specified, or
                if the run has an asset that differs from the provided asset.
        """
        checklist_rid = rid_from_instance_or_string(checklist)
        run_rid = rid_from_instance_or_string(run)
        asset_rid = None if asset is None else rid_from_instance_or_string(asset)

        raw_run = self._clients.run.get_run(self._clients.auth_header, run_rid)
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
        """
        data_reviews = _initiate_data_reviews(
            self._clients,
            self._requests,
            [
                data_review_pb2.NotificationConfiguration(integration_rid=c, tags=self._tags)
                for c in self._integration_rids
            ],
        )
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


def _initiate_data_reviews(
    clients: DataReview._Clients,
    requests: Sequence[data_review_pb2.CreateDataReviewRequest],
    notification_configurations: Sequence[data_review_pb2.NotificationConfiguration] = (),
) -> Sequence[DataReview]:
    """Initiate the requested data reviews and return them hydrated."""
    request = data_review_pb2.BatchInitiateRequest(
        requests=list(requests),
        notification_configurations=list(notification_configurations),
    )
    with translate_grpc_errors():
        rids = clients.datareview.BatchInitiate(request).rids
    return [_get_data_review(clients, rid) for rid in rids]


def _initiate_data_review(
    clients: DataReview._Clients, *, checklist_rid: str, run_rid: str, commit: str | None
) -> DataReview:
    """Initiate a single data review.

    Raises:
        RuntimeError: If the backend does not return exactly one review.
    """
    reviews = _initiate_data_reviews(
        clients,
        [data_review_pb2.CreateDataReviewRequest(checklist_rid=checklist_rid, run_rid=run_rid, commit=commit)],
    )
    if len(reviews) != 1:
        raise RuntimeError(f"Expected exactly one response from BatchInitiate, received {len(reviews)}")
    return reviews[0]


def _get_data_review_proto(clients: DataReview._Clients, rid: str) -> data_review_pb2.DataReview:
    with translate_grpc_errors():
        response = clients.datareview.GetDataReview(data_review_pb2.GetDataReviewRequest(data_review_rid=rid))
    return response.data_review


def _get_data_review(clients: DataReview._Clients, rid: str) -> DataReview:
    """The data review with the given rid.

    Raises:
        NominalNotFoundError: If no data review has that rid.
    """
    return DataReview._from_proto(clients, _get_data_review_proto(clients, rid))
