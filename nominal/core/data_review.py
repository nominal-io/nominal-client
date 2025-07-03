from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from time import sleep
from typing import Protocol, Sequence

from nominal_api import (
    event as event_api,
)
from nominal_api import (
    scout,
    scout_api,
    scout_checklistexecution_api,
    scout_checks_api,
    scout_datareview_api,
    scout_integrations_api,
)
from typing_extensions import Self, deprecated

from nominal.core import checklist, event
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class DataReview(HasRid):
    rid: str
    run_rid: str
    checklist_rid: str
    checklist_commit: str
    completed: bool

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def datareview(self) -> scout_datareview_api.DataReviewService: ...
        @property
        def checklist(self) -> scout_checks_api.ChecklistService: ...
        @property
        def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...
        @property
        def event(self) -> event_api.EventService: ...
        @property
        def run(self) -> scout.RunService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, data_review: scout_datareview_api.DataReview) -> Self:
        executing_states = [
            check.state._pending_execution or check.state._executing for check in data_review.check_evaluations
        ]
        completed = not any(executing_states)
        return cls(
            rid=data_review.rid,
            run_rid=data_review.run_rid,
            checklist_rid=data_review.checklist_ref.rid,
            checklist_commit=data_review.checklist_ref.commit,
            completed=completed,
            _clients=clients,
        )

    def get_checklist(self) -> checklist.Checklist:
        return checklist.Checklist._from_conjure(
            self._clients,
            self._clients.checklist.get(self._clients.auth_header, self.checklist_rid, commit=self.checklist_commit),
        )

    @deprecated(
        "CheckViolations are deprecated and will be removed in a future version. "
        "Checklists now produce Events. Use get_events() instead."
    )
    def get_violations(self) -> Sequence[CheckViolation]:
        """Retrieves the list of check violations for the data review."""
        response = self._clients.datareview.get_check_alerts_for_data_review(self._clients.auth_header, self.rid)
        return [CheckViolation._from_conjure(alert) for alert in response]

    def get_events(self) -> Sequence[event.Event]:
        """Retrieves the list of events for the data review."""
        data_review_response = self._clients.datareview.get(self._clients.auth_header, self.rid).check_evaluations
        all_event_rids = [
            event_rid
            for check in data_review_response
            if check.state._generated_alerts
            for event_rid in check.state._generated_alerts.event_rids
        ]
        event_response = self._clients.event.batch_get_events(self._clients.auth_header, all_event_rids)
        return [event.Event._from_conjure(self._clients, data_review_event) for data_review_event in event_response]

    def reload(self) -> DataReview:
        """Reloads the data review from the server."""
        return DataReview._from_conjure(
            self._clients, self._clients.datareview.get(self._clients.auth_header, self.rid)
        )

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
        self._clients.datareview.archive_data_review(self._clients.auth_header, self.rid)

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Data Review in the Nominal app"""
        run = self._clients.run.get_run(self._clients.auth_header, self.run_rid)
        return f"{self._clients.app_base_url}/runs/{run.run_number}/?tab=checklists&openChecklistDetails={self.rid}&openCheckExecutionErrorReview="  # noqa: E501


@dataclass(frozen=True)
class CheckViolation:
    rid: str
    check_rid: str
    name: str
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    priority: checklist.Priority | None

    @classmethod
    def _from_conjure(cls, check_alert: scout_datareview_api.CheckAlert) -> CheckViolation:
        return cls(
            rid=check_alert.rid,
            check_rid=check_alert.check_rid,
            name=check_alert.name,
            start=_SecondsNanos.from_api(check_alert.start).to_nanoseconds(),
            end=_SecondsNanos.from_api(check_alert.end).to_nanoseconds() if check_alert.end is not None else None,
            priority=checklist._conjure_priority_to_priority(check_alert.priority)
            if check_alert.priority is not scout_api.Priority.UNKNOWN
            else None,
        )


@dataclass(frozen=True)
class DataReviewBuilder:
    _integration_rids: list[str]
    _requests: list[scout_datareview_api.CreateDataReviewRequest]
    _clients: DataReview._Clients = field(repr=False)

    def add_integration(self, integration_rid: str) -> DataReviewBuilder:
        self._integration_rids.append(integration_rid)
        return self

    def add_request(self, run_rid: str, checklist_rid: str, commit: str) -> DataReviewBuilder:
        self._requests.append(scout_datareview_api.CreateDataReviewRequest(checklist_rid, run_rid, commit))
        return self

    def initiate(self, wait_for_completion: bool = True) -> Sequence[DataReview]:
        """Initiates a batch data review process.

        Args:
            wait_for_completion (bool): If True, waits for the data review process to complete before returning.
                                        Default is True.
        """
        request = scout_datareview_api.BatchInitiateDataReviewRequest(
            notification_configurations=[
                scout_integrations_api.NotificationConfiguration(c, tags=[]) for c in self._integration_rids
            ],
            requests=self._requests,
        )
        response = self._clients.datareview.batch_initiate(self._clients.auth_header, request)

        data_reviews = [
            DataReview._from_conjure(self._clients, self._clients.datareview.get(self._clients.auth_header, rid))
            for rid in response.rids
        ]
        if wait_for_completion:
            return poll_until_completed(data_reviews)
        else:
            return data_reviews


def poll_until_completed(
    data_reviews: Sequence[DataReview], interval: timedelta = timedelta(seconds=2)
) -> Sequence[DataReview]:
    return [review.poll_for_completion(interval) for review in data_reviews]
