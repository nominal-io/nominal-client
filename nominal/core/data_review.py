from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from time import sleep
from typing import Protocol, Sequence

from typing_extensions import Self

from nominal._api.scout_service_api import scout_checks_api, scout_datareview_api, scout_integrations_api
from nominal.core import checklist
from nominal.core._clientsbunch import HasAuthHeader
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

    class _Clients(HasAuthHeader, Protocol):
        @property
        def datareview(self) -> scout_datareview_api.DataReviewService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, data_review: scout_datareview_api.DataReview) -> Self:
        executing_states = [
            check.automatic_check.state._pending_execution or check.automatic_check.state._executing
            for check in data_review.checklist.checks
            if check.automatic_check
        ]
        completed = not any(executing_states)
        return cls(
            rid=data_review.rid,
            run_rid=data_review.run_rid,
            checklist_rid=data_review.checklist.checklist.rid,
            checklist_commit=data_review.checklist.checklist.commit,
            completed=completed,
            _clients=clients,
        )

    def get_violations(self) -> Sequence[CheckViolation]:
        """Retrieves the list of check violations for the data review."""
        response = self._clients.datareview.get_check_alerts_for_data_review(self._clients.auth_header, self.rid)
        return [CheckViolation._from_conjure(alert) for alert in response]


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
            if check_alert.priority is not scout_checks_api.Priority.UNKNOWN
            else None,
        )


@dataclass(frozen=True)
class DataReviewBatchBuilder:
    notification_configurations: list[str]
    _requests: list[scout_datareview_api.CreateDataReviewRequest]
    _clients: DataReview._Clients = field(repr=False)

    def add_notification_configuration(self, notification_configuration: str) -> DataReviewBatchBuilder:
        self.notification_configurations.append(notification_configuration)
        return self

    def add_request(self, run_rid: str, checklist_rid: str, commit: str) -> DataReviewBatchBuilder:
        self._requests.append(scout_datareview_api.CreateDataReviewRequest(checklist_rid, run_rid, commit))
        return self

    def initiate(
        self, wait_for_completion: bool = True, wait_timeout: timedelta = timedelta(minutes=1)
    ) -> Sequence[DataReview]:
        """Initiates a batch data review process.

        Args:
            wait_for_completion (bool): If True, waits for the data review process to complete before returning.
                                        Default is True.
            wait_timeout (timedelta): The maximum time to wait for the data review process to complete.
                                      Default is 1 minute.

        Raises:
            TimeoutError: If the data review process does not complete before the wait_timeout.
        """
        request = scout_datareview_api.BatchInitiateDataReviewRequest(
            notification_configurations=[
                scout_integrations_api.NotificationConfiguration(c) for c in self.notification_configurations
            ],
            requests=self._requests,
        )
        response = self._clients.datareview.batch_initiate(self._clients.auth_header, request)

        if not wait_for_completion:
            return [
                DataReview._from_conjure(self._clients, self._clients.datareview.get(self._clients.auth_header, rid))
                for rid in response.rids
            ]

        started = datetime.now()
        completed_review_rids = []
        completed_reviews = []
        while datetime.now() - started <= timedelta(seconds=wait_timeout.total_seconds()):
            sleep(2)
            for rid in response.rids:
                if rid not in completed_review_rids:
                    review_response = self._clients.datareview.get(self._clients.auth_header, rid)
                    review = DataReview._from_conjure(self._clients, review_response)
                    if review.completed:
                        completed_review_rids.append(rid)
                        completed_reviews.append(review)
            if len(completed_reviews) == len(response.rids):
                return completed_reviews

        raise TimeoutError(f"Data review initiation did not complete before wait_timeout. Review rids: {response.rids}")
