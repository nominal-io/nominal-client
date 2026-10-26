from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from nominal.core.client import NominalClient
from nominal.core.data_review import DataReview
from nominal.core.integration import Integration
from nominal.protos.datareview.v2 import data_review_pb2
from nominal.protos.integrations.v1 import integrations_pb2

PENDING = data_review_pb2.AutomaticCheckEvaluationState(pending_execution=data_review_pb2.PendingExecutionState())
EXECUTING = data_review_pb2.AutomaticCheckEvaluationState(executing=data_review_pb2.ExecutingState())
PASSING = data_review_pb2.AutomaticCheckEvaluationState(passing=data_review_pb2.PassingExecutionState())
ALERTED = data_review_pb2.AutomaticCheckEvaluationState(generated_alerts=data_review_pb2.GeneratedAlertsState())
UNSET = data_review_pb2.AutomaticCheckEvaluationState()


@pytest.mark.parametrize(
    ("states", "completed"),
    [
        pytest.param((), True, id="no-checks"),
        pytest.param((PASSING, ALERTED), True, id="all-settled"),
        pytest.param((UNSET,), True, id="unset-or-unknown-state"),
        pytest.param((PASSING, PENDING), False, id="pending"),
        pytest.param((EXECUTING,), False, id="executing"),
    ],
)
def test_completed_tracks_unsettled_check_evaluations(states, completed: bool) -> None:
    """A review is complete once no check is pending or executing; a state this client cannot read counts as done."""
    review = data_review_pb2.DataReview(
        check_evaluations=[data_review_pb2.AutomaticCheckEvaluation(state=state) for state in states]
    )

    assert DataReview._from_proto(MagicMock(), review).completed is completed


def test_notifications_preserve_order_duplicates_and_late_tags() -> None:
    """Notification destinations retain order, duplicates, and tags added later."""
    clients = MagicMock()
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse()
    builder = NominalClient(_clients=clients).data_review_builder()

    builder.add_integration("first")
    builder.add_integration("second")
    builder.add_integration("first")
    builder.add_tags(["campaign"])
    builder.initiate(wait_for_completion=False)

    request = clients.datareview.BatchInitiate.call_args.args[0]
    assert list(request.notification_configurations) == [
        data_review_pb2.NotificationConfiguration(integration_rid="first", tags=["campaign"]),
        data_review_pb2.NotificationConfiguration(integration_rid="second", tags=["campaign"]),
        data_review_pb2.NotificationConfiguration(integration_rid="first", tags=["campaign"]),
    ]


def test_reload_warns_and_refreshes_in_place() -> None:
    """Deprecated reload warns at the caller and returns the same updated instance."""
    clients = MagicMock()
    original = data_review_pb2.DataReview(rid="review", asset_rid="first")
    updated = data_review_pb2.DataReview(rid="review", asset_rid="second")
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(data_review=updated)
    review = DataReview._from_proto(clients, original)

    with pytest.warns(DeprecationWarning, match=r"Use DataReview.refresh\(\)") as warnings:
        reloaded = review.reload()

    assert reloaded is review
    assert review.asset_rid == "second"
    assert len(warnings) == 1
    assert warnings[0].filename == __file__
    assert clients.datareview.GetDataReview.call_count == 1


def test_poll_for_completion_refreshes_without_deprecation_warning() -> None:
    """Polling refreshes the same review without calling the deprecated reload method."""
    clients = MagicMock()
    pending = data_review_pb2.DataReview(
        rid="review", check_evaluations=[data_review_pb2.AutomaticCheckEvaluation(state=PENDING)]
    )
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(
        data_review=data_review_pb2.DataReview(rid="review", asset_rid="asset")
    )
    review = DataReview._from_proto(clients, pending)

    assert review.poll_for_completion(interval=timedelta()) is review
    assert review.completed
    assert review.asset_rid == "asset"


def test_notification_accepts_sdk_resources() -> None:
    """Integration instances are normalized to their RIDs without a lookup at the notification boundary."""
    clients = MagicMock()
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse()
    integration = Integration._from_proto(clients, integrations_pb2.Integration(rid="integration"))
    builder = NominalClient(_clients=clients).data_review_builder()

    builder.add_integration(integration).initiate(wait_for_completion=False)

    notifications = clients.datareview.BatchInitiate.call_args.args[0].notification_configurations
    assert list(notifications) == [data_review_pb2.NotificationConfiguration(integration_rid="integration")]
    clients.integrations.GetIntegration.assert_not_called()
