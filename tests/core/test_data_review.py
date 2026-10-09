from __future__ import annotations

from typing import Sequence
from unittest.mock import MagicMock

import pytest
from google.protobuf import timestamp_pb2

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.checklist import Checklist
from nominal.core.client import NominalClient
from nominal.core.data_review import DataReview
from nominal.protos.datareview.v2 import data_review_pb2
from nominal.protos.event.v2 import event_pb2
from nominal.protos.run.v1 import run_service_pb2
from nominal.protos.types import types_pb2

PENDING = data_review_pb2.AutomaticCheckEvaluationState(pending_execution=data_review_pb2.PendingExecutionState())
EXECUTING = data_review_pb2.AutomaticCheckEvaluationState(executing=data_review_pb2.ExecutingState())
PASSING = data_review_pb2.AutomaticCheckEvaluationState(passing=data_review_pb2.PassingExecutionState())
ALERTED = data_review_pb2.AutomaticCheckEvaluationState(
    generated_alerts=data_review_pb2.GeneratedAlertsState(event_rids=["ri.event.1"])
)
UNSET = data_review_pb2.AutomaticCheckEvaluationState()


def _proto_review(
    *, created_by: str = "", states: Sequence[data_review_pb2.AutomaticCheckEvaluationState] = ()
) -> data_review_pb2.DataReview:
    return data_review_pb2.DataReview(
        rid="ri.datareview.1",
        run_rid="ri.run.1",
        created_by=created_by,
        created_at=timestamp_pb2.Timestamp(seconds=1),
        checklist_ref=data_review_pb2.PinnedChecklistRef(rid="ri.checklist.1", commit="abc"),
        check_evaluations=[
            data_review_pb2.AutomaticCheckEvaluation(rid="ri.eval.1", check_rid="ri.check.1", state=state)
            for state in states
        ],
    )


def _get_response(
    *, states: Sequence[data_review_pb2.AutomaticCheckEvaluationState] = ()
) -> data_review_pb2.GetDataReviewResponse:
    return data_review_pb2.GetDataReviewResponse(data_review=_proto_review(states=states))


@pytest.fixture
def clients():
    return MagicMock()


@pytest.fixture
def checklist(clients):
    return Checklist(rid="ri.checklist.1", name="checks", description="", properties={}, labels=[], _clients=clients)


@pytest.fixture
def batch_initiate(clients):
    """BatchInitiate returning one rid, with GetDataReview stubbed for the hydration that follows."""
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["ri.datareview.1"])
    clients.datareview.GetDataReview.return_value = _get_response()
    return clients.datareview.BatchInitiate


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
    assert DataReview._from_proto(MagicMock(), _proto_review(states=states)).completed is completed


@pytest.mark.parametrize(("created_by", "expected"), [("", None), ("ri.user.1", "ri.user.1")])
def test_from_proto_normalizes_empty_created_by(created_by: str, expected: str | None) -> None:
    """created_by is a plain proto string, so an unset value arrives as "" and must read as None."""
    assert DataReview._from_proto(MagicMock(), _proto_review(created_by=created_by)).created_by_rid == expected


def test_get_events_collects_rids_from_alerting_checks_only(clients) -> None:
    """Only checks in the generated-alerts state carry event rids; other states contribute none."""
    clients.datareview.GetDataReview.return_value = _get_response(states=(PASSING, ALERTED))
    clients.event.BatchGetEvents.return_value = event_pb2.BatchGetEventsResponse(events=[])

    DataReview._from_proto(clients, _proto_review()).get_events()

    assert list(clients.event.BatchGetEvents.call_args.args[0].event_rids) == ["ri.event.1"]


def test_reload_returns_a_new_snapshot_and_leaves_the_original_unchanged(clients) -> None:
    """reload() fetches the review again by rid and returns a new snapshot rather than mutating this one."""
    clients.datareview.GetDataReview.return_value = _get_response()
    review = NominalClient(_clients=clients).get_data_review("ri.datareview.1")
    clients.datareview.GetDataReview.return_value = _get_response(states=(PENDING,))

    reloaded = review.reload()

    request = clients.datareview.GetDataReview.call_args.args[0]
    assert request == data_review_pb2.GetDataReviewRequest(data_review_rid="ri.datareview.1")
    assert (review.completed, reloaded.completed) == (True, False)


def test_search_wraps_archived_statuses_in_a_set_message(clients) -> None:
    """Unlike the other gRPC searches, FindDataReviews takes archived statuses wrapped in ArchivedStatusSet."""
    clients.datareview.FindDataReviews.return_value = data_review_pb2.FindDataReviewsResponse()

    NominalClient(_clients=clients).search_data_reviews(
        assets=["ri.asset.1"], archive_status=ArchiveStatusFilter.ARCHIVED
    )

    request = clients.datareview.FindDataReviews.call_args.args[0]
    assert list(request.archived_statuses.values) == [types_pb2.ArchivedStatus.ARCHIVED]
    assert list(request.asset_rids) == ["ri.asset.1"]


@pytest.mark.parametrize("commit", [None, "abc"])
def test_checklist_execute_requests_one_review_of_the_run(checklist, batch_initiate, commit: str | None) -> None:
    """execute() requests one review of the run, and an unpinned commit stays absent rather than empty."""
    review = checklist.execute("ri.run.1", commit=commit)

    assert review.rid == "ri.datareview.1"
    assert list(batch_initiate.call_args.args[0].requests) == [
        data_review_pb2.CreateDataReviewRequest(checklist_rid="ri.checklist.1", run_rid="ri.run.1", commit=commit)
    ]


def test_checklist_execute_rejects_a_batch_that_is_not_exactly_one(clients, checklist, batch_initiate) -> None:
    """A batch that does not yield one review is a protocol violation, not a silent pick-first."""
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["a", "b"])

    with pytest.raises(RuntimeError, match="Expected exactly one response from BatchInitiate"):
        checklist.execute("ri.run.1")


@pytest.mark.parametrize(("asset", "commit"), [(None, None), ("ri.asset.1", "abc")])
def test_builder_leaves_omitted_asset_and_commit_absent(
    clients, batch_initiate, asset: str | None, commit: str | None
) -> None:
    """An omitted asset or commit stays absent from the request rather than empty, keeping the backend defaults."""
    clients.run.GetRun.return_value = run_service_pb2.GetRunResponse(run=run_service_pb2.Run(assets=["ri.asset.1"]))
    builder = NominalClient(_clients=clients).data_review_builder()

    builder.execute_checklist("ri.run.1", "ri.checklist.1", asset=asset, commit=commit).initiate(
        wait_for_completion=False
    )

    assert list(batch_initiate.call_args.args[0].requests) == [
        data_review_pb2.CreateDataReviewRequest(
            run_rid="ri.run.1", checklist_rid="ri.checklist.1", asset_rid=asset, commit=commit
        )
    ]


def test_builder_fans_tags_across_every_integration(clients) -> None:
    """Each integration rid becomes its own NotificationConfiguration carrying the full tag list."""
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=[])
    builder = NominalClient(_clients=clients).data_review_builder().add_tags(["tag-a", "tag-b"])
    builder.add_integration("ri.integration.1").add_integration("ri.integration.2")

    builder.initiate(wait_for_completion=False)

    assert list(clients.datareview.BatchInitiate.call_args.args[0].notification_configurations) == [
        data_review_pb2.NotificationConfiguration(integration_rid="ri.integration.1", tags=["tag-a", "tag-b"]),
        data_review_pb2.NotificationConfiguration(integration_rid="ri.integration.2", tags=["tag-a", "tag-b"]),
    ]
