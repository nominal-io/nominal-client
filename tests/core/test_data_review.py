from __future__ import annotations

from typing import Sequence
from unittest.mock import MagicMock

import pytest
from google.protobuf import timestamp_pb2

from nominal.core._checklist_types import Priority
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.checklist import Checklist
from nominal.core.client import NominalClient
from nominal.core.data_review import CheckViolation, DataReview, DataReviewBuilder, _iter_search_data_reviews
from nominal.protos.datareview.v2 import data_review_pb2
from nominal.protos.event.v2 import event_pb2
from nominal.protos.types import common_pb2, types_pb2

PENDING = data_review_pb2.AutomaticCheckEvaluationState(pending_execution=data_review_pb2.PendingExecutionState())
EXECUTING = data_review_pb2.AutomaticCheckEvaluationState(executing=data_review_pb2.ExecutingState())
PASSING = data_review_pb2.AutomaticCheckEvaluationState(passing=data_review_pb2.PassingExecutionState())
ALERTED = data_review_pb2.AutomaticCheckEvaluationState(
    generated_alerts=data_review_pb2.GeneratedAlertsState(event_rids=["ri.event.1"])
)


def _evaluation(state: data_review_pb2.AutomaticCheckEvaluationState) -> data_review_pb2.AutomaticCheckEvaluation:
    return data_review_pb2.AutomaticCheckEvaluation(
        rid="ri.eval.1", check_rid="ri.check.1", data_review_rid="ri.datareview.1", state=state
    )


def _proto_review(
    rid: str = "ri.datareview.1",
    *,
    created_by: str = "",
    states: Sequence[data_review_pb2.AutomaticCheckEvaluationState] = (),
) -> data_review_pb2.DataReview:
    return data_review_pb2.DataReview(
        rid=rid,
        run_rid="ri.run.1",
        created_by=created_by,
        created_at=timestamp_pb2.Timestamp(seconds=1),
        checklist_ref=data_review_pb2.PinnedChecklistRef(rid="ri.checklist.1", commit="abc"),
        check_evaluations=[_evaluation(state) for state in states],
    )


def _get_response(rid: str = "ri.datareview.1") -> data_review_pb2.GetDataReviewResponse:
    return data_review_pb2.GetDataReviewResponse(data_review=_proto_review(rid))


@pytest.fixture
def clients():
    return MagicMock()


@pytest.fixture
def review(clients):
    return DataReview._from_proto(clients, _proto_review())


@pytest.fixture
def checklist(clients):
    return Checklist(rid="ri.checklist.1", name="checks", description="", properties={}, labels=[], _clients=clients)


@pytest.fixture
def initiate(clients):
    """BatchInitiate returning one rid, with GetDataReview stubbed for the hydration that follows."""
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["ri.datareview.1"])
    clients.datareview.GetDataReview.return_value = _get_response()
    return clients.datareview.BatchInitiate


@pytest.mark.parametrize(
    ("states", "completed"),
    [
        pytest.param((), True, id="no-checks"),
        pytest.param((PASSING, ALERTED), True, id="all-settled"),
        pytest.param((PENDING,), False, id="pending"),
        pytest.param((EXECUTING,), False, id="executing"),
    ],
)
def test_completed_tracks_unsettled_check_evaluations(states, completed: bool) -> None:
    """Completion reads the evaluation-state oneof, so a check still running must keep the review incomplete."""
    assert DataReview._from_proto(MagicMock(), _proto_review(states=states)).completed is completed


@pytest.mark.parametrize(("created_by", "expected"), [("", None), ("ri.user.1", "ri.user.1")])
def test_from_proto_normalizes_empty_created_by(created_by: str, expected: str | None) -> None:
    """created_by is a plain proto string, so an unset value arrives as "" and must read as None."""
    assert DataReview._from_proto(MagicMock(), _proto_review(created_by=created_by)).created_by_rid == expected


def test_get_events_collects_rids_from_alerting_checks_only(clients, review) -> None:
    """Only checks in the generated-alerts state carry event rids; other states contribute none."""
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(
        data_review=_proto_review(states=(PASSING, ALERTED))
    )
    clients.event.BatchGetEvents.return_value = event_pb2.BatchGetEventsResponse(events=[])

    review.get_events()

    assert list(clients.event.BatchGetEvents.call_args.args[0].event_rids) == ["ri.event.1"]


def test_initiate_fans_tags_across_every_integration(clients) -> None:
    """Each integration rid becomes its own NotificationConfiguration carrying the full tag list."""
    builder = DataReviewBuilder(["ri.integration.1", "ri.integration.2"], [], ["tag-a", "tag-b"], _clients=clients)
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=[])

    builder.initiate(wait_for_completion=False)

    request = clients.datareview.BatchInitiate.call_args.args[0]
    assert [(c.integration_rid, list(c.tags)) for c in request.notification_configurations] == [
        ("ri.integration.1", ["tag-a", "tag-b"]),
        ("ri.integration.2", ["tag-a", "tag-b"]),
    ]


def test_search_wraps_archived_statuses_in_a_set_message(clients) -> None:
    """Unlike the other gRPC searches, find takes archived statuses wrapped in ArchivedStatusSet."""
    clients.datareview.FindDataReviews.return_value = data_review_pb2.FindDataReviewsResponse()

    list(_iter_search_data_reviews(clients, assets=["ri.asset.1"], archive_status=ArchiveStatusFilter.ARCHIVED))

    request = clients.datareview.FindDataReviews.call_args.args[0]
    assert list(request.archived_statuses.values) == [types_pb2.ArchivedStatus.ARCHIVED]
    assert list(request.asset_rids) == ["ri.asset.1"]


def test_checklist_execute_pins_the_checklist_run_and_commit(checklist, initiate) -> None:
    """execute() initiates exactly one review, and an unpinned commit stays absent rather than empty."""
    assert checklist.execute("ri.run.1", commit="abc").rid == "ri.datareview.1"

    request = initiate.call_args.args[0].requests[0]
    assert (request.checklist_rid, request.run_rid, request.commit) == ("ri.checklist.1", "ri.run.1", "abc")

    checklist.execute("ri.run.1")
    assert not initiate.call_args.args[0].requests[0].HasField("commit")


def test_checklist_execute_rejects_a_batch_that_is_not_exactly_one(clients, checklist, initiate) -> None:
    """A batch that does not yield one review is a protocol violation, not a silent pick-first."""
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["a", "b"])

    with pytest.raises(RuntimeError, match="Expected exactly one response from BatchInitiate"):
        checklist.execute("ri.run.1")


@pytest.mark.parametrize(
    ("wire_value", "expected"),
    [
        pytest.param(common_pb2.P0, Priority.P0, id="P0"),
        pytest.param(common_pb2.P1, Priority.P1, id="P1"),
        pytest.param(common_pb2.P2, Priority.P2, id="P2"),
        pytest.param(common_pb2.P3, Priority.P3, id="P3"),
        pytest.param(common_pb2.P4, Priority.P4, id="P4"),
        pytest.param(common_pb2.PRIORITY_UNSPECIFIED, None, id="unspecified"),
        pytest.param(99, None, id="a-level-added-after-this-client-was-built"),
    ],
)
def test_priority_maps_from_the_wire(wire_value: common_pb2.Priority.ValueType, expected: Priority | None) -> None:
    """Inbound conversion has no exhaustiveness check, so every level is pinned here.

    A value this client cannot name degrades to None rather than raising, matching the conjure transport,
    whose decoder collapsed unrecognized values into `Priority.UNKNOWN`.
    """
    assert Priority._from_proto(wire_value) is expected


def test_check_violation_reads_optional_end_and_priority() -> None:
    """An open violation has no end timestamp, and an unspecified priority is absence rather than a level."""
    open_alert = data_review_pb2.CheckAlert(
        rid="ri.alert.1", check_rid="ri.check.1", name="n", start=timestamp_pb2.Timestamp(seconds=5)
    )

    violation = CheckViolation._from_proto(open_alert)

    assert violation.start == 5_000_000_000
    assert violation.end is None
    assert violation.priority is None


def test_get_data_review_returns_a_hydrated_review(clients) -> None:
    """The getter hydrates, so callers do not repeat _from_proto at every site."""
    clients.datareview.GetDataReview.return_value = _get_response()

    assert isinstance(NominalClient(_clients=clients).get_data_review("ri.datareview.1"), DataReview)
    assert clients.datareview.GetDataReview.call_args.args[0].data_review_rid == "ri.datareview.1"
