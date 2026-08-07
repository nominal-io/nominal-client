from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf import timestamp_pb2

from nominal.core._checklist_types import Priority
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.checklist import Checklist
from nominal.core.client import NominalClient
from nominal.core.data_review import CheckViolation, DataReview, DataReviewBuilder, _iter_search_data_reviews
from nominal.core.exceptions import NominalNotFoundError
from nominal.protos.datareview.v2 import data_review_pb2
from nominal.protos.types import common_pb2, types_pb2


def _evaluation(**state_kwargs: object) -> data_review_pb2.AutomaticCheckEvaluation:
    return data_review_pb2.AutomaticCheckEvaluation(
        rid="ri.eval.1",
        check_rid="ri.check.1",
        data_review_rid="ri.datareview.1",
        state=data_review_pb2.AutomaticCheckEvaluationState(**state_kwargs),  # type: ignore[arg-type]
    )


def _proto_review(rid: str = "ri.datareview.1", **kwargs: object) -> data_review_pb2.DataReview:
    defaults: dict[str, object] = {
        "run_rid": "ri.run.1",
        "created_at": timestamp_pb2.Timestamp(seconds=1),
        "checklist_ref": data_review_pb2.PinnedChecklistRef(rid="ri.checklist.1", commit="abc"),
    }
    defaults.update(kwargs)
    return data_review_pb2.DataReview(rid=rid, **defaults)  # type: ignore[arg-type]


def test_completed_is_false_while_any_check_is_pending_or_executing() -> None:
    """A review is only complete once no check evaluation is still pending or executing."""
    pending = _proto_review(check_evaluations=[_evaluation(pending_execution=data_review_pb2.PendingExecutionState())])
    executing = _proto_review(check_evaluations=[_evaluation(executing=data_review_pb2.ExecutingState())])

    assert DataReview._from_proto(MagicMock(), pending).completed is False
    assert DataReview._from_proto(MagicMock(), executing).completed is False


def test_completed_is_true_when_every_check_has_settled() -> None:
    """Passing and generated-alerts states are terminal, so the review reads as complete."""
    settled = _proto_review(
        check_evaluations=[
            _evaluation(passing=data_review_pb2.PassingExecutionState()),
            _evaluation(generated_alerts=data_review_pb2.GeneratedAlertsState(event_rids=["ri.event.1"])),
        ]
    )

    assert DataReview._from_proto(MagicMock(), settled).completed is True


def test_completed_is_true_when_there_are_no_checks() -> None:
    """No evaluations means nothing is still running."""
    assert DataReview._from_proto(MagicMock(), _proto_review()).completed is True


def test_from_proto_maps_empty_created_by_to_none() -> None:
    """An unset created_by arrives as an empty string and must read as None."""
    assert DataReview._from_proto(MagicMock(), _proto_review()).created_by_rid is None
    assert DataReview._from_proto(MagicMock(), _proto_review(created_by="ri.user.1")).created_by_rid == "ri.user.1"


def test_get_events_collects_rids_from_generated_alerts_only() -> None:
    """Only checks in the generated-alerts state carry event rids; other states contribute none."""
    clients = MagicMock()
    review = DataReview._from_proto(clients, _proto_review())
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(
        data_review=_proto_review(
            check_evaluations=[
                _evaluation(passing=data_review_pb2.PassingExecutionState()),
                _evaluation(generated_alerts=data_review_pb2.GeneratedAlertsState(event_rids=["ri.event.1"])),
            ]
        )
    )
    clients.event.batch_get_events.return_value = []

    review.get_events()

    assert clients.event.batch_get_events.call_args.args[1] == ["ri.event.1"]


def test_get_data_review_translates_not_found(fake_rpc_error) -> None:
    """A NOT_FOUND status surfaces as NominalNotFoundError, not grpc.RpcError."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.datareview.GetDataReview.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        client.get_data_review("ri.datareview.missing")


def test_archive_addresses_this_review() -> None:
    """archive() addresses exactly this review."""
    clients = MagicMock()
    review = DataReview._from_proto(clients, _proto_review())

    review.archive()

    assert clients.datareview.ArchiveDataReview.call_args.args[0].data_review_rid == review.rid


def test_check_violation_maps_unspecified_priority_to_none() -> None:
    """An unspecified priority is absence, not a P-level."""
    unspecified = data_review_pb2.CheckAlert(rid="ri.alert.1", check_rid="ri.check.1", name="n")
    prioritized = data_review_pb2.CheckAlert(rid="ri.alert.1", check_rid="ri.check.1", name="n", priority=common_pb2.P2)

    assert CheckViolation._from_proto(unspecified).priority is None
    assert CheckViolation._from_proto(prioritized).priority is Priority.P2


def test_check_violation_maps_absent_end_to_none() -> None:
    """An open violation has no end timestamp."""
    open_alert = data_review_pb2.CheckAlert(
        rid="ri.alert.1", check_rid="ri.check.1", name="n", start=timestamp_pb2.Timestamp(seconds=5)
    )

    violation = CheckViolation._from_proto(open_alert)

    assert violation.start == 5 * 1_000_000_000
    assert violation.end is None


@pytest.mark.parametrize(
    ("proto_priority", "expected"),
    [
        (common_pb2.P0, Priority.P0),
        (common_pb2.P1, Priority.P1),
        (common_pb2.P2, Priority.P2),
        (common_pb2.P3, Priority.P3),
        (common_pb2.P4, Priority.P4),
        (common_pb2.PRIORITY_UNSPECIFIED, None),
    ],
)
def test_priority_from_proto_maps_each_value(proto_priority: int, expected: Priority | None) -> None:
    """Inbound conversion has no exhaustiveness check, so every level is pinned here."""
    assert Priority._from_proto(proto_priority) is expected


def test_initiate_fans_tags_across_every_integration() -> None:
    """Each integration rid becomes its own NotificationConfiguration carrying the full tag list."""
    clients = MagicMock()
    builder = DataReviewBuilder(["ri.integration.1", "ri.integration.2"], [], ["tag-a", "tag-b"], _clients=clients)
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=[])

    builder.initiate(wait_for_completion=False)

    request = clients.datareview.BatchInitiate.call_args.args[0]
    assert [(c.integration_rid, list(c.tags)) for c in request.notification_configurations] == [
        ("ri.integration.1", ["tag-a", "tag-b"]),
        ("ri.integration.2", ["tag-a", "tag-b"]),
    ]


def test_initiate_hydrates_each_returned_rid() -> None:
    """Every rid BatchInitiate returns is fetched back and hydrated into a DataReview."""
    clients = MagicMock()
    builder = DataReviewBuilder([], [], [], _clients=clients)
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["ri.datareview.1"])
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(
        data_review=_proto_review("ri.datareview.1")
    )

    reviews = builder.initiate(wait_for_completion=False)

    assert [review.rid for review in reviews] == ["ri.datareview.1"]


def test_search_wraps_archived_statuses_in_a_set_message() -> None:
    """Unlike the other grpc searches, find takes archived statuses wrapped in ArchivedStatusSet."""
    clients = MagicMock()
    clients.datareview.FindDataReviews.return_value = data_review_pb2.FindDataReviewsResponse()

    list(_iter_search_data_reviews(clients, assets=["ri.asset.1"], archive_status=ArchiveStatusFilter.ARCHIVED))

    request = clients.datareview.FindDataReviews.call_args.args[0]
    assert list(request.archived_statuses.values) == [types_pb2.ArchivedStatus.ARCHIVED]
    assert list(request.asset_rids) == ["ri.asset.1"]


def _checklist(clients: MagicMock) -> Checklist:
    return Checklist(
        rid="ri.checklist.1",
        name="checks",
        description="",
        properties={},
        labels=[],
        _clients=clients,
    )


def test_checklist_execute_sends_the_checklist_and_run() -> None:
    """execute() initiates a single review pinned to this checklist and the given run."""
    clients = MagicMock()
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["ri.datareview.1"])
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(
        data_review=_proto_review("ri.datareview.1")
    )

    review = _checklist(clients).execute("ri.run.1", commit="abc")

    request = clients.datareview.BatchInitiate.call_args.args[0]
    assert len(request.requests) == 1
    assert request.requests[0].checklist_rid == "ri.checklist.1"
    assert request.requests[0].run_rid == "ri.run.1"
    assert request.requests[0].commit == "abc"
    assert review.rid == "ri.datareview.1"


def test_checklist_execute_omits_commit_when_not_pinned() -> None:
    """Commit has explicit presence, so omitting it means 'latest' rather than an empty commit hash."""
    clients = MagicMock()
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["ri.datareview.1"])
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(
        data_review=_proto_review("ri.datareview.1")
    )

    _checklist(clients).execute("ri.run.1")

    assert not clients.datareview.BatchInitiate.call_args.args[0].requests[0].HasField("commit")


def test_checklist_execute_raises_when_the_backend_returns_the_wrong_count() -> None:
    """A batch that does not yield exactly one review is a protocol violation, not a silent pick-first."""
    clients = MagicMock()
    clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse(rids=["a", "b"])
    clients.datareview.GetDataReview.return_value = data_review_pb2.GetDataReviewResponse(
        data_review=_proto_review("ri.datareview.1")
    )

    with pytest.raises(RuntimeError, match="Expected exactly one response from BatchInitiate"):
        _checklist(clients).execute("ri.run.1")
