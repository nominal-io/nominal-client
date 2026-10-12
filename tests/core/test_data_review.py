from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.data_review import DataReview
from nominal.protos.datareview.v2 import data_review_pb2

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
