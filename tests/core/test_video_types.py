from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from conjure_python_client import ConjureEncoder

from nominal.core._video_types import _scale_parameter
from nominal.core.exceptions import NominalVideoScaleModeError

_END = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def test_scale_parameter_is_none_when_nothing_given() -> None:
    """A video with no rate adjustment carries no scale parameter."""
    assert _scale_parameter() is None


@pytest.mark.parametrize(
    ("kwargs", "expected_document"),
    [
        pytest.param(
            {"true_frame_rate": 59.94},
            {"type": "trueFrameRate", "trueFrameRate": 59.94},
            id="true-frame-rate",
        ),
        pytest.param(
            {"scale_factor": 2.0},
            {"type": "scaleFactor", "scaleFactor": 2.0},
            id="scale-factor",
        ),
        pytest.param(
            {"ending_timestamp": _END},
            {"type": "endingTimestamp", "endingTimestamp": {"seconds": 1785412800, "nanos": 0}},
            id="ending-timestamp",
        ),
        pytest.param(
            {"ending_timestamp": "2026-07-30T12:00:00Z"},
            {"type": "endingTimestamp", "endingTimestamp": {"seconds": 1785412800, "nanos": 0}},
            id="ending-timestamp-as-iso-string",
        ),
        pytest.param(
            {"ending_timestamp": 1785412800_000000000},
            {"type": "endingTimestamp", "endingTimestamp": {"seconds": 1785412800, "nanos": 0}},
            id="ending-timestamp-as-epoch-nanos",
        ),
    ],
)
def test_scale_parameter_sets_only_the_arm_it_was_given(
    kwargs: dict[str, Any], expected_document: dict[str, Any]
) -> None:
    """Each form populates its own union arm; comparing the encoded document proves the others are unset."""
    assert ConjureEncoder.do_encode(_scale_parameter(**kwargs)) == expected_document


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"true_frame_rate": 30.0, "scale_factor": 2.0}, id="rate-and-factor"),
        pytest.param({"ending_timestamp": _END, "scale_factor": 2.0}, id="end-and-factor"),
        pytest.param({"ending_timestamp": _END, "true_frame_rate": 30.0}, id="end-and-rate"),
        pytest.param({"ending_timestamp": _END, "true_frame_rate": 30.0, "scale_factor": 2.0}, id="all-three"),
    ],
)
def test_scale_parameter_rejects_more_than_one_argument(kwargs: dict[str, Any]) -> None:
    """The three forms are mutually exclusive: they describe the same quantity three ways."""
    with pytest.raises(NominalVideoScaleModeError, match="at most one of"):
        _scale_parameter(**kwargs)


def test_scale_mode_error_is_also_a_value_error() -> None:
    """This raised a bare ValueError before the type existed, so `except ValueError` still works."""
    with pytest.raises(ValueError, match="at most one of"):
        _scale_parameter(true_frame_rate=30.0, scale_factor=2.0)
