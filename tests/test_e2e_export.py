"""Export readiness contracts, exercised without network requests or real sleeps."""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from conjure_python_client import ConjureHTTPError

from tests.e2e._export import wait_for_export


@dataclass
class FakeClock:
    """Advance elapsed time only when the polling helper sleeps."""

    elapsed: float = 0

    def now(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        self.elapsed += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def _export_error(status: int = 400, name: str = "Default:InvalidArgument") -> ConjureHTTPError:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps({"errorName": name}).encode()
    return ConjureHTTPError(requests.HTTPError(response=response))


def test_export_ready_on_first_read(clock):
    """A complete export returns immediately without consuming the polling budget."""
    expected = [20.0, 21.0]
    export = Mock(return_value=expected)

    assert wait_for_export(export, 2, now=clock.now, sleep=clock.sleep) is expected
    export.assert_called_once()
    assert clock.elapsed == 0


def test_export_waits_for_missing_table_then_empty_and_partial_data(clock):
    """Missing tables and incomplete exports are retried until all rows are visible."""
    expected = [20.0, 21.0]
    export = Mock(side_effect=[_export_error(), [], [20.0], expected])

    assert wait_for_export(export, 2, now=clock.now, sleep=clock.sleep) is expected
    assert clock.elapsed > 0


@pytest.mark.parametrize("columns", [["temperature"], ["relative_minutes", "temperature", "humidity"]])
def test_dataframe_waits_for_expected_column_names(clock, columns):
    """Correct row counts cannot hide missing or incorrectly named columns."""
    expected = pd.DataFrame({name: range(10) for name in columns})
    export = Mock(
        side_effect=[
            expected.iloc[:, :-1],
            expected.rename(columns={columns[0]: "wrong_channel"}),
            expected.iloc[:, ::-1],
        ]
    )

    result = wait_for_export(
        export,
        10,
        is_ready=lambda frame: sorted(frame.columns) == sorted(columns),
        now=clock.now,
        sleep=clock.sleep,
    )

    pd.testing.assert_frame_equal(result, expected.iloc[:, ::-1])
    assert export.call_count == 3


def test_dataframe_fails_when_columns_never_become_ready(clock):
    """A full row count with a missing column exhausts the readiness budget."""
    with pytest.raises(AssertionError, match="budget exhausted.*additional readiness check failed"):
        wait_for_export(
            lambda: pd.DataFrame({"temperature": range(10)}),
            10,
            is_ready=lambda frame: list(frame.columns) == ["temperature", "humidity"],
            timeout_seconds=5,
            now=clock.now,
            sleep=clock.sleep,
        )

    assert clock.elapsed == 5


@pytest.mark.parametrize(
    ("status", "name"),
    [
        (401, "Authorization:Unauthorized"),
        (403, "Authorization:Forbidden"),
        (400, "Compute:InvalidRange"),
        (500, "Default:Internal"),
    ],
)
def test_export_does_not_retry_unrelated_http_errors(clock, status, name):
    """Only the known missing-table HTTP error is eligible for retry."""
    error = _export_error(status, name)
    export = Mock(side_effect=error)

    with pytest.raises(ConjureHTTPError) as exc:
        wait_for_export(export, 2, now=clock.now, sleep=clock.sleep)

    assert exc.value is error
    export.assert_called_once()
    assert clock.elapsed == 0


def test_export_preserves_html_bad_request(clock):
    """An ingress HTML error is preserved even without a Conjure error_name attribute."""
    response = requests.Response()
    response.status_code = 400
    response._content = b"<html>Bad Request</html>"
    error = ConjureHTTPError(requests.HTTPError(response=response))

    with pytest.raises(ConjureHTTPError) as exc:
        wait_for_export(Mock(side_effect=error), 2, now=clock.now, sleep=clock.sleep)

    assert exc.value is error
    assert "Bad Request" in str(exc.value)
    assert clock.elapsed == 0


@pytest.mark.parametrize("rows", [[], [20.0]])
def test_export_fails_if_row_count_never_matches(clock, rows):
    """Incomplete data reports its last row count when the budget expires."""
    with pytest.raises(AssertionError, match=f"budget exhausted: expected 2 rows, got {len(rows)}"):
        wait_for_export(lambda: rows, 2, timeout_seconds=5, now=clock.now, sleep=clock.sleep)

    assert clock.elapsed == 5


def test_export_fails_immediately_on_excess_rows(clock):
    """Excess rows in the fixed dataset fail without consuming siblings' shared budget."""
    export = Mock(return_value=[20.0, 21.0, 22.0])

    with pytest.raises(AssertionError, match="excess rows: expected 2 rows, got 3"):
        wait_for_export(export, 2, now=clock.now, sleep=clock.sleep)

    export.assert_called_once()
    assert clock.elapsed == 0


def test_export_preserves_http_error_at_deadline(clock):
    """Retry exhaustion re-raises the original server error for diagnosis."""
    error = _export_error()

    with pytest.raises(ConjureHTTPError) as exc:
        wait_for_export(Mock(side_effect=error), 2, timeout_seconds=5, now=clock.now, sleep=clock.sleep)

    assert exc.value is error
    assert clock.elapsed == 5


def test_export_uses_configured_readiness_deadline(clock):
    """A caller's larger budget replaces the default timeout."""
    with pytest.raises(AssertionError, match="expected 2 rows, got 0"):
        wait_for_export(lambda: [], 2, timeout_seconds=600, now=clock.now, sleep=clock.sleep)

    assert clock.elapsed == 600


@pytest.mark.parametrize("rows", [[], [20.0, 21.0]])
def test_export_with_exhausted_shared_budget_reads_once(clock, rows):
    """Later callers can still read ready data, but cannot start another retry budget."""
    export = Mock(return_value=rows)

    if rows:
        assert wait_for_export(export, 2, timeout_seconds=0, now=clock.now, sleep=clock.sleep) is rows
    else:
        with pytest.raises(AssertionError, match="budget exhausted"):
            wait_for_export(export, 2, timeout_seconds=0, now=clock.now, sleep=clock.sleep)

    export.assert_called_once()
    assert clock.elapsed == 0
