from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
import requests
from conjure_python_client import ConjureHTTPError

from tests.e2e._export import _wait_for_export


def _export_error(status: int = 400, name: str = "Default:InvalidArgument") -> ConjureHTTPError:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps({"errorName": name}).encode()
    return ConjureHTTPError(requests.HTTPError(response=response))


def test_export_waits_for_missing_table_then_empty_and_partial_data(monkeypatch):
    monkeypatch.setattr("tests.e2e._export.time.monotonic", lambda: 0)
    sleep = Mock()
    monkeypatch.setattr("tests.e2e._export.time.sleep", sleep)
    expected = [20.0, 21.0]
    export = Mock(side_effect=[_export_error(), [], [20.0], expected])

    assert _wait_for_export(export, expected_rows=2) is expected
    assert sleep.call_count == 3


@pytest.mark.parametrize(
    ("status", "name"),
    [
        (401, "Authorization:Unauthorized"),
        (403, "Authorization:Forbidden"),
        (400, "Compute:InvalidRange"),
        (500, "Default:Internal"),
    ],
)
def test_export_does_not_retry_unrelated_http_errors(monkeypatch, status, name):
    sleep = Mock()
    monkeypatch.setattr("tests.e2e._export.time.sleep", sleep)
    error = _export_error(status, name)

    with pytest.raises(ConjureHTTPError) as exc:
        _wait_for_export(Mock(side_effect=error), expected_rows=2)

    assert exc.value is error
    sleep.assert_not_called()


def test_export_preserves_html_bad_request(monkeypatch):
    sleep = Mock()
    monkeypatch.setattr("tests.e2e._export.time.sleep", sleep)
    response = requests.Response()
    response.status_code = 400
    response._content = b"<html>Bad Request</html>"
    error = ConjureHTTPError(requests.HTTPError(response=response))

    with pytest.raises(ConjureHTTPError) as exc:
        _wait_for_export(Mock(side_effect=error), expected_rows=2)

    assert exc.value is error
    assert "Bad Request" in str(exc.value)
    sleep.assert_not_called()


@pytest.mark.parametrize("rows", [[], [20.0], [20.0, 21.0, 22.0]])
def test_export_fails_if_row_count_never_matches(monkeypatch, rows):
    monkeypatch.setattr("tests.e2e._export.time.monotonic", Mock(side_effect=[0, 120]))

    with pytest.raises(AssertionError, match=f"expected 2 rows, got {len(rows)}"):
        _wait_for_export(lambda: rows, expected_rows=2)


def test_export_preserves_http_error_at_deadline(monkeypatch):
    monkeypatch.setattr("tests.e2e._export.time.monotonic", Mock(side_effect=[0, 120]))
    error = _export_error()

    with pytest.raises(ConjureHTTPError) as exc:
        _wait_for_export(Mock(side_effect=error), expected_rows=2)

    assert exc.value is error


def test_export_uses_configured_readiness_deadline(monkeypatch):
    monkeypatch.setattr("tests.e2e._export.time.monotonic", Mock(side_effect=[0, 120, 600]))
    sleep = Mock()
    monkeypatch.setattr("tests.e2e._export.time.sleep", sleep)
    export = Mock(return_value=[])

    with pytest.raises(AssertionError, match="expected 2 rows, got 0"):
        _wait_for_export(export, expected_rows=2, timeout_seconds=600)

    assert export.call_count == 2
    sleep.assert_called_once()
