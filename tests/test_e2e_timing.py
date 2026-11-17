from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest_plugins = ["pytester"]


def test_phase_timings_survive_process_exit(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    """Completed phases and the active phase remain in output when pytest exits without teardown."""
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1]))
    pytester.makepyfile("""
        import os
        import pytest

        def test_pass():
            pass

        @pytest.fixture
        def broken():
            raise RuntimeError("setup failed")

        def test_setup_error(broken):
            pass

        def test_exit():
            os._exit(7)
    """)
    normal = pytester.runpytest_subprocess("-q", "--capture=fd")
    assert normal.ret == 7
    assert not any("E2E_TIMING " in line for line in normal.stdout.lines)
    result = pytester.runpytest_subprocess("-p", "tests.e2e_timing", "-q", "--capture=fd")
    assert result.ret == 7
    records = [json.loads(line.split("E2E_TIMING ", 1)[1]) for line in result.stdout.lines if "E2E_TIMING " in line]
    passed = [r for r in records if r["nodeid"].endswith("::test_pass")]
    assert [(r["phase"], r["event"]) for r in passed] == [
        (phase, event) for phase in ("setup", "call", "teardown") for event in ("start", "finish")
    ]
    assert all(r["duration_seconds"] >= 0 and r["outcome"] == "passed" for r in passed if r["event"] == "finish")
    failed = [r for r in records if r["nodeid"].endswith("::test_setup_error")]
    assert any(r["phase"] == "setup" and r.get("outcome") == "failed" for r in failed)
    assert not any(r["phase"] == "call" for r in failed)
    assert records[-1]["nodeid"].endswith("::test_exit")
    assert records[-1]["phase"] == "call"
    assert records[-1]["event"] == "start"
