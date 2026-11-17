"""Opt-in phase timings: load with ``-p tests.e2e_timing`` to retain progress on CI timeout."""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Iterator

import pytest


def _emit(nodeid: str, phase: str, event: str, **fields: str | float) -> None:
    # Bypass Python capture and flush each record; no end-of-session summary is needed.
    print(
        "E2E_TIMING " + json.dumps(dict(nodeid=nodeid, phase=phase, event=event, timestamp=time.time(), **fields)),
        file=sys.__stdout__,
        flush=True,
    )


# Outermost wrappers emit before pytest enables phase capture, including fd capture.
@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> Iterator[None]:
    _emit(item.nodeid, "setup", "start")
    yield


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_call(item: pytest.Item) -> Iterator[None]:
    _emit(item.nodeid, "call", "start")
    yield


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_teardown(item: pytest.Item) -> Iterator[None]:
    _emit(item.nodeid, "teardown", "start")
    yield


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    _emit(report.nodeid, report.when, "finish", duration_seconds=report.duration, outcome=report.outcome)
