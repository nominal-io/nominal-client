"""Shared fixtures and fakes for the experimental-ingest test suite."""

from __future__ import annotations

import pathlib
from typing import Any, Callable

import pytest

from nominal.experimental.ingest._multipart_uploader import MultipartUploader
from nominal.experimental.ingest._upload_pacing import _ThrottleGate


class FakeClock:
    """Deterministic clock whose sleep advances time — no real sleeping in fake-time tests."""

    def __init__(self) -> None:
        """Start at time zero with no sleeps recorded."""
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def write_file(tmp_path: pathlib.Path) -> Callable[[str, int], pathlib.Path]:
    """Factory writing a `size`-byte file named `name` into this test's tmp dir."""

    def _write(name: str, size: int) -> pathlib.Path:
        path = tmp_path / name
        path.write_bytes(b"x" * size)
        return path

    return _write


@pytest.fixture
def fake_clock() -> FakeClock:
    """A fresh deterministic clock for tests that drive time seams directly."""
    return FakeClock()


@pytest.fixture
def make_gate() -> Callable[..., tuple[_ThrottleGate, FakeClock]]:
    """Factory for a throttle gate on a fresh fake clock with deterministic (identity) jitter."""

    def _make(**overrides: Any) -> tuple[_ThrottleGate, FakeClock]:
        clock = FakeClock()
        kwargs: dict[str, Any] = {
            "max_concurrency": 4,
            "deadline_seconds": 120.0,
            "clock": clock,
            "sleep": clock.sleep,
            "jitter": lambda delay: delay,  # deterministic: sleep the full damper delay
        }
        kwargs.update(overrides)
        return _ThrottleGate(**kwargs), clock

    return _make


@pytest.fixture
def install_test_gate() -> Callable[..., FakeClock]:
    """Factory swapping an uploader's gate for one on a fake clock: retries instant and countable.

    The lane is made wide enough that admission never blocks; the gate's own semantics are
    covered in test_upload_pacing.
    """

    def _install(up: MultipartUploader, *, deadline_seconds: float = 120.0) -> FakeClock:
        clock = FakeClock()
        up._gate = _ThrottleGate(
            max_concurrency=1000, deadline_seconds=deadline_seconds, clock=clock, sleep=clock.sleep, jitter=lambda d: d
        )
        return clock

    return _install
