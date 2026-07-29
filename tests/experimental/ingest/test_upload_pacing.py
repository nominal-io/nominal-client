from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

from nominal.core.exceptions import NominalRequestThrottledError
from nominal.experimental.ingest._upload_pacing import (
    _GlobalBackoff,
    _is_throttle_error,
    _ThrottleGate,
)


class _FakeClock:
    """Deterministic clock whose sleep advances time — no real sleeping in fake-time tests."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_gate(**overrides):
    clock = _FakeClock()
    kwargs = dict(
        max_concurrency=4,
        deadline_seconds=120.0,
        clock=clock,
        sleep=clock.sleep,
        jitter=lambda delay: delay,  # deterministic: sleep the full damper delay
    )
    kwargs.update(overrides)
    return _ThrottleGate(**kwargs), clock


class _StatusError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"status {status}")
        self.status_code = status


class TestGlobalBackoff:
    def test_starts_at_zero_and_success_keeps_it_there(self) -> None:
        backoff = _GlobalBackoff()
        backoff.on_success()
        assert backoff.current == 0.0

    def test_first_throttle_lands_on_the_base(self) -> None:
        backoff = _GlobalBackoff(base=0.05, cap=2.0)
        backoff.on_throttle()
        assert backoff.current == pytest.approx(0.05)

    def test_throttles_double_up_to_the_cap(self) -> None:
        backoff = _GlobalBackoff(base=0.05, cap=2.0)
        for _ in range(20):
            backoff.on_throttle()
        assert backoff.current == pytest.approx(2.0)

    def test_success_decays_and_snaps_to_zero(self) -> None:
        backoff = _GlobalBackoff(base=0.05, cap=2.0, decay=0.5)
        backoff.on_throttle()  # 0.05
        backoff.on_success()  # 0.025: at the base/2 boundary, snaps to zero
        assert backoff.current == 0.0

    def test_storm_then_recovery_round_trip(self) -> None:
        backoff = _GlobalBackoff(base=0.05, cap=2.0, decay=0.9)
        for _ in range(10):
            backoff.on_throttle()
        assert backoff.current == pytest.approx(2.0)
        for _ in range(200):
            backoff.on_success()
        assert backoff.current == 0.0


class TestThrottleClassification:
    def test_429_is_throttle(self) -> None:
        assert _is_throttle_error(_StatusError(429))

    def test_503_is_throttle(self) -> None:
        assert _is_throttle_error(_StatusError(503))

    def test_retry_error_is_throttle(self) -> None:
        assert _is_throttle_error(requests.exceptions.RetryError())

    def test_400_is_not_throttle(self) -> None:
        assert not _is_throttle_error(_StatusError(400))

    def test_plain_exception_is_not_throttle(self) -> None:
        assert not _is_throttle_error(ValueError("nope"))


class TestThrottleGateSuccessPath:
    def test_success_returns_result_with_no_sleeping(self) -> None:
        gate, clock = make_gate()
        assert gate.call(lambda: "ok") == "ok"
        assert clock.sleeps == []
        assert gate.current_backoff == 0.0

    def test_non_throttle_error_raises_immediately_and_releases_the_lane(self) -> None:
        gate, _clock = make_gate(max_concurrency=1)
        calls: list[int] = []

        def op() -> None:
            calls.append(1)
            raise ValueError("terminal")

        with pytest.raises(ValueError):
            gate.call(op)
        assert len(calls) == 1
        # The single ticket must have been released on the failure path:
        assert gate.call(lambda: "still works") == "still works"

    def test_lane_admits_at_most_max_concurrency(self) -> None:
        gate, _clock = make_gate(max_concurrency=3)
        admitted = threading.Semaphore(0)
        release = threading.Event()
        lock = threading.Lock()
        active = [0]
        peak = [0]

        def op() -> str:
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            admitted.release()
            release.wait(timeout=10)
            with lock:
                active[0] -= 1
            return "ok"

        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(gate.call, op) for _ in range(6)]
            for _ in range(3):
                assert admitted.acquire(timeout=10)  # three calls admitted concurrently
            release.set()
            for fut in futures:
                assert fut.result(timeout=10) == "ok"
        assert peak[0] == 3  # never more than the lane width, and the lane was fully used


class TestThrottleGateBackoffPath:
    def test_throttle_bumps_damper_sleeps_then_succeeds(self) -> None:
        gate, clock = make_gate()
        outcomes: list = [_StatusError(429), _StatusError(429), "ok"]

        def op() -> str:
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        assert gate.call(op) == "ok"
        # Two throttles: the damper went base -> 2*base, and each retry slept the full delay.
        assert clock.sleeps == [pytest.approx(0.05), pytest.approx(0.10)]

    def test_retry_error_is_the_primary_signal(self) -> None:
        gate, clock = make_gate()
        outcomes: list = [requests.exceptions.RetryError(), "ok"]

        def op() -> str:
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        assert gate.call(op) == "ok"
        assert clock.sleeps == [pytest.approx(0.05)]

    def test_backoff_sleep_holds_no_lane_ticket(self) -> None:
        # One-ticket lane; a nested successful call runs from INSIDE the backoff sleep. If the
        # sleeping thread still held its ticket, the nested call could never be admitted and
        # would exhaust its short deadline instead of succeeding.
        clock = _FakeClock()
        gate = _ThrottleGate(
            max_concurrency=1, deadline_seconds=120.0, clock=clock, sleep=clock.sleep, jitter=lambda d: d
        )
        nested_result: list[str] = []
        original_sleep = gate._sleep

        def sleep_and_probe(seconds: float) -> None:
            original_sleep(seconds)
            nested_result.append(gate.call(lambda: "nested ok", deadline_seconds=0.5))

        gate._sleep = sleep_and_probe  # type: ignore[method-assign]
        outcomes: list = [_StatusError(429), "outer ok"]

        def op() -> str:
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        assert gate.call(op) == "outer ok"
        assert nested_result == ["nested ok"]

    def test_shared_damper_across_gates(self) -> None:
        backoff = _GlobalBackoff(base=0.05, cap=2.0)
        gate_a, clock_a = make_gate(backoff=backoff)
        gate_b, clock_b = make_gate(backoff=backoff)

        def throttled_and_slow() -> str:
            clock_a.now += 1.0  # one attempt runs, bumps the damper, then exhausts the budget
            raise _StatusError(429)

        with pytest.raises(NominalRequestThrottledError):
            gate_a.call(throttled_and_slow, deadline_seconds=0.5)
        # gate_b's retry sleeps a delay seeded by gate_a's storm signal (0.05 doubled to 0.10):
        outcomes: list = [_StatusError(429), "ok"]

        def op() -> str:
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        assert gate_b.call(op) == "ok"
        assert clock_b.sleeps == [pytest.approx(0.10)]


class TestThrottleGateDeadlines:
    def test_sustained_throttle_exhausts_the_budget(self) -> None:
        gate, clock = make_gate(deadline_seconds=10.0)

        def op() -> None:
            clock.now += 3.0
            raise _StatusError(429)

        with pytest.raises(NominalRequestThrottledError) as excinfo:
            gate.call(op)
        assert isinstance(excinfo.value.__cause__, _StatusError)
        message = str(excinfo.value)
        assert "server kept throttling" in message
        assert "attempts" in message

    def test_backoff_sleeps_consume_the_budget(self) -> None:
        # op costs zero wall time; only damper sleeps (via clock.sleep) advance the clock, so
        # exhaustion here proves backoff time counts against the deadline.
        gate, clock = make_gate(deadline_seconds=1.0)

        def op() -> None:
            raise _StatusError(429)

        with pytest.raises(NominalRequestThrottledError):
            gate.call(op)
        assert clock.now >= 1.0
        assert clock.sleeps  # it got there by sleeping, not by op time

    def test_per_call_deadline_override(self) -> None:
        gate, clock = make_gate(deadline_seconds=120.0)

        def op() -> None:
            clock.now += 3.0
            raise _StatusError(429)

        with pytest.raises(NominalRequestThrottledError):
            gate.call(op, deadline_seconds=5.0)
        assert clock.now < 10.0  # the 120s default demonstrably did not govern

    def test_full_lane_refuses_a_short_deadline_caller_promptly(self) -> None:
        # Real threads and a real (bounded, sub-second) semaphore timeout: the lane's single
        # ticket is parked on an Event, so the short-deadline caller cannot be admitted and
        # must fail fast with no attempt made — the abort-path guarantee.
        gate = _ThrottleGate(max_concurrency=1, deadline_seconds=120.0)
        occupied = threading.Event()
        release = threading.Event()

        def parked() -> str:
            occupied.set()
            release.wait(timeout=10)
            return "parked done"

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(gate.call, parked)
            assert occupied.wait(timeout=10)
            with pytest.raises(NominalRequestThrottledError) as excinfo:
                gate.call(lambda: "never runs", deadline_seconds=0.05)
            assert "lane could not admit" in str(excinfo.value)
            assert excinfo.value.__cause__ is None  # no throttle was ever observed
            release.set()
            assert fut.result(timeout=10) == "parked done"
