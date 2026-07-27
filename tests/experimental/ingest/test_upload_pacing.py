from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

from nominal.core.exceptions import NominalRequestThrottledError
from nominal.experimental.ingest._upload_pacing import (
    _AdaptivePacer,
    _is_throttle_error,
    _ThrottleGate,
)


class _FakeClock:
    """Deterministic clock whose sleep advances time — no real sleeping anywhere."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_pacer(**overrides):
    clock = _FakeClock()
    kwargs = dict(initial_rate=10.0, clock=clock, sleep=clock.sleep)
    kwargs.update(overrides)
    return _AdaptivePacer(**kwargs), clock


def saturate_one_interval(pacer: _AdaptivePacer, clock: _FakeClock) -> None:
    """Back-to-back acquires + successes until an interval boundary has passed."""
    end = clock.now + 2.0
    while clock.now <= end:
        pacer.acquire()
        pacer.on_success()


class TestAdaptivePacer:
    def test_acquires_are_spaced_one_over_rate(self) -> None:
        pacer, clock = make_pacer()
        times = []
        for _ in range(3):
            pacer.acquire()
            times.append(clock.now)
        assert times == pytest.approx([0.0, 0.1, 0.2])

    def test_idle_gap_does_not_bank_slots(self) -> None:
        pacer, clock = make_pacer()
        pacer.acquire()  # t=0.0
        clock.now = 10.0  # long idle gap
        pacer.acquire()  # admits at 10.0 with no wait (nothing banked)
        assert clock.now == pytest.approx(10.0)
        pacer.acquire()  # next claim is exactly 1/rate later
        assert clock.now == pytest.approx(10.1)

    def test_probe_multiplies_on_clean_saturated_interval(self) -> None:
        pacer, clock = make_pacer(interval=2.0)
        saturate_one_interval(pacer, clock)
        assert pacer.rate == pytest.approx(15.0)

    def test_unsaturated_interval_never_raises_rate(self) -> None:
        pacer, clock = make_pacer(interval=2.0)
        for _ in range(6):  # demand 1/s << 10/s: no acquire ever waits
            pacer.acquire()
            pacer.on_success()
            clock.now += 1.0
        assert pacer.rate == pytest.approx(10.0)

    def test_zero_request_intervals_do_not_advance_adaptation(self) -> None:
        pacer, clock = make_pacer(interval=2.0)
        clock.now += 60.0  # 30 silent intervals
        pacer.acquire()
        pacer.on_success()
        assert pacer.rate == pytest.approx(10.0)

    def test_throttle_cuts_once_per_interval(self) -> None:
        pacer, clock = make_pacer()
        pacer.acquire()
        pacer.on_throttle()
        assert pacer.rate == pytest.approx(6.0)
        pacer.acquire()
        pacer.on_throttle()  # same interval: debounced
        assert pacer.rate == pytest.approx(6.0)

    def test_converged_regime_adds_after_clean_saturated_interval(self) -> None:
        pacer, clock = make_pacer()
        pacer.acquire()
        pacer.on_throttle()  # leaves probe; rate = 6.0
        saturate_one_interval(pacer, clock)  # the interval that saw the throttle never raises
        assert pacer.rate == pytest.approx(6.0)
        saturate_one_interval(pacer, clock)  # the first clean saturated interval after it does
        assert pacer.rate == pytest.approx(7.0)  # +1.0, NOT ×1.5

    def test_rate_never_below_floor(self) -> None:
        pacer, clock = make_pacer(initial_rate=1.0, min_rate=0.5)
        for _ in range(5):
            pacer.acquire()
            pacer.on_throttle()
            clock.now += 3.0  # new interval each time so every cut applies
        assert pacer.rate == pytest.approx(0.5)

    def test_rate_change_does_not_respace_claimed_slots(self) -> None:
        pacer, clock = make_pacer()
        pacer.acquire()  # t=0.0 (claims next=0.1)
        pacer.acquire()  # t=0.1 (claims next=0.2)
        pacer.on_throttle()  # rate -> 6.0
        pacer.acquire()  # already-claimed 0.2 stands...
        assert clock.now == pytest.approx(0.2)
        pacer.acquire()  # ...but the new claim is spaced at the new rate
        assert clock.now == pytest.approx(0.2 + 1.0 / 6.0)

    def test_concurrent_acquires_all_admit_and_stay_paced(self) -> None:
        # Real threads contending on the pacer, against a frozen clock (sleep records instead
        # of advancing it) so the claimed spacing is exact and not scheduler-dependent.
        clock = _FakeClock()
        waits: list[float] = []
        lock = threading.Lock()

        def record(seconds: float) -> None:
            with lock:
                waits.append(seconds)

        pacer = _AdaptivePacer(initial_rate=100.0, clock=clock, sleep=record)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: pacer.acquire(), range(32)))  # returning at all = no deadlock
        # One acquirer admits immediately; the other 31 hold distinct, contiguous slots — no two
        # threads ever share a slot, so concurrency cannot burst past the rate.
        assert sorted(waits) == pytest.approx([k / 100.0 for k in range(1, 32)])


class _StatusError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"status {status}")
        self.status_code = status


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


def make_gate(**pacer_overrides):
    pacer, clock = make_pacer(**pacer_overrides)
    gate = _ThrottleGate(pacer, deadline_seconds=120.0, clock=clock)
    return gate, pacer, clock


class TestThrottleGate:
    def test_success_returns_result_and_feeds_pacer(self) -> None:
        gate, pacer, clock = make_gate()
        assert gate.call(lambda: "ok") == "ok"
        # successes reach the pacer: a clean, saturated interval of them raises the rate
        while clock.now <= 2.0:
            gate.call(lambda: "ok")
        assert pacer.rate == pytest.approx(15.0)

    def test_non_throttle_error_raises_immediately(self) -> None:
        gate, pacer, clock = make_gate()
        calls = []

        def op():
            calls.append(1)
            raise ValueError("terminal")

        with pytest.raises(ValueError):
            gate.call(op)
        assert len(calls) == 1

    def test_throttled_op_retries_until_success_with_only_pacer_waits(self) -> None:
        gate, pacer, clock = make_gate()
        outcomes = [_StatusError(429), _StatusError(429), "ok"]

        def op():
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        assert gate.call(op) == "ok"
        # the only waits are pace slots (the second at the post-cut rate) — the gate adds no
        # backoff sleep of its own; the re-paced admission is the backoff
        assert clock.sleeps == pytest.approx([1.0 / 10.0, 1.0 / 6.0])

    def test_throttle_cuts_rate(self) -> None:
        gate, pacer, clock = make_gate()
        outcomes = [_StatusError(429), "ok"]

        def op():
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        gate.call(op)
        assert pacer.rate < 10.0

    def test_deadline_exceeded_raises_with_cause_and_attempts(self) -> None:
        gate, pacer, clock = make_gate()

        def op():
            clock.now += 30.0  # each attempt burns 30s of wall clock
            raise _StatusError(429)

        with pytest.raises(NominalRequestThrottledError) as excinfo:
            gate.call(op)
        assert isinstance(excinfo.value.__cause__, _StatusError)
        assert "attempts" in str(excinfo.value)

    def test_per_call_deadline_override(self) -> None:
        gate, pacer, clock = make_gate()

        def op():
            clock.now += 3.0
            raise _StatusError(429)

        with pytest.raises(NominalRequestThrottledError):
            gate.call(op, deadline_seconds=5.0)

    def test_current_rate_exposed(self) -> None:
        gate, pacer, clock = make_gate()
        assert gate.current_rate == pytest.approx(10.0)
