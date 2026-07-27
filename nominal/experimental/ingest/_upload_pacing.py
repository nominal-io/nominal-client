"""Adaptive request pacing for the experimental uploader.

The server meters *requests* with a token-bucket-style budget discovered at runtime; the
mirror-image client is a paced admission gate whose rate adapts on 429s.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, TypeVar

import requests

from nominal.core.exceptions import NominalRequestThrottledError

T = TypeVar("T")

DEFAULT_THROTTLE_DEADLINE_S = 120.0  # a request unadmitted this long means unavailable, not busy
_ABORT_THROTTLE_DEADLINE_S = 5.0  # best-effort rollback must not compete with live uploads


def _is_throttle_error(exc: BaseException) -> bool:
    """True if `exc` is the server refusing the request because the caller is over budget.

    429 is the budget refusal; 503 is the server shedding load (with transport status-retries
    disabled it must not become terminal); RetryError is kept defensively for any call path
    still riding a transport that absorbs throttle statuses internally.
    """
    if isinstance(exc, requests.exceptions.RetryError):
        return True
    status = getattr(exc, "status_code", None)
    if status is None:
        status = getattr(getattr(exc, "response", None), "status_code", None)
    return status in (429, 503)


class _AdaptivePacer:
    """Paces admissions at an adaptive rate; the single control variable is `rate` (req/s).

    Claim-slot-then-sleep: each acquirer claims its own wake time under the lock and sleeps
    outside it, so there are no condition waits and no wakeup ordering. Idle time is never
    banked. An adaptation interval may raise the rate only if some acquirer actually waited
    during it (the saturation gate) — an unsaturated lane can never inflate the estimate.
    """

    def __init__(
        self,
        *,
        initial_rate: float = 10.0,
        min_rate: float = 0.5,
        interval: float = 2.0,
        probe_factor: float = 1.5,
        decrease_factor: float = 0.6,
        additive_increase: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._min = min_rate
        self._interval = interval
        self._probe_factor = probe_factor
        self._decrease_factor = decrease_factor
        self._additive_increase = additive_increase
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._rate = max(min_rate, initial_rate)
        self._next_slot = float("-inf")
        self._probing = True
        self._interval_start = clock()
        self._interval_saturated = False
        self._interval_throttled = False

    @property
    def rate(self) -> float:
        with self._lock:
            return self._rate

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            slot = max(now, self._next_slot)
            if slot > now:
                self._interval_saturated = True  # demand met pacing: this interval may increase
            self._next_slot = slot + 1.0 / self._rate
        wait = slot - self._clock()
        if wait > 0:
            self._sleep(wait)

    def on_success(self) -> None:
        with self._lock:
            self._maybe_roll_interval()

    def on_throttle(self) -> None:
        with self._lock:
            self._maybe_roll_interval()
            self._probing = False
            if not self._interval_throttled:  # one empty-bucket event throttles everything at
                self._interval_throttled = True  # once; count it as one signal, not twenty
                self._rate = max(self._min, self._rate * self._decrease_factor)

    def _maybe_roll_interval(self) -> None:
        # Lazy interval accounting: no timer thread. Multiple silent intervals collapse into
        # one roll with saturated=False, so quiet time never moves the rate.
        now = self._clock()
        if now - self._interval_start < self._interval:
            return
        if self._interval_saturated and not self._interval_throttled:
            if self._probing:
                self._rate *= self._probe_factor
            else:
                self._rate += self._additive_increase
        self._interval_start = now
        self._interval_saturated = False
        self._interval_throttled = False


class _ThrottleGate:
    """Admits Nominal API requests through the pacer and owns throttle retry semantics.

    On a throttle the request feeds the pacer and simply re-enters paced admission — the
    post-cut pacing IS the backoff (a herd cannot form through a paced gate). The only
    safeguard is the per-request wall-clock deadline.
    """

    def __init__(
        self,
        pacer: _AdaptivePacer,
        *,
        deadline_seconds: float = DEFAULT_THROTTLE_DEADLINE_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._pacer = pacer
        self._deadline_seconds = deadline_seconds
        self._clock = clock

    @property
    def current_rate(self) -> float:
        return self._pacer.rate

    def call(self, op: Callable[[], T], *, deadline_seconds: float | None = None) -> T:
        budget = self._deadline_seconds if deadline_seconds is None else deadline_seconds
        started = self._clock()
        attempt = 0
        while True:
            self._pacer.acquire()
            try:
                result = op()
            except BaseException as exc:
                if not _is_throttle_error(exc):
                    raise
                self._pacer.on_throttle()
                elapsed = self._clock() - started
                if elapsed >= budget:
                    raise NominalRequestThrottledError(
                        f"server kept throttling this request for {elapsed:.1f}s "
                        f"(budget {budget}s) across {attempt + 1} attempts; giving up"
                    ) from exc
                attempt += 1
            else:
                self._pacer.on_success()
                return result
