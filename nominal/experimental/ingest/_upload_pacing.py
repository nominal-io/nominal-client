"""Adaptive request pacing for the experimental uploader.

The server refuses requests over budget with an immediate 429; the client discovers its
sustainable request rate at runtime and paces admissions to it.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, TypeVar

import requests

from nominal.core.exceptions import NominalRequestThrottledError

T = TypeVar("T")


class _AdmissionDeadlineExceeded(Exception):
    """The next free pace slot lies past the caller's deadline; no slot was claimed.

    Module-private: `_ThrottleGate` translates it into `NominalRequestThrottledError`, so it
    never reaches a caller.
    """


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

    def acquire(self, *, deadline_at: float | None = None) -> None:
        """Claim the next pace slot and sleep until it comes due.

        Args:
            deadline_at: Absolute time by which admission must have happened, or None to wait
                however long the queue takes. A caller that cannot wait that long must not hold
                a slot it will never use, so a slot past the deadline is refused rather than
                claimed — the queue is left exactly as it was found.

        Raises:
            _AdmissionDeadlineExceeded: The next free slot falls after `deadline_at`.
        """
        with self._lock:
            now = self._clock()
            slot = max(now, self._next_slot)
            if deadline_at is not None and slot > deadline_at:
                # Refuse before mutating anything: no slot consumed, no saturation recorded.
                raise _AdmissionDeadlineExceeded(
                    f"next pace slot is {slot - now:.1f}s away, past the caller's deadline"
                )
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
        # Lazy interval accounting: no timer thread, so a roll can arrive long after the window
        # it closes. A gap spanning more than one interval means a whole interval went by without
        # a settlement, so whatever the flags recorded describes a window that has since fallen
        # silent — discard that evidence instead of letting it bank an increase. Dropping a stale
        # throttle along with it only suppresses an increase, which is the safe direction.
        now = self._clock()
        elapsed = now - self._interval_start
        if elapsed < self._interval:
            return
        if elapsed >= 2 * self._interval:
            self._interval_saturated = False
            self._interval_throttled = False
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
    safeguard is the per-request wall-clock deadline, which bounds the whole call: waiting for
    a pace slot spends the same budget as the request does, so a short-deadline caller (an
    abort, say) fails fast instead of blocking behind a queue it can never reach the front of.
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
        """Run `op` under paced admission, retrying for as long as the server throttles it.

        Args:
            op: The request to run. It is retried verbatim on a throttle, so it must be safe to
                repeat.
            deadline_seconds: Wall-clock budget for the whole call, counting time spent waiting
                for paced admission as well as time spent in `op`. Defaults to the gate's own
                budget.

        Returns:
            Whatever `op` returns on the first attempt the server does not throttle.

        Raises:
            NominalRequestThrottledError: The budget ran out — either the server kept throttling
                the request, or the paced queue could not admit it in time. `__cause__` is the
                last throttle seen, or None if the budget expired before any attempt was made.
            Exception: Any non-throttle error from `op`, re-raised unchanged from the attempt
                that produced it (never retried).
        """
        budget = self._deadline_seconds if deadline_seconds is None else deadline_seconds
        started = self._clock()
        deadline_at = started + budget
        attempt = 0
        last_throttle: BaseException | None = None
        while True:
            try:
                # Admission waits spend the same budget as the request itself, so a queue deeper
                # than the budget fails fast instead of blocking past the caller's deadline.
                self._pacer.acquire(deadline_at=deadline_at)
            except _AdmissionDeadlineExceeded:
                raise self._exhausted(
                    "the paced queue could not admit this request", started, budget, attempt
                ) from last_throttle
            try:
                result = op()
            except BaseException as exc:
                if not _is_throttle_error(exc):
                    raise
                self._pacer.on_throttle()
                last_throttle = exc
                if self._clock() - started >= budget:
                    raise self._exhausted("server kept throttling this request", started, budget, attempt + 1) from exc
                attempt += 1
            else:
                self._pacer.on_success()
                return result

    def _exhausted(self, detail: str, started: float, budget: float, attempts: int) -> NominalRequestThrottledError:
        elapsed = self._clock() - started
        return NominalRequestThrottledError(
            f"{detail} after {elapsed:.1f}s of a {budget}s budget across {attempts} attempts; giving up"
        )
