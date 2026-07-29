"""Request governance for the experimental uploader: two-layer backoff, throughput-first.

The transport performs each request's own short jittered retries (the standard conjure
client retries throttle statuses locally on a sub-second ladder). This module adds the two
pieces that need to be global:

- a fixed-width admission lane (`max_concurrency` tickets) so at most N requests ever touch
  the Nominal API concurrently — bounding the offered pressure structurally instead of
  estimating a rate; and
- one shared damping delay (`_GlobalBackoff`) that rises when *sustained* throttling is
  observed (transport retry exhaustion, or raw 429/503 from transports without local
  retries) and decays on success — smoothing storms without a controller.

A refused request is cheap for the server, so the design goal is throughput, not 429
avoidance: isolated throttles resolve invisibly in the transport's local retries, and the
global layer only engages when the budget is genuinely saturated.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Callable, TypeVar

import requests

from nominal.core.exceptions import NominalRequestThrottledError

logger = logging.getLogger(__name__)

T = TypeVar("T")

# The structural bound on offered request pressure: at typical round-trip times this lane
# width offers roughly the server's admission budget, so refusals stay rare. Benchmark-tuned —
# wider lanes finished many-small-file batches no sooner while a large fraction of requests
# were refused and re-sent (each refused single-shot upload re-transmits its whole body).
# Fits well inside the shared client's per-host connection pool, so a full lane never churns
# connections.
NOMINAL_MAX_CONCURRENCY = 5

DEFAULT_THROTTLE_DEADLINE_S = 120.0  # a request unadmitted this long means unavailable, not busy
_ABORT_THROTTLE_DEADLINE_S = 5.0  # best-effort rollback must not compete with live uploads

_BACKOFF_BASE_S = 0.05  # first storm signal: pause in the tens of milliseconds
# Cap on the damper's delay: a fully-stormed lane still offers max_concurrency/cap requests per
# second. Exposed as the uploader's `max_backoff_duration` knob.
DEFAULT_MAX_BACKOFF_DURATION_S = 2.0
_BACKOFF_DECAY = 0.9  # per-success decay: a healthy lane forgets a storm within ~a second of traffic


def _is_throttle_error(exc: BaseException) -> bool:
    """True if `exc` is the server refusing the request because the caller is over budget.

    `RetryError` is the primary signal: the transport's own short retry ladder was exhausted,
    meaning the throttling is sustained rather than momentary. Raw 429/503 surface as
    `requests.HTTPError` (the conjure client's errors subclass it) from transports that do not
    retry statuses locally.
    """
    if isinstance(exc, requests.exceptions.RetryError):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        return exc.response is not None and exc.response.status_code in (429, 503)
    return False


class _GlobalBackoff:
    """One shared damping delay — no estimation, two moves.

    A throttle signal doubles the delay (floored at `base`, capped at `cap`); a success
    decays it (snapping to zero once it falls below half the base, so a recovered lane pays
    nothing). Every backing-off thread sleeps a full-jittered fraction of the same shared
    number, which is what keeps a storm's retries from re-herding.
    """

    def __init__(
        self,
        *,
        base: float = _BACKOFF_BASE_S,
        cap: float = DEFAULT_MAX_BACKOFF_DURATION_S,
        decay: float = _BACKOFF_DECAY,
    ) -> None:
        self._base = base
        self._cap = cap
        self._decay = decay
        self._lock = threading.Lock()
        self._delay = 0.0

    @property
    def current(self) -> float:
        with self._lock:
            return self._delay

    def on_throttle(self) -> None:
        with self._lock:
            self._delay = min(self._cap, max(self._base, self._delay * 2.0))

    def on_success(self) -> None:
        with self._lock:
            if self._delay > 0.0:
                self._delay *= self._decay
                if self._delay <= self._base / 2.0:
                    self._delay = 0.0


class _ThrottleGate:
    """Admits Nominal API requests through a fixed-width lane and owns global throttle retry.

    `call` acquires one of `max_concurrency` tickets for the duration of the request (the
    transport's local retries included), so offered pressure is bounded structurally. On a
    sustained-throttle signal the ticket is released *before* any backoff sleep — a sleeping
    thread must never occupy the lane it is backing off from — the shared damper is bumped,
    and the request re-enters the lane after a jittered fraction of the damper's delay.

    The only wall clock is the per-request deadline, which bounds the whole call: lane
    admission waits spend the same budget as the request does, so a short-deadline caller
    (an abort, say) fails fast instead of blocking behind a stormed lane.
    """

    def __init__(
        self,
        *,
        # Deliberately no defaults on the tuning config: the uploader's `create()` owns the
        # defaults, and a second set here could only drift from it.
        max_concurrency: int,
        deadline_seconds: float,
        backoff: _GlobalBackoff | None = None,
        # The time seams below default to the real clock; tests inject deterministic ones so
        # throttle/backoff behavior is testable without real sleeping.
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[float], float] | None = None,
    ) -> None:
        self._semaphore = threading.Semaphore(max_concurrency)
        self._backoff = backoff if backoff is not None else _GlobalBackoff()
        self._deadline_seconds = deadline_seconds
        self._clock = clock
        self._sleep = sleep
        self._jitter = jitter if jitter is not None else (lambda delay: random.uniform(0.0, delay))

    @property
    def current_backoff(self) -> float:
        """The shared damper's current delay, in seconds (0.0 when the lane is healthy)."""
        return self._backoff.current

    def call(self, op: Callable[[], T], *, deadline_seconds: float | None = None) -> T:
        """Run `op` under lane admission, retrying for as long as the server throttles it.

        Args:
            op: The request to run. It is retried verbatim on a throttle, so it must be safe
                to repeat.
            deadline_seconds: Wall-clock budget for the whole call, counting lane-admission
                waits and backoff sleeps as well as time spent in `op`. Defaults to the
                gate's own budget.

        Returns:
            Whatever `op` returns on the first attempt the server does not throttle.

        Raises:
            NominalRequestThrottledError: The budget ran out — either the server kept
                throttling the request, or the lane could not admit it in time. `__cause__`
                is the last throttle seen, or None if the budget expired before any attempt
                was made.
            Exception: Any non-throttle error from `op`, re-raised unchanged from the attempt
                that produced it (never retried).
        """
        budget = self._deadline_seconds if deadline_seconds is None else deadline_seconds
        started = self._clock()
        deadline_at = started + budget
        attempt = 0
        last_throttle: BaseException | None = None
        while True:
            remaining = deadline_at - self._clock()
            if remaining <= 0 or not self._semaphore.acquire(timeout=remaining):
                raise self._exhausted(
                    "the nominal lane could not admit this request", started, budget, attempt
                ) from last_throttle
            try:
                result = op()
            except BaseException as exc:
                # Released before classification: whatever happens next, this thread is done
                # using the lane, and a backoff sleep must not hold a ticket.
                self._semaphore.release()
                if not _is_throttle_error(exc):
                    raise
                self._backoff.on_throttle()
                last_throttle = exc
                elapsed = self._clock() - started
                logger.debug(
                    "request throttled by the server (attempt %d, %.1fs into a %.0fs budget); global damper now %.3fs",
                    attempt + 1,
                    elapsed,
                    budget,
                    self._backoff.current,
                )
                if elapsed >= budget:
                    raise self._exhausted("server kept throttling this request", started, budget, attempt + 1) from exc
                attempt += 1
                delay = self._backoff.current
                if delay > 0.0:
                    self._sleep(min(self._jitter(delay), budget - elapsed))
            else:
                self._semaphore.release()
                self._backoff.on_success()
                return result

    def _exhausted(self, detail: str, started: float, budget: float, attempts: int) -> NominalRequestThrottledError:
        elapsed = self._clock() - started
        return NominalRequestThrottledError(
            f"{detail} after {elapsed:.1f}s of a {budget}s budget across {attempts} attempts; giving up"
        )
