"""Per-stage timing instrumentation for MultipartUploader, to find which part(s)
/ stage(s) misbehave when uploading many small files.

It times every *network* stage of the upload by wrapping the two clients the
uploader talks through:

  Nominal control-plane (rate-limited)   S3 storage (bandwidth-bound)
  ------------------------------------   ----------------------------
  initiate_multipart_upload              PUT (per part)
  sign_part (per part)
  list_parts
  complete_multipart_upload
  abort

For each call it records: stage, population (nominal|s3), start/end (so we see
front-loading), duration, the *per-population* concurrency in flight when it
started, outcome, and a short identity label. `summarize()` then prints, per
stage, the latency distribution (p50/p90/p99/max), failures, the peak concurrency
and request rate for each population, and the slowest individual calls (outliers).

The local **read** stage (open+seek+read inside `_upload_part`) is NOT wrapped —
it's a local disk read of microseconds for small files, so it isn't the suspect.
If you want it too, ask and I'll add a tiny opt-in hook in the uploader.

--------------------------------------------------------------------------------
USAGE — against the REAL backend (the measurement that matters):

    import threading
    from benchmarks.instrument_multipart_upload import build_instrumented_uploader, sample_gate_limit, summarize

    up, rec = build_instrumented_uploader(client._clients, max_workers=8)  # the setting that 429s
    limit_samples: list[tuple[float, float]] = []
    stop = threading.Event()
    sampler = threading.Thread(target=sample_gate_limit, args=(up._gate, stop, limit_samples), daemon=True)
    sampler.start()
    try:
        futs = [up.enqueue_file(p) for p in paths]     # e.g. 1000 tiny files
        for f in futs:
            try: f.result()
            except Exception: pass                     # keep going; failures are recorded
    finally:
        stop.set()
        sampler.join(timeout=2.0)
    up.close()
    summarize(rec, max_workers=8, limit_samples=limit_samples)

USAGE — locally, no backend (illustrates the pattern the design produces):

    uv run python benchmarks/instrument_multipart_upload.py --files 1000 --workers 8 \
        --api-latency-ms 40 --put-latency-ms 15 --jitter
"""

from __future__ import annotations

import argparse
import pathlib
import random
import tempfile
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable

NOMINAL = "nominal"  # control-plane / rate-limited calls
S3 = "s3"  # storage PUTs / bandwidth-bound


@dataclass
class CallRecord:
    stage: str  # initiate | sign_part | put | list_parts | complete | abort
    population: str  # NOMINAL | S3
    t_start: float  # monotonic seconds
    t_end: float
    inflight_at_start: int  # concurrency within this population when the call began
    ok: bool
    error: str | None
    label: str  # short identity, e.g. "key-7#1" or a URL tail

    @property
    def ms(self) -> float:
        return (self.t_end - self.t_start) * 1000.0


class Recorder:
    """Thread-safe collector. Tracks in-flight concurrency per population separately,
    so we can see Nominal-API concurrency vs S3-PUT concurrency independently."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records: list[CallRecord] = []
        self._inflight: dict[str, int] = {NOMINAL: 0, S3: 0}

    def _enter(self, population: str) -> int:
        with self._lock:
            self._inflight[population] += 1
            return self._inflight[population]

    def _exit(self, population: str, rec: CallRecord) -> None:
        with self._lock:
            self._inflight[population] -= 1
            self.records.append(rec)


def sample_gate_limit(
    gate: Any, stop: threading.Event, out: list[tuple[float, float]], *, interval_s: float = 0.25
) -> None:
    """Poll the throttle gate's adaptive limit until `stop` is set.

    Run this on a daemon thread for the duration of an upload run. Whether the limit converges or
    oscillates is the signal that decides whether concurrency is the right control variable at all.
    """
    t0 = time.monotonic()
    while not stop.is_set():
        out.append((time.monotonic() - t0, gate.limit))
        stop.wait(interval_s)


def _error_label(exc: BaseException) -> str:
    status = getattr(exc, "status_code", None)
    if status is None:
        status = getattr(getattr(exc, "response", None), "status_code", None)
    return type(exc).__name__ + (f"[{status}]" if status is not None else "")


def _timed(rec: Recorder, stage: str, population: str, label: str, fn: Callable[[], Any]) -> Any:
    inflight = rec._enter(population)
    t0 = time.monotonic()
    ok, err = True, None
    try:
        return fn()
    except BaseException as exc:  # record then re-raise
        ok, err = False, _error_label(exc)
        raise
    finally:
        rec._exit(population, CallRecord(stage, population, t0, time.monotonic(), inflight, ok, err, label))


class InstrumentedUploadService:
    """Drop-in wrapper for upload_api.UploadService that times every Nominal-API call.

    `_verify` is proxied because the uploader's part-upload path (`_sign_part` then `_put_part`)
    reads it for the S3 PUT.
    """

    def __init__(self, inner: Any, recorder: Recorder) -> None:
        self._inner = inner
        self._rec = recorder

    @property
    def _verify(self) -> Any:
        return self._inner._verify

    def initiate_multipart_upload(self, auth_header: Any, request: Any) -> Any:
        label = getattr(request, "filename", "")
        return _timed(self._rec, "initiate", NOMINAL, label, lambda: self._inner.initiate_multipart_upload(auth_header, request))

    def sign_part(self, auth_header: Any, key: Any, part: Any, upload_id: Any) -> Any:
        return _timed(self._rec, "sign_part", NOMINAL, f"{key}#{part}", lambda: self._inner.sign_part(auth_header, key, part, upload_id))

    def list_parts(self, auth_header: Any, key: Any, upload_id: Any) -> Any:
        return _timed(self._rec, "list_parts", NOMINAL, str(key), lambda: self._inner.list_parts(auth_header, key, upload_id))

    def complete_multipart_upload(self, auth_header: Any, key: Any, upload_id: Any, parts: Any) -> Any:
        return _timed(self._rec, "complete", NOMINAL, str(key), lambda: self._inner.complete_multipart_upload(auth_header, key, upload_id, parts))

    def abort_multipart_upload(self, auth_header: Any, key: Any, upload_id: Any) -> Any:
        return _timed(self._rec, "abort", NOMINAL, str(key), lambda: self._inner.abort_multipart_upload(auth_header, key, upload_id))

    def upload_file(self, auth_header: Any, body: Any, file_name: Any, size_bytes: Any = None, workspace: Any = None) -> Any:
        # Single-shot small-file path: one control-plane call that also carries the body.
        label = f"{file_name} ({size_bytes}B)"
        return _timed(
            self._rec, "upload_file", NOMINAL, label,
            lambda: self._inner.upload_file(auth_header, body, file_name, size_bytes, workspace),
        )


class InstrumentedSession:
    """Wraps the S3 PUT session (requests.Session-like) to time each part PUT.

    Only `.put()` and `.close()` are used by the uploader; both are proxied.
    """

    def __init__(self, inner: Any, recorder: Recorder) -> None:
        self._inner = inner
        self._rec = recorder

    def put(self, url: str, data: Any = None, headers: Any = None, verify: Any = None, timeout: Any = None) -> Any:
        size = len(data) if isinstance(data, (bytes, bytearray)) else -1
        label = f"{url.split('?', 1)[0].rsplit('/', 1)[-1]} ({size}B)"
        return _timed(
            self._rec, "put", S3, label,
            lambda: self._inner.put(url, data=data, headers=headers, verify=verify, timeout=timeout),
        )

    def close(self) -> None:
        self._inner.close()


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------

_STAGE_ORDER = ("upload_file", "initiate", "sign_part", "put", "list_parts", "complete", "abort")


def _pct(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * q))]


_SPARK = " .:-=+*#@"  # ASCII ramp (0..8); avoids Windows cp1252 / notebook encoding issues


def _sparkline(values: list[int], vmax: float) -> str:
    if vmax <= 0:
        return " " * len(values)
    return "".join(_SPARK[min(8, int(round(v / vmax * 8)))] for v in values)


def _pool_composition(recs: list[CallRecord], t0: float, wall: float, max_workers: int | None) -> None:
    """Where does the pool's worker-time actually go, and how does it evolve?

    Tests the hypothesis 'mostly parts uploading in the pool at any given moment':
    a high S3-PUT share of busy time + a PUT-dominated timeline means the pool is
    spending its slots on bandwidth work, not rate-limited metadata.
    """
    busy: dict[str, float] = defaultdict(float)
    for r in recs:
        busy[r.population] += r.t_end - r.t_start
    total_busy = sum(busy.values()) or 1.0

    print("\n=== pool composition (where worker-time goes) ===")
    for pop in (S3, NOMINAL):
        b = busy.get(pop, 0.0)
        print(f"  {pop:8s}: {b:7.1f} worker-s  ({100 * b / total_busy:4.1f}% of busy time)  mean_concurrency={b / wall:4.1f}")
    print(f"  -> S3-PUT share of pool work = {100 * busy.get(S3, 0.0) / total_busy:.0f}%  (higher = 'mostly parts uploading' = your target)")

    n = 48
    times = [t0 + wall * (i + 0.5) / n for i in range(n)]
    nom = [sum(1 for r in recs if r.population == NOMINAL and r.t_start <= t < r.t_end) for t in times]
    s3 = [sum(1 for r in recs if r.population == S3 and r.t_start <= t < r.t_end) for t in times]
    vmax = max([*nom, *s3, max_workers or 1])
    print(f"\n  concurrency over time (ramp ' .:-=+*#@' = 0..{vmax} = pool size; left=start, right=end):")
    print(f"    nominal |{_sparkline(nom, vmax)}|")
    print(f"    s3-put  |{_sparkline(s3, vmax)}|")


def summarize(
    recorder: Recorder,
    *,
    rate_window_s: float = 0.5,
    slowest_n: int = 5,
    max_workers: int | None = None,
    limit_samples: list[tuple[float, float]] | None = None,
) -> None:
    recs = sorted(recorder.records, key=lambda r: r.t_start)
    if not recs:
        print("no calls recorded")
        return
    t0 = recs[0].t_start
    wall = recs[-1].t_end - t0
    by_stage: dict[str, list[CallRecord]] = defaultdict(list)
    for r in recs:
        by_stage[r.stage].append(r)

    print(f"\n=== per-stage latency ===  {len(recs)} calls over {wall:.1f}s")
    print(f"{'stage':10s} {'n':>6s} {'p50':>7s} {'p90':>7s} {'p99':>7s} {'max':>8s} {'fails':>6s}  {'window(s)':>14s}")
    for stage in _STAGE_ORDER:
        rs = by_stage.get(stage)
        if not rs:
            continue
        ms = sorted(r.ms for r in rs)
        fails = sum(1 for r in rs if not r.ok)
        first, last = min(r.t_start for r in rs) - t0, max(r.t_start for r in rs) - t0
        print(
            f"{stage:10s} {len(rs):6d} {_pct(ms, .50):6.0f}m {_pct(ms, .90):6.0f}m "
            f"{_pct(ms, .99):6.0f}m {ms[-1]:7.0f}m {fails:6d}  {f'[{first:.1f},{last:.1f}]':>14s}"
        )

    # Two-population concurrency + rate.
    print("\n=== concurrency & rate by population ===")
    for pop in (NOMINAL, S3):
        prs = [r for r in recs if r.population == pop]
        if not prs:
            continue
        peak_conc = max(r.inflight_at_start for r in prs)
        buckets: dict[int, int] = defaultdict(int)
        for r in prs:
            buckets[int((r.t_start - t0) / rate_window_s)] += 1
        peak_rate = max(buckets.values()) / rate_window_s
        print(f"  {pop:8s}: peak concurrency={peak_conc:3d}   peak rate={peak_rate:6.0f} req/s   calls={len(prs)}")

    _pool_composition(recs, t0, wall, max_workers)

    # Failures.
    fails = [r for r in recs if not r.ok]
    if fails:
        grouped: dict[tuple[str, str | None], int] = defaultdict(int)
        for r in fails:
            grouped[(r.stage, r.error)] += 1
        print(f"\n=== surfaced failures: {len(fails)} ===")
        for (stage, err), n in sorted(grouped.items(), key=lambda kv: -kv[1]):
            print(f"  {stage:10s} {err}: {n}")
    else:
        print("\nno surfaced failures — if a stage's p99/max >> p50, that's retry/backoff (likely 429s) being absorbed")

    # Outliers: slowest individual calls per stage.
    print(f"\n=== slowest {slowest_n} calls per stage (outliers) ===")
    for stage in _STAGE_ORDER:
        rs = by_stage.get(stage)
        if not rs:
            continue
        top = sorted(rs, key=lambda r: r.ms, reverse=True)[:slowest_n]
        shown = ", ".join(f"{r.ms:.0f}ms({r.label}{'' if r.ok else ' ' + str(r.error)})" for r in top)
        print(f"  {stage:10s} {shown}")

    if limit_samples:
        limits = [lim for _t, lim in limit_samples]
        ramp = " .:-=+*#@"
        observed_peak = max(limits)
        ceiling = max(observed_peak, float(max_workers or 0)) or 1.0
        line = "".join(ramp[min(len(ramp) - 1, int(lim / ceiling * (len(ramp) - 1)))] for lim in limits)
        print("\n=== adaptive concurrency limit over time ===")
        print(
            f"  min={min(limits):.2f}  mean={sum(limits) / len(limits):.2f}  "
            f"max={observed_peak:.2f}  ceiling={ceiling:.2f}"
        )
        print(f"  limit   |{line}|")
        print("  a limit that keeps sawtoothing never found the ceiling; one that pins flat found it early")


# --------------------------------------------------------------------------------------
# Real-backend helper: fully-instrumented uploader (wraps client AND session)
# --------------------------------------------------------------------------------------


def build_instrumented_uploader(
    clients: Any,
    *,
    max_workers: int = 8,
    max_files_in_flight: int | None = None,
    small_file_route_max_bytes: int | None = None,
    timeout: float = 30.0,
    max_part_retries: int = 3,
) -> tuple[Any, Recorder]:
    """Build a MultipartUploader whose Nominal client and S3 session are both timed.

    `clients` is a NominalClient's clients-bunch (e.g. `client._clients`). Returns
    (uploader, recorder); run your uploads through the uploader, close it, then
    call summarize(recorder). Set `max_files_in_flight` to bound how many files upload
    at once (backpressure at enqueue). Set `small_file_route_max_bytes` to route files
    at/below that size single-shot via upload_file (EXPERIMENTAL) — then the report's
    `upload_file` stage replaces the initiate/sign/put/list/complete rows.
    """
    from nominal.core._utils.multipart_uploader import MultipartUploader

    recorder = Recorder()
    up = MultipartUploader.create(
        upload_client=InstrumentedUploadService(clients.upload, recorder),
        auth_header=clients.auth_header,
        workspace_rid=clients.resolve_default_workspace_rid(),
        max_workers=max_workers,
        max_files_in_flight=max_files_in_flight,
        small_file_route_max_bytes=small_file_route_max_bytes,
        timeout=timeout,
        max_part_retries=max_part_retries,
        header_provider=clients.header_provider,
    )
    # Wrap the real session create() just built (keeps its TLS/retry/pool config).
    up._session = InstrumentedSession(up._session, recorder)  # type: ignore[assignment]
    return up, recorder


def verify_upload_file_roundtrip(clients: Any, *, size: int = 100_000, file_name: str = "_gzip_probe.bin") -> bool:
    """Prove the small-file route isn't gzip-corrupting objects.

    Uploads `size` random bytes via `upload_file`, downloads them back through a signed GET
    URL, and checks byte-identity. This client auto-gzips POST bodies; if the server doesn't
    decompress, the stored object is gzip-wrapped and this catches it. Returns True if clean.
    Leaves one small probe object in the uploads bucket.
    """
    import gzip
    import os

    from nominal_api import ingest_api

    from nominal.core._utils.networking import create_multipart_request_session

    payload = os.urandom(size)
    path = clients.upload.upload_file(
        clients.auth_header, payload, file_name, size_bytes=len(payload),
        workspace=clients.resolve_default_workspace_rid(),
    )
    resp = clients.upload.sign_download(clients.auth_header, ingest_api.SignDownloadRequest(path=path))
    session = create_multipart_request_session(pool_size=1)
    try:
        got = session.get(resp.url, timeout=60)
        got.raise_for_status()
        downloaded = got.content
    finally:
        session.close()

    if downloaded == payload:
        print(f"OK: upload_file round-trip is byte-identical ({size} bytes) — no gzip corruption. Path: {path}")
        return True
    print(f"MISMATCH: sent {size} bytes, got {len(downloaded)} back. Path: {path}")
    try:
        if gzip.decompress(downloaded) == payload:
            print(
                "  -> the stored object is GZIP-COMPRESSED: the server did not decompress the client's "
                "auto-gzipped body. upload_file needs a non-gzip transport before it can be used."
            )
    except Exception:
        print("  -> not plain gzip; inspect the bytes.")
    return False


# --------------------------------------------------------------------------------------
# Local, no-backend demo: drive the REAL MultipartUploader with latency-simulating fakes.
# --------------------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, url: str) -> None:
        self.url, self.status_code = url, 200
        self.headers = {"ETag": '"sim-etag"'}

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, latency_s: float, jitter: bool) -> None:
        self._latency, self._jitter = latency_s, jitter

    def _sleep(self) -> None:
        base = self._latency
        if self._jitter and base:
            base = max(0.0, random.gauss(base, base * 0.35)) + (base * 8 if random.random() < 0.02 else 0)  # 2% slow tail
        if base:
            time.sleep(base)

    def put(self, url: str, data: Any = None, headers: Any = None, verify: Any = None, timeout: Any = None) -> _FakeResponse:
        self._sleep()
        return _FakeResponse(url)

    def close(self) -> None:
        return None


class _FakeClient:
    def __init__(self, latency_s: float, jitter: bool) -> None:
        self._latency, self._jitter = latency_s, jitter
        self._verify, self._n = False, 0
        self._lock = threading.Lock()

    def _sleep(self) -> None:
        base = self._latency
        if self._jitter and base:
            base = max(0.0, random.gauss(base, base * 0.35)) + (base * 10 if random.random() < 0.02 else 0)
        if base:
            time.sleep(base)

    def initiate_multipart_upload(self, auth_header: Any, request: Any) -> Any:
        self._sleep()
        with self._lock:
            self._n += 1
            key = f"key-{self._n}"
        return type("R", (), {"key": key, "upload_id": f"uid-{key}"})()

    def sign_part(self, auth_header: Any, key: Any, part: Any, upload_id: Any) -> Any:
        self._sleep()
        return type("R", (), {"url": f"https://s3.example/{key}/{part}?sig=x", "headers": {}})()

    def list_parts(self, auth_header: Any, key: Any, upload_id: Any) -> Any:
        self._sleep()
        return [type("P", (), {"etag": "e", "part_number": 1})()]

    def complete_multipart_upload(self, auth_header: Any, key: Any, upload_id: Any, parts: Any) -> Any:
        self._sleep()
        return type("R", (), {"location": f"s3://bucket/{key}"})()

    def abort_multipart_upload(self, auth_header: Any, key: Any, upload_id: Any) -> Any:
        self._sleep()
        return None

    def upload_file(self, auth_header: Any, body: Any, file_name: Any, size_bytes: Any = None, workspace: Any = None) -> str:
        self._sleep()
        return f"s3://bucket/{file_name}"


def _run_demo(
    n_files: int, max_workers: int, api_ms: float, put_ms: float, jitter: bool, file_bytes: int, part_size: int,
    max_files_in_flight: int | None, small_file_route_max_bytes: int | None,
) -> None:
    from nominal.core._utils.multipart_uploader import MultipartUploader, _AdaptiveLimiter, _ThrottleGate
    from nominal.core.filetype import FileTypes

    rec = Recorder()
    client = InstrumentedUploadService(_FakeClient(api_ms / 1000.0, jitter), rec)
    session = InstrumentedSession(_FakeSession(put_ms / 1000.0, jitter), rec)
    parts_per_file = max(1, -(-file_bytes // part_size))  # ceil
    file_slots = threading.BoundedSemaphore(max_files_in_flight) if max_files_in_flight else None

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = pathlib.Path(tmp)
        paths = []
        for i in range(n_files):
            p = tmpdir / f"f{i}.bin"
            p.write_bytes(b"\0" * file_bytes)  # content is irrelevant; size drives the part count
            paths.append(p)

        up = MultipartUploader(
            max_workers=max_workers, timeout=30.0, max_part_retries=3,
            _upload_client=client, _auth_header="auth", _workspace_rid=None,
            _session=session, _pool=ThreadPoolExecutor(max_workers=max_workers),
            _gate=_ThrottleGate(_AdaptiveLimiter(initial=8, min_limit=1, max_limit=max_workers)),
            _closed=False, _file_slots=file_slots, _small_file_route_max_bytes=small_file_route_max_bytes,
        )
        print(
            f"running {n_files} files x {parts_per_file} part(s) each @ max_workers={max_workers}, "
            f"max_files_in_flight={max_files_in_flight}, api={api_ms}ms put={put_ms}ms jitter={jitter} ..."
        )
        t0 = time.monotonic()
        limit_samples: list[tuple[float, float]] = []
        stop = threading.Event()
        sampler = threading.Thread(target=sample_gate_limit, args=(up._gate, stop, limit_samples), daemon=True)
        sampler.start()
        try:
            try:
                futures = [up.enqueue_file(p, file_type=FileTypes.CSV, part_size=part_size) for p in paths]
                done = sum(1 for f in futures if _safe_result(f))
            finally:
                up.close()
        finally:
            stop.set()
            sampler.join(timeout=2.0)
        print(f"done: {done}/{n_files} in {time.monotonic() - t0:.1f}s")
    summarize(rec, max_workers=max_workers, limit_samples=limit_samples)


def _safe_result(fut: Any) -> bool:
    try:
        fut.result()
        return True
    except BaseException:
        return False


def main() -> None:
    ap = argparse.ArgumentParser(description="Per-stage timing for MultipartUploader (local fake-backed demo).")
    ap.add_argument("--files", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--api-latency-ms", type=float, default=40.0)
    ap.add_argument("--put-latency-ms", type=float, default=15.0)
    ap.add_argument("--file-bytes", type=int, default=8, help="size of each generated file")
    ap.add_argument(
        "--part-size", type=int, default=64_000_000,
        help="multipart chunk size; a SMALL value makes each file multi-part (simulates 'few big files')",
    )
    ap.add_argument("--max-files-in-flight", type=int, default=None, help="cap concurrent files (backpressure at enqueue)")
    ap.add_argument("--small-file-route-max-bytes", type=int, default=None, help="route files <= this single-shot via upload_file")
    ap.add_argument("--jitter", action="store_true", help="add gaussian jitter + a 2%% slow tail so outlier reporting is visible")
    args = ap.parse_args()
    _run_demo(
        args.files, args.workers, args.api_latency_ms, args.put_latency_ms, args.jitter, args.file_bytes,
        args.part_size, args.max_files_in_flight, args.small_file_route_max_bytes,
    )


if __name__ == "__main__":
    main()
