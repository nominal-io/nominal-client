#!/usr/bin/env python3
"""Generate one continuous 1Hz sinus dataset and cut offset 30-minute runs from it.

Default layout (overridable via flags):

- 10,000 runs, each 30 minutes long
- 30-minute pause between runs
- Continuous 1Hz sine on one channel across the full timeline (pauses included)
- Each run has numeric typed property ``run_instance`` from 1 to N

Public ``NominalClient.create_run`` only accepts string properties. This script
uses ``CreateRunRequest.typed_properties`` so ``run_instance`` is numeric and
filterable in the Nominal UI.

Usage::

    uv run python scripts/generate_offset_runs.py --profile PROFILE
    uv run python scripts/generate_offset_runs.py --dry-run --count 2
"""

from __future__ import annotations

import argparse
import gzip
import logging
import math
import random
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import numpy as np
from conjure_python_client import ConjureHTTPError
from dateutil import parser as dateutil_parser
from nominal_api import api, scout_run_api
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from nominal.core._constants import DEFAULT_API_BASE_URL
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset
from nominal.ts import _SecondsNanos

logger = logging.getLogger(__name__)

DEFAULT_COUNT = 10_000
DEFAULT_RUN_SECONDS = 1_800
DEFAULT_PAUSE_SECONDS = 1_800
DEFAULT_PERIOD_SECONDS = 60.0
DEFAULT_CHANNEL = "sine"
DEFAULT_CHUNK_SIZE = 1_000_000
DEFAULT_WORKERS = 8
DEFAULT_RUN_NAME_PREFIX = "Offset run "
DEFAULT_LABEL = "offset-runs"
DEFAULT_REF_NAME = "dataset"
RUN_INSTANCE_PROPERTY = "run_instance"
MAX_CREATE_ATTEMPTS = 5
PROGRESS_EVERY = 100


@dataclass(frozen=True)
class RunWindow:
    """Half-open time window ``[start_epoch_s, end_epoch_s)`` for one run."""

    instance: int
    start_epoch_s: int
    end_epoch_s: int


def total_samples(count: int, run_seconds: int, pause_seconds: int) -> int:
    """Number of 1Hz samples from the first run start through the last run end.

    The pause after the last run is omitted.
    """
    if count <= 0:
        raise ValueError(f"count must be >= 1, got {count}")
    if run_seconds < 1:
        raise ValueError(f"run_seconds must be >= 1, got {run_seconds}")
    if pause_seconds < 0:
        raise ValueError(f"pause_seconds must be >= 0, got {pause_seconds}")
    return count * run_seconds + (count - 1) * pause_seconds


def dataset_end_epoch_s(start_epoch_s: int, count: int, run_seconds: int, pause_seconds: int) -> int:
    """Exclusive end timestamp of the dataset (last run end)."""
    return start_epoch_s + total_samples(count, run_seconds, pause_seconds)


def run_window(instance: int, start_epoch_s: int, run_seconds: int, pause_seconds: int) -> RunWindow:
    """Return the half-open window for 1-based run ``instance``."""
    if instance < 1:
        raise ValueError(f"instance must be >= 1, got {instance}")
    offset = (instance - 1) * (run_seconds + pause_seconds)
    start = start_epoch_s + offset
    return RunWindow(instance=instance, start_epoch_s=start, end_epoch_s=start + run_seconds)


def run_windows(count: int, start_epoch_s: int, run_seconds: int, pause_seconds: int) -> list[RunWindow]:
    """Return windows for runs ``1..count``."""
    if count <= 0:
        raise ValueError(f"count must be >= 1, got {count}")
    return [run_window(i, start_epoch_s, run_seconds, pause_seconds) for i in range(1, count + 1)]


def chunk_ranges(total: int, chunk_size: int) -> list[tuple[int, int]]:
    """Split ``total`` samples into ``(offset, size)`` chunks."""
    if total < 0:
        raise ValueError(f"total must be >= 0, got {total}")
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    return [(offset, min(chunk_size, total - offset)) for offset in range(0, total, chunk_size)]


def sine_values(offsets: Sequence[int] | np.ndarray, period_seconds: float) -> np.ndarray:
    """1Hz sine samples for integer second offsets from the dataset start."""
    if period_seconds <= 0:
        raise ValueError(f"period_seconds must be > 0, got {period_seconds}")
    offs = np.asarray(offsets, dtype=np.float64)
    return np.sin(2.0 * math.pi * offs / period_seconds)


def write_sine_chunk_csv_gz(
    path: Path,
    *,
    dataset_start_epoch_s: int,
    chunk_offset: int,
    sample_count: int,
    channel: str,
    period_seconds: float,
) -> None:
    """Write one gzip CSV chunk of ``timestamp,channel`` at 1Hz."""
    offsets = np.arange(chunk_offset, chunk_offset + sample_count, dtype=np.int64)
    timestamps = dataset_start_epoch_s + offsets
    values = sine_values(offsets, period_seconds)
    header = f"timestamp,{channel}\n"
    with gzip.open(path, "wt", compresslevel=1, encoding="ascii") as handle:
        handle.write(header)
        np.savetxt(handle, np.column_stack((timestamps, values)), fmt=["%d", "%.10g"], delimiter=",")


def parse_start(value: str | None) -> datetime:
    """Parse an ISO-8601 start, defaulting to the current UTC second."""
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0)
    parsed = dateutil_parser.isoparse(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def format_utc(epoch_s: int) -> str:
    """Format an epoch second as ISO-8601 UTC."""
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_retryable_create_error(error: BaseException) -> bool:
    if isinstance(error, ConjureHTTPError) and error.response is not None:
        status = error.response.status_code
        return status in (408, 429) or status >= 500
    return isinstance(error, (RequestsConnectionError, RequestsTimeout, TimeoutError, ConnectionError))


def _create_run_request(
    *,
    window: RunWindow,
    dataset_rid: str,
    workspace_rid: str | None,
    run_name_prefix: str,
    labels: Sequence[str],
) -> scout_run_api.CreateRunRequest:
    return scout_run_api.CreateRunRequest(
        assets=[],
        attachments=[],
        data_sources={
            DEFAULT_REF_NAME: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(dataset=dataset_rid),
                series_tags={},
                offset=None,
            )
        },
        description=f"Offset run window with numeric {RUN_INSTANCE_PROPERTY}={window.instance}",
        labels=list(labels),
        links=[],
        properties={},
        start_time=_SecondsNanos(window.start_epoch_s, 0).to_scout_run_api(),
        title=f"{run_name_prefix}{window.instance}",
        typed_properties={
            RUN_INSTANCE_PROPERTY: api.TypedPropertyValue(numeric_value=float(window.instance)),
        },
        end_time=_SecondsNanos(window.end_epoch_s, 0).to_scout_run_api(),
        workspace=workspace_rid,
    )


def create_offset_run(
    client: NominalClient,
    *,
    window: RunWindow,
    dataset_rid: str,
    workspace_rid: str | None,
    run_name_prefix: str,
    labels: Sequence[str],
) -> str:
    """Create one run, retrying transient HTTP failures. Returns the run RID."""
    request = _create_run_request(
        window=window,
        dataset_rid=dataset_rid,
        workspace_rid=workspace_rid,
        run_name_prefix=run_name_prefix,
        labels=labels,
    )
    last_error: BaseException | None = None
    for attempt in range(1, MAX_CREATE_ATTEMPTS + 1):
        try:
            created = client._clients.run.create_run(client._clients.auth_header, request)
            return created.rid
        except Exception as exc:
            last_error = exc
            if not _is_retryable_create_error(exc) or attempt == MAX_CREATE_ATTEMPTS:
                raise
            sleep_s = min(2 ** (attempt - 1), 30) + random.random()
            logger.warning(
                "Retrying create for run_instance=%s after %s (attempt %s/%s, sleep %.1fs)",
                window.instance,
                exc,
                attempt,
                MAX_CREATE_ATTEMPTS,
                sleep_s,
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"failed to create run_instance={window.instance}") from last_error


def build_client(*, profile: str | None, token: str | None, base_url: str) -> NominalClient:
    if profile:
        return NominalClient.from_profile(profile)
    if token:
        return NominalClient.from_token(token, base_url)
    raise SystemExit("provide --profile or --token")


def print_plan(
    *,
    start_epoch_s: int,
    count: int,
    run_seconds: int,
    pause_seconds: int,
    channel: str,
    period_seconds: float,
    chunk_size: int,
    dataset_name: str,
    run_name_prefix: str,
) -> None:
    samples = total_samples(count, run_seconds, pause_seconds)
    end_epoch_s = dataset_end_epoch_s(start_epoch_s, count, run_seconds, pause_seconds)
    windows = run_windows(count, start_epoch_s, run_seconds, pause_seconds)
    chunks = chunk_ranges(samples, chunk_size)
    print(f"dataset_name: {dataset_name}")
    print(f"count:        {count}")
    print(f"start:        {format_utc(start_epoch_s)}")
    print(f"end:          {format_utc(end_epoch_s)}")
    print(f"samples:      {samples}")
    print(f"channel:      {channel}")
    print(f"period_s:     {period_seconds}")
    print(f"run_seconds:  {run_seconds}")
    print(f"pause_seconds:{pause_seconds}")
    print(f"chunks:       {len(chunks)} (chunk_size={chunk_size})")
    print(f"run_prefix:   {run_name_prefix!r}")
    print(
        f"first_run:    [{format_utc(windows[0].start_epoch_s)}, {format_utc(windows[0].end_epoch_s)}) "
        f"{RUN_INSTANCE_PROPERTY}={windows[0].instance}"
    )
    print(
        f"last_run:     [{format_utc(windows[-1].start_epoch_s)}, {format_utc(windows[-1].end_epoch_s)}) "
        f"{RUN_INSTANCE_PROPERTY}={windows[-1].instance}"
    )


def ingest_dataset(
    client: NominalClient,
    *,
    dataset_name: str,
    start_epoch_s: int,
    samples: int,
    channel: str,
    period_seconds: float,
    chunk_size: int,
) -> Dataset:
    dataset = client.create_dataset(
        name=dataset_name,
        description=(
            f"Continuous 1Hz {channel} sinus for {samples} samples, "
            f"cut into offset runs tagged with {RUN_INSTANCE_PROPERTY}"
        ),
        labels=[DEFAULT_LABEL],
    )
    logger.info("Created dataset %s (%s)", dataset.name, dataset.rid)

    last_file = None
    chunks = chunk_ranges(samples, chunk_size)
    with tempfile.TemporaryDirectory(prefix="offset-runs-") as tmp:
        tmp_path = Path(tmp)
        for index, (offset, size) in enumerate(chunks, start=1):
            chunk_path = tmp_path / f"sine-{index:05d}.csv.gz"
            logger.info("Writing chunk %s/%s (%s samples, offset=%s)", index, len(chunks), size, offset)
            write_sine_chunk_csv_gz(
                chunk_path,
                dataset_start_epoch_s=start_epoch_s,
                chunk_offset=offset,
                sample_count=size,
                channel=channel,
                period_seconds=period_seconds,
            )
            last_file = dataset.add_tabular_data(
                chunk_path,
                timestamp_column="timestamp",
                timestamp_type="epoch_seconds",
            )
            logger.info("Uploaded chunk %s/%s as %s", index, len(chunks), last_file.id)

    if last_file is None:
        raise RuntimeError("no data chunks were ingested")
    logger.info("Waiting for ingest of final chunk to complete")
    last_file.poll_until_ingestion_completed()
    return dataset


def create_runs(
    client: NominalClient,
    *,
    dataset_rid: str,
    windows: Sequence[RunWindow],
    run_name_prefix: str,
    labels: Sequence[str],
    workers: int,
) -> list[str]:
    workspace_rid = client._clients.resolve_default_workspace_rid()
    rids_by_instance: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                create_offset_run,
                client,
                window=window,
                dataset_rid=dataset_rid,
                workspace_rid=workspace_rid,
                run_name_prefix=run_name_prefix,
                labels=labels,
            ): window.instance
            for window in windows
        }
        for future in as_completed(futures):
            instance = futures[future]
            rids_by_instance[instance] = future.result()
            completed = len(rids_by_instance)
            if completed % PROGRESS_EVERY == 0 or completed == len(windows):
                logger.info(
                    "Created %s/%s runs (latest run_instance=%s rid=%s)",
                    completed,
                    len(windows),
                    instance,
                    rids_by_instance[instance],
                )
    return [rids_by_instance[window.instance] for window in windows]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    auth = parser.add_mutually_exclusive_group()
    auth.add_argument("--profile", help="Nominal config profile name")
    auth.add_argument("--token", help="Raw Nominal API token")
    parser.add_argument("--base-url", default=DEFAULT_API_BASE_URL, help="API base URL when using --token")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of runs to create (default: 10000)")
    parser.add_argument("--start", help="ISO-8601 UTC start of the first run (default: now)")
    parser.add_argument("--run-seconds", type=int, default=DEFAULT_RUN_SECONDS, help="Run duration in seconds")
    parser.add_argument(
        "--pause-seconds", type=int, default=DEFAULT_PAUSE_SECONDS, help="Pause between runs in seconds"
    )
    parser.add_argument("--channel", default=DEFAULT_CHANNEL, help="Channel name for the sinus")
    parser.add_argument("--period-seconds", type=float, default=DEFAULT_PERIOD_SECONDS, help="Sine period in seconds")
    parser.add_argument("--dataset-name", default=None, help="Dataset name (default: includes start timestamp)")
    parser.add_argument(
        "--run-name-prefix", default=DEFAULT_RUN_NAME_PREFIX, help="Run title prefix before the instance"
    )
    parser.add_argument("--label", default=DEFAULT_LABEL, help="Label applied to the dataset and every run")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Samples per uploaded CSV.gz chunk")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel run-create workers")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan and exit without creating resources")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.count < 1:
        parser.error("--count must be >= 1")
    if args.run_seconds < 1:
        parser.error("--run-seconds must be >= 1")
    if args.pause_seconds < 0:
        parser.error("--pause-seconds must be >= 0")
    if args.period_seconds <= 0:
        parser.error("--period-seconds must be > 0")
    if args.chunk_size < 1:
        parser.error("--chunk-size must be >= 1")
    if args.workers < 1:
        parser.error("--workers must be >= 1")

    start = parse_start(args.start)
    start_epoch_s = int(start.timestamp())
    dataset_name = args.dataset_name or f"Offset-runs sinus {format_utc(start_epoch_s)}"

    print_plan(
        start_epoch_s=start_epoch_s,
        count=args.count,
        run_seconds=args.run_seconds,
        pause_seconds=args.pause_seconds,
        channel=args.channel,
        period_seconds=args.period_seconds,
        chunk_size=args.chunk_size,
        dataset_name=dataset_name,
        run_name_prefix=args.run_name_prefix,
    )
    if args.dry_run:
        return 0
    if not args.profile and not args.token:
        parser.error("provide --profile or --token (or pass --dry-run)")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    samples = total_samples(args.count, args.run_seconds, args.pause_seconds)
    windows = run_windows(args.count, start_epoch_s, args.run_seconds, args.pause_seconds)
    client = build_client(profile=args.profile, token=args.token, base_url=args.base_url)
    dataset = ingest_dataset(
        client,
        dataset_name=dataset_name,
        start_epoch_s=start_epoch_s,
        samples=samples,
        channel=args.channel,
        period_seconds=args.period_seconds,
        chunk_size=args.chunk_size,
    )
    rids = create_runs(
        client,
        dataset_rid=dataset.rid,
        windows=windows,
        run_name_prefix=args.run_name_prefix,
        labels=[args.label],
        workers=args.workers,
    )
    print(f"dataset_rid: {dataset.rid}")
    print(f"runs_created: {len(rids)}")
    print(f"first_run_rid: {rids[0]}")
    print(f"last_run_rid: {rids[-1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
