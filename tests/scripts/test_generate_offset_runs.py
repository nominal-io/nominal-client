from __future__ import annotations

import gzip
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest
from generate_offset_runs import (
    DEFAULT_PAUSE_SECONDS,
    DEFAULT_REF_NAME,
    DEFAULT_RUN_SECONDS,
    RUN_INSTANCE_PROPERTY,
    RunWindow,
    _create_run_request,
    chunk_ranges,
    dataset_end_epoch_s,
    format_utc,
    main,
    parse_start,
    run_window,
    run_windows,
    sine_values,
    total_samples,
    write_sine_chunk_csv_gz,
)


def test_total_samples_omits_trailing_pause() -> None:
    """10k * 30min runs plus 9999 * 30min pauses is 35,998,200 1Hz samples."""
    assert total_samples(10_000, DEFAULT_RUN_SECONDS, DEFAULT_PAUSE_SECONDS) == 35_998_200


def test_total_samples_count_two_matches_dry_run_plan() -> None:
    """Two 30-minute runs with a 30-minute gap is 90 minutes of 1Hz data."""
    assert total_samples(2, DEFAULT_RUN_SECONDS, DEFAULT_PAUSE_SECONDS) == 5_400


def test_total_samples_single_run_has_no_pause() -> None:
    """A single run spans only the run duration."""
    assert total_samples(1, DEFAULT_RUN_SECONDS, DEFAULT_PAUSE_SECONDS) == DEFAULT_RUN_SECONDS


def test_total_samples_rejects_invalid_args() -> None:
    """Zero/negative layout values are rejected."""
    with pytest.raises(ValueError, match="count"):
        total_samples(0, 10, 10)
    with pytest.raises(ValueError, match="run_seconds"):
        total_samples(1, 0, 10)
    with pytest.raises(ValueError, match="pause_seconds"):
        total_samples(1, 10, -1)


def test_run_windows_are_offset_by_run_plus_pause() -> None:
    """Run i starts after (i-1) full cycles; adjacent runs are separated by the pause."""
    start = 1_700_000_000
    windows = run_windows(3, start, run_seconds=30, pause_seconds=30)

    assert windows[0] == RunWindow(instance=1, start_epoch_s=start, end_epoch_s=start + 30)
    assert windows[1] == RunWindow(instance=2, start_epoch_s=start + 60, end_epoch_s=start + 90)
    assert windows[2] == RunWindow(instance=3, start_epoch_s=start + 120, end_epoch_s=start + 150)
    assert windows[1].start_epoch_s - windows[0].end_epoch_s == 30
    assert windows[2].start_epoch_s - windows[1].end_epoch_s == 30


def test_last_run_ends_at_dataset_end() -> None:
    """The last run's exclusive end is the dataset exclusive end."""
    start = 0
    count = 4
    run_s, pause_s = 10, 5
    windows = run_windows(count, start, run_s, pause_s)
    assert windows[-1].end_epoch_s == dataset_end_epoch_s(start, count, run_s, pause_s)
    assert windows[-1].end_epoch_s == total_samples(count, run_s, pause_s)


def test_run_window_is_one_based() -> None:
    """Instance numbers are 1-based."""
    with pytest.raises(ValueError, match="instance"):
        run_window(0, 0, 10, 10)


def test_chunk_ranges_cover_the_full_span() -> None:
    """Chunks are contiguous, sized to chunk_size, and sum to the total."""
    ranges = chunk_ranges(2_500_000, 1_000_000)
    assert ranges == [(0, 1_000_000), (1_000_000, 1_000_000), (2_000_000, 500_000)]
    assert sum(size for _, size in ranges) == 2_500_000


def test_chunk_ranges_rejects_bad_size() -> None:
    """chunk_size must be at least 1."""
    with pytest.raises(ValueError, match="chunk_size"):
        chunk_ranges(10, 0)


def test_sine_values_match_one_cycle_per_period() -> None:
    """A 60-second period is 0 at t=0, 1 at t=15, 0 at t=30, -1 at t=45."""
    values = sine_values([0, 15, 30, 45, 60], period_seconds=60.0)
    expected = [
        0.0,
        1.0,
        0.0,
        -1.0,
        0.0,
    ]
    np.testing.assert_allclose(values, expected, atol=1e-12)


def test_sine_is_continuous_across_chunk_boundary() -> None:
    """Sine phase is computed from the dataset-relative offset, not the chunk start."""
    period = 60.0
    left = sine_values([999_999], period)
    right = sine_values([1_000_000], period)
    expected_left = math.sin(2.0 * math.pi * 999_999 / period)
    expected_right = math.sin(2.0 * math.pi * 1_000_000 / period)
    np.testing.assert_allclose(left, [expected_left])
    np.testing.assert_allclose(right, [expected_right])


def test_write_sine_chunk_csv_gz_uses_absolute_timestamps(tmp_path: Path) -> None:
    """CSV rows are epoch-second timestamps and sine values from the dataset start."""
    path = tmp_path / "chunk.csv.gz"
    write_sine_chunk_csv_gz(
        path,
        dataset_start_epoch_s=1_000,
        chunk_offset=10,
        sample_count=3,
        channel="sine",
        period_seconds=60.0,
    )
    with gzip.open(path, "rt", encoding="ascii") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    assert lines[0] == "timestamp,sine"
    rows = [line.split(",") for line in lines[1:]]
    assert [int(ts) for ts, _ in rows] == [1_010, 1_011, 1_012]
    written = [float(value) for _, value in rows]
    np.testing.assert_allclose(written, sine_values([10, 11, 12], 60.0), atol=1e-9)


def test_parse_start_defaults_to_utc_now_without_micros() -> None:
    """A missing --start value is the current UTC second."""
    parsed = parse_start(None)
    assert parsed.tzinfo is not None
    assert parsed.microsecond == 0
    assert abs((parsed - datetime.now(timezone.utc)).total_seconds()) < 2


def test_parse_start_accepts_naive_iso_as_utc() -> None:
    """Naive ISO-8601 strings are treated as UTC."""
    parsed = parse_start("2026-01-01T00:00:00")
    assert parsed == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_format_utc_is_zulu() -> None:
    """Plan output uses a trailing Z, not +00:00."""
    assert format_utc(0) == "1970-01-01T00:00:00Z"


def test_dry_run_count_two_prints_expected_plan(capsys: pytest.CaptureFixture[str]) -> None:
    """--dry-run --count 2 prints the 5400-sample plan and creates nothing."""
    exit_code = main(
        [
            "--dry-run",
            "--count",
            "2",
            "--start",
            "2026-01-01T00:00:00Z",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "count:        2" in captured.out
    assert "samples:      5400" in captured.out
    assert "start:        2026-01-01T00:00:00Z" in captured.out
    assert "end:          2026-01-01T01:30:00Z" in captured.out
    assert "first_run:    [2026-01-01T00:00:00Z, 2026-01-01T00:30:00Z) run_instance=1" in captured.out
    assert "last_run:     [2026-01-01T01:00:00Z, 2026-01-01T01:30:00Z) run_instance=2" in captured.out


def test_create_run_request_uses_numeric_run_instance() -> None:
    """Runs carry typed numeric run_instance and attach the shared dataset."""
    window = RunWindow(instance=42, start_epoch_s=1_000, end_epoch_s=2_800)
    request = _create_run_request(
        window=window,
        dataset_rid="ri.catalog.dataset.dev.example",
        workspace_rid="ri.workspace.dev.example",
        run_name_prefix="Offset run ",
        labels=["offset-runs"],
    )

    typed = request.typed_properties[RUN_INSTANCE_PROPERTY]
    assert typed.numeric_value == 42.0
    assert typed.string_value is None
    assert request.properties == {}
    assert request.title == "Offset run 42"
    assert request.start_time.seconds_since_epoch == 1_000
    assert request.end_time is not None
    assert request.end_time.seconds_since_epoch == 2_800
    assert request.data_sources[DEFAULT_REF_NAME].data_source.dataset == "ri.catalog.dataset.dev.example"


def test_dry_run_default_count_prints_10k_sample_total(capsys: pytest.CaptureFixture[str]) -> None:
    """Default --count is 10k and the printed sample count matches the helper."""
    exit_code = main(["--dry-run", "--start", "2026-01-01T00:00:00Z"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "count:        10000" in captured.out
    assert "samples:      35998200" in captured.out
    assert "chunks:       36 (chunk_size=1000000)" in captured.out
