"""Backfill the channel data a destination dataset is missing, from a source dataset in another tenant.

This is a runnable driver for ``sync_missing_channel_data`` (the same logic behind
``nom migrate sync-channels``). It reads a YAML config, builds a client for the source tenant and
one for the destination tenant, resolves a dataset on each, then:

  1. detects, per channel and per time bucket, where the destination has fewer points than the
     source over the window;
  2. exports only the short ranges from the source via presigned links (PolarsExportHandler);
  3. streams the points into the destination dataset (auto-creating series); and
  4. waits for ingestion to settle, re-detects, and re-streams anything still short.

Anything that cannot be filled is logged (channel + tags + time-slice) and listed in the report.

Usage:
    uv run python nominal/experimental/migration/channel_sync/sync_channel_wrapper.py config.yml

See nominal/experimental/migration/channel_sync/ for the sync config schema.
"""

from __future__ import annotations

import argparse
import datetime
import gzip
import logging
import pathlib
import sys
from typing import Any, Mapping

import yaml

from nominal.core.client import NominalClient
from nominal.experimental.migration.channel_sync import (
    ChannelSyncOptions,
    sync_missing_channel_data,
    sync_missing_channel_data_for_tag_filters,
)

logger = logging.getLogger("sync_channel_wrapper")


def _iso_to_nanos(value: str) -> int:
    """Parse an ISO 8601 timestamp (UTC; a trailing ``Z`` is accepted) into integer nanoseconds."""
    dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def _build_client(cfg: Mapping[str, Any], label: str) -> NominalClient:
    """Build a NominalClient from either an explicit base_url/token or a named profile."""
    base_url = cfg.get("base_url")
    if base_url:
        logger.info("[%s] Connecting to %s", label, base_url)
        return NominalClient.create(
            base_url=base_url,
            token=cfg.get("token"),
            workspace_rid=cfg.get("workspace_rid"),
        )
    profile = cfg.get("profile", "default")
    logger.info("[%s] Connecting using profile %r", label, profile)
    return NominalClient.from_profile(profile)


def sync_from_config(cfg: Mapping[str, Any]) -> None:
    """Run sync_missing_channel_data() driven by the parsed `sync` config block."""
    source_cfg = cfg["source"]
    destination_cfg = cfg["destination"]

    phase = cfg.get("phase", "all")
    source_client = _build_client(source_cfg, "source")
    source_dataset = source_client.get_dataset(source_cfg["dataset_rid"])
    logger.info("Source dataset: %s", source_dataset.rid)

    if destination_cfg:
        destination_client = _build_client(destination_cfg, "destination")
        destination_dataset = destination_client.get_dataset(destination_cfg["dataset_rid"])
        logger.info("Destination dataset: %s", destination_dataset.rid)
    else:
        destination_dataset = None

    output_dir = cfg.get("output_dir")
    raw_allowlist = cfg.get("channel_allowlist")
    channel_allowlist: frozenset[str] | None = frozenset(raw_allowlist) if raw_allowlist else None
    defaults = ChannelSyncOptions()
    base_options = ChannelSyncOptions(
        bucket=int(float(cfg.get("bucket_seconds", 3600)) * 1_000_000_000),
        max_retries=int(cfg.get("max_retries", 2)),
        settle_seconds=float(cfg.get("settle_seconds", 30)),
        detect_workers=int(cfg.get("detect_workers", defaults.detect_workers)),
        detect_channels_per_request=int(cfg.get("detect_channels_per_request", defaults.detect_channels_per_request)),
        detect_request_delay=float(cfg.get("detect_request_delay", defaults.detect_request_delay)),
        num_workers=int(cfg.get("num_workers", defaults.num_workers)),
        batch_size=int(cfg.get("batch_size", defaults.batch_size)),
        points_per_request=int(cfg.get("points_per_request", defaults.points_per_request)),
        points_per_dataframe=int(cfg.get("points_per_dataframe", defaults.points_per_dataframe)),
        channels_per_request=int(cfg.get("channels_per_request", defaults.channels_per_request)),
        max_concurrent_links=int(cfg.get("max_concurrent_links", defaults.max_concurrent_links)),
        show_progress=bool(cfg.get("show_progress", defaults.show_progress)),
        output_dir=pathlib.Path(output_dir) if output_dir else None,
        phase=phase,
        channel_allowlist=channel_allowlist,
    )

    start_ns = _iso_to_nanos(cfg["start"])
    end_ns = _iso_to_nanos(cfg["end"])

    tag_filters = cfg.get("tag_filters") or None
    expand_underconstrained = bool(cfg.get("expand_underconstrained_tags", False))
    if tag_filters or phase == "stream":
        reports = sync_missing_channel_data_for_tag_filters(
            source_dataset,
            source_client,
            destination_dataset,
            start=start_ns,
            end=end_ns,
            tag_filters=tag_filters,  # None for stream = auto-discover from output_dir
            base_options=base_options,
            expand_underconstrained=expand_underconstrained,
        )
        for report in reports:
            _log_report(report)
    else:
        from dataclasses import replace
        options = replace(base_options, tags=cfg.get("tags") or None)
        report = sync_missing_channel_data(
            source_dataset,
            source_client,
            destination_dataset,
            start=start_ns,
            end=end_ns,
            options=options,
        )
        _log_report(report)


def _log_channel_examples(output_dir: pathlib.Path, tag_filter: Mapping[str, Any], max_per_range: int = 5) -> None:
    """After a download, log example channel names grouped by time range for manual verification."""
    if not output_dir.exists():
        return
    from collections import defaultdict
    range_channels: dict[tuple[int, int], set[str]] = defaultdict(set)
    for f in sorted(output_dir.glob("*.csv.gz")):
        parts = f.name.split("_")
        if len(parts) < 3 or parts[0] != "sync":
            continue
        try:
            file_start = int(parts[1])
            file_end = int(parts[2])
        except ValueError:
            continue
        with gzip.open(f, "rb") as fh:
            header = fh.readline().decode().strip()
        cols = {c.strip().strip('"') for c in header.split(",") if c.strip().strip('"') != "timestamp"}
        range_channels[(file_start, file_end)].update(cols)
    if not range_channels:
        return
    def _fmt(ns: int) -> str:
        return datetime.datetime.utcfromtimestamp(ns / 1e9).strftime("%Y-%m-%d %H:%M UTC")

    tag_str = " ".join(f"{k}={v}" for k, v in tag_filter.items())
    logger.info("--- Channel examples for %s ---", tag_str)
    for (rstart, rend), channels in sorted(range_channels.items()):
        examples = ", ".join(sorted(channels)[:max_per_range])
        logger.info("  [%s, %s)  %d channel(s) — e.g. %s", _fmt(rstart), _fmt(rend), len(channels), examples)


def _log_report(report: Any) -> None:
    logger.info("--- Sync report ---")
    logger.info("Channels examined:          %d", report.channels_examined)
    logger.info("Channels skipped (type):    %d", report.channels_skipped_unsupported)
    logger.info("Channels initially missing: %d", report.channels_missing)
    logger.info("Channels fully synced:      %d", report.channels_synced)
    logger.info("Points streamed:            %d", report.points_streamed)
    logger.info("Still short:                %d", len(report.still_short))
    for entry in report.still_short:
        logger.warning(
            "  still short: channel=%r tags=%s range=[%d, %d)",
            entry.channel,
            entry.tags,
            entry.time_range[0],
            entry.time_range[1],
        )
    already_present = getattr(report, "channels_already_present", [])
    if already_present:
        logger.info("--- Channels already present in destination (%d) ---", len(already_present))
        for name in already_present:
            logger.info("  %s", name)


def _configure_logging(cfg: Mapping[str, Any], cli_log_file: str | None) -> None:
    """Route logging to a file (when configured) or stdout.

    Precedence: ``--log-file`` CLI flag > ``log_file`` config key > stdout.
    """
    level_name = str(cfg.get("log_level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    log_file = cli_log_file or cfg.get("log_file")
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"

    if log_file:
        handler: logging.Handler = logging.FileHandler(log_file, mode="w")
        print(f"Logging to {log_file} at level {level_name}")
    else:
        handler = logging.StreamHandler()

    # force=True replaces any handlers a prior basicConfig (or import side effect) installed.
    logging.basicConfig(level=level, format=fmt, handlers=[handler], force=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=pathlib.Path, help="Path to the sync YAML config")
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write logs to this file instead of stdout (overrides the config's log_file).",
    )
    args = parser.parse_args(argv)

    raw = yaml.safe_load(args.config.read_text())
    cfg = raw["sync"]

    _configure_logging(cfg, args.log_file)
    sync_from_config(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
