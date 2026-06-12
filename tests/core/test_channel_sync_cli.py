from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

import click
from click.testing import CliRunner

from nominal.core.client import NominalClient
from nominal.experimental.migration import migration_cli
from nominal.experimental.migration.channel_sync import ChannelSyncReport

SEC = 1_000_000_000


# --- _parse_tags --------------------------------------------------------------------------


def test_parse_tags_parses_key_values() -> None:
    assert migration_cli._parse_tags(("a=1", "b=2")) == {"a": "1", "b": "2"}


def test_parse_tags_allows_empty_value() -> None:
    assert migration_cli._parse_tags(("a=",)) == {"a": ""}


def test_parse_tags_empty() -> None:
    assert migration_cli._parse_tags(()) == {}


@pytest.mark.parametrize("bad", ["nokey", "=value"])
def test_parse_tags_rejects_malformed(bad: str) -> None:
    with pytest.raises(click.BadParameter):
        migration_cli._parse_tags((bad,))


# --- sync-channels command wiring ---------------------------------------------------------


def test_sync_channels_command_wires_args(monkeypatch: pytest.MonkeyPatch) -> None:
    datasets = {"src-rid": SimpleNamespace(rid="src-rid"), "dst-rid": SimpleNamespace(rid="dst-rid")}
    fake_client = SimpleNamespace(get_dataset=lambda rid: datasets[rid])
    monkeypatch.setattr(NominalClient, "from_profile", classmethod(lambda cls, *a, **k: fake_client))

    captured: dict[str, Any] = {}

    def fake_sync(
        source_dataset: Any,
        source_client: Any,
        destination_dataset: Any,
        start: int,
        end: int,
        options: Any,
    ) -> ChannelSyncReport:
        captured.update(
            source=source_dataset.rid,
            destination=destination_dataset.rid,
            start=start,
            end=end,
            bucket=options.bucket,
            tags=options.tags,
            max_retries=options.max_retries,
        )
        return ChannelSyncReport(channels_examined=3, channels_missing=1, channels_synced=1)

    monkeypatch.setattr(migration_cli, "sync_missing_channel_data", fake_sync)

    result = CliRunner().invoke(
        migration_cli.migrate_cmd,
        [
            "sync-channels",
            "--source-profile",
            "SRC",
            "--destination-profile",
            "DST",
            "--source-dataset-rid",
            "src-rid",
            "--destination-dataset-rid",
            "dst-rid",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "1970-01-01T00:00:01Z",
            "--bucket-seconds",
            "0.5",
            "--tag",
            "site=daq",
            "--tag",
            "unit=2",
            "--max-retries",
            "0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["source"] == "src-rid"
    assert captured["destination"] == "dst-rid"
    assert captured["start"] == 0
    assert captured["end"] == SEC
    assert captured["bucket"] == SEC // 2
    assert captured["tags"] == {"site": "daq", "unit": "2"}
    assert captured["max_retries"] == 0
