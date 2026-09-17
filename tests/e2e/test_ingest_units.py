"""Verify ingestion-time unit metadata, CSV units rows, and unit updates."""

from pathlib import Path
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import IngestStatus
from nominal.experimental.ingest import IngestBuilder
from tests.e2e import POLL_INTERVAL


@pytest.fixture
def dataset_with_units(client: NominalClient, archive, tmp_path: Path) -> Dataset:
    """Ingest a CSV units row with an explicit pressure-unit override into a fresh dataset."""
    dataset = client.create_dataset(f"units-e2e-{uuid4().hex[:8]}")
    archive(dataset)
    path = tmp_path / "readings.csv"
    path.write_text("time,pressure,speed\n,Pa,m/s\n1700000000,101325,4\n")
    job = (
        IngestBuilder(client, dataset)
        .add_csv(
            path,
            "time",
            "epoch_seconds",
            units_row=2,
            data_row=3,
            units={"pressure": "kPa"},
        )
        .submit()
    )
    [result] = list(job.as_files_ingested(poll_interval=POLL_INTERVAL))
    assert result.ingest_status == IngestStatus.SUCCESS
    return dataset


def test_csv_unit_map_overrides_units_row(dataset_with_units: Dataset) -> None:
    """Explicit units override matching CSV row units while other columns retain their row units."""
    assert dataset_with_units.get_channel("pressure").unit == "kPa"
    assert dataset_with_units.get_channel("speed").unit == "m/s"


def test_ingestion_updates_only_supplied_channel_units(
    client: NominalClient, dataset_with_units: Dataset, tmp_path: Path
) -> None:
    """Later ingestion updates supplied channel units while preserving units for omitted channels."""
    path = tmp_path / "updated.csv"
    path.write_text("time,pressure,speed\n1700000001,102,5\n")
    job = (
        IngestBuilder(client, dataset_with_units)
        .add_csv(
            path,
            "time",
            "epoch_seconds",
            units={"pressure": "bar"},
        )
        .submit()
    )
    [result] = list(job.as_files_ingested(poll_interval=POLL_INTERVAL))
    assert result.ingest_status == IngestStatus.SUCCESS
    assert dataset_with_units.get_channel("pressure").unit == "bar"
    assert dataset_with_units.get_channel("speed").unit == "m/s"
