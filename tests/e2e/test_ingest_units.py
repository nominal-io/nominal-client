"""Verify ingestion-time unit metadata, CSV units rows, and unit updates."""

from pathlib import Path
from uuid import uuid4

from nominal.core import NominalClient
from nominal.core.dataset_file import IngestStatus
from nominal.experimental.ingest import IngestBuilder
from tests.e2e import POLL_INTERVAL


def test_csv_units_row_map_override_and_upsert(client: NominalClient, archive, tmp_path: Path) -> None:
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
    assert dataset.get_channel("pressure").unit == "kPa"
    assert dataset.get_channel("speed").unit == "m/s"

    # Ingesting a new unit updates an existing channel; omitted channels keep their units.
    path.write_text("time,pressure,speed\n1700000001,102,5\n")
    job = (
        IngestBuilder(client, dataset)
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
    assert dataset.get_channel("pressure").unit == "bar"
    assert dataset.get_channel("speed").unit == "m/s"
