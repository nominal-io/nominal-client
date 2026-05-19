"""End-to-end test for self-hosted containerized extractors.

Run against staging with a profile that includes a workspace RID:

    uv run pytest tests/e2e/test_containerized_extractor_registry.py --profile staging --no-cov -v
"""

from __future__ import annotations

import shutil
import subprocess
import time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from nominal.core import NominalClient
from nominal.core.container_image import ContainerImage, ContainerImageStatus
from nominal.core.containerized_extractors import ContainerizedExtractor, FileExtractionInput, FileOutputFormat
from nominal.thirdparty.pandas import datasource_to_dataframe
from tests.e2e import POLL_INTERVAL

BUILD_CONTEXT = Path(__file__).parent / "data" / "containerized_csv_extractor"
SIMULATED_TELEMETRY_CSV = BUILD_CONTEXT / "simulated_telemetry.csv"
PICKLE_INPUT_NAME = "simulated_telemetry.pkl"
INPUT_ENV_VAR = "INPUT_FILE"
EXTRACTOR_NAME = "test-unpickle-extractor"
TIMESTAMP_COLUMN = "timestamps-nanos"
TIMESTAMP_TYPE = "epoch_nanoseconds"
DATA_COLUMNS = (
    "speed_mps",
    "acceleration_mps2",
    "battery_voltage_v",
    "motor_current_a",
    "steering_angle_deg",
    "brake_pressure_kpa",
    "tire_temp_fl_c",
    "tire_temp_fr_c",
    "state_of_charge_pct",
)
IMAGE_READY_TIMEOUT = timedelta(minutes=10)


def _require_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker executable not found")
    result = subprocess.run(
        ["docker", "info"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"docker daemon unavailable: {result.stderr.strip() or result.stdout.strip()}")


def _run_docker_command(args: list[str]) -> None:
    subprocess.run(args, check=True, cwd=BUILD_CONTEXT)


def _build_and_save_image(tmp_path: Path, image_tag: str) -> Path:
    _run_docker_command(
        [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "--tag",
            image_tag,
            ".",
        ]
    )
    tar_path = tmp_path / "containerized-csv-extractor.tar"
    _run_docker_command(["docker", "save", "--output", str(tar_path), image_tag])
    return tar_path


def _wait_for_image_ready(client: NominalClient, image: ContainerImage, workspace_rid: str) -> ContainerImage:
    deadline = time.monotonic() + IMAGE_READY_TIMEOUT.total_seconds()
    while time.monotonic() < deadline:
        refreshed = client.get_container_image(image.rid, workspace_rid=workspace_rid)
        if refreshed.status is ContainerImageStatus.READY:
            return refreshed
        if refreshed.status is ContainerImageStatus.FAILED:
            raise AssertionError(f"Container image {image.rid} failed to import")
        time.sleep(POLL_INTERVAL.total_seconds())
    raise TimeoutError(f"Timed out waiting for container image {image.rid} to become READY")


def _register_simulated_telemetry_extractor(
    client: NominalClient,
    *,
    image: ContainerImage,
    session_id: str,
) -> ContainerizedExtractor:
    return client.create_containerized_extractor(
        name=EXTRACTOR_NAME,
        description="E2E extractor that reads INPUT_FILE as a pickle and emits CSV data.",
        container_image_rid=image.rid,
        inputs=[
            FileExtractionInput(
                name=PICKLE_INPUT_NAME,
                description="Pickled pandas DataFrame with epoch nanosecond timestamps.",
                environment_variable=INPUT_ENV_VAR,
                file_suffixes=[".pkl"],
                required=True,
            )
        ],
        properties={"e2e": "containerized-extractor-registry", "session_id": session_id},
        labels=["e2e", "containerized-extractor"],
        timestamp_column=TIMESTAMP_COLUMN,
        timestamp_type=TIMESTAMP_TYPE,
        file_output_format=FileOutputFormat.CSV,
    )


def _read_simulated_telemetry_csv() -> pd.DataFrame:
    df = pd.read_csv(
        SIMULATED_TELEMETRY_CSV,
        dtype={TIMESTAMP_COLUMN: "int64"},
    )

    assert tuple(df.columns) == (TIMESTAMP_COLUMN, *DATA_COLUMNS)
    assert (df[TIMESTAMP_COLUMN] % 1_000 != 0).all()
    assert df[TIMESTAMP_COLUMN].is_monotonic_increasing
    return df


def _write_pickle_input(tmp_path: Path) -> tuple[Path, pd.DataFrame]:
    source = _read_simulated_telemetry_csv()
    pickle_path = tmp_path / PICKLE_INPUT_NAME
    source.to_pickle(pickle_path)
    pd.testing.assert_frame_equal(
        pd.read_pickle(pickle_path),
        source,
        check_exact=True,
    )
    return pickle_path, source


def _expected_dataframe(source: pd.DataFrame) -> pd.DataFrame:
    expected = source.loc[:, DATA_COLUMNS].astype("float64")
    expected.index = pd.to_datetime(source[TIMESTAMP_COLUMN], unit="ns", utc=True)
    expected.index.name = "timestamp"
    return expected


def test_self_hosted_containerized_extractor_round_trip(client: NominalClient, tmp_path: Path, request) -> None:
    """Build, upload, register, run, verify, archive, and delete a self-hosted extractor."""
    _require_docker()

    session_id = uuid4().hex[:8]
    workspace_rid = client._clients.resolve_default_workspace_rid()
    image_tag = f"{EXTRACTOR_NAME}:{session_id}"

    def remove_local_image() -> None:
        subprocess.run(["docker", "image", "rm", "--force", image_tag], check=False, capture_output=True)

    request.addfinalizer(remove_local_image)

    tar_path = _build_and_save_image(tmp_path, image_tag)
    pickle_input_path, source = _write_pickle_input(tmp_path)
    with tar_path.open("rb") as tarball:
        image = client.upload_container_image_from_io(tarball, EXTRACTOR_NAME, session_id)
    request.addfinalizer(lambda: client.delete_container_image(image.rid, workspace_rid=workspace_rid))

    image = _wait_for_image_ready(client, image, workspace_rid)
    extractor = _register_simulated_telemetry_extractor(
        client,
        image=image,
        session_id=session_id,
    )
    request.addfinalizer(lambda: client.get_containerized_extractor(extractor.rid).archive())

    dataset = client.create_dataset(
        f"{EXTRACTOR_NAME}-{session_id}",
        description="E2E output dataset for self-hosted containerized extractor registry flow.",
        properties={"e2e": "containerized-extractor-registry", "session_id": session_id},
        labels=["e2e", "containerized-extractor"],
    )
    request.addfinalizer(dataset.archive)

    dataset_file = dataset.add_containerized(
        extractor.rid,
        {INPUT_ENV_VAR: pickle_input_path},
        tags={"session_id": session_id},
        timestamp_column=TIMESTAMP_COLUMN,
        timestamp_type=TIMESTAMP_TYPE,
    )
    dataset_file.poll_until_ingestion_completed(interval=POLL_INTERVAL)

    expected = _expected_dataframe(source)
    actual = datasource_to_dataframe(dataset)
    missing_columns = sorted(set(expected.columns) - set(actual.columns))
    unexpected_columns = sorted(set(actual.columns) - set(expected.columns))
    assert not missing_columns
    assert not unexpected_columns

    actual = actual.reindex(expected.columns, axis=1)
    pd.testing.assert_frame_equal(
        actual,
        expected,
        check_exact=True,
    )
