from datetime import datetime, timedelta
from typing import Callable
from unittest import mock
from uuid import uuid4

import pytest

import nominal as nm


@pytest.fixture(scope="module")
def temperature_data() -> list[tuple[int, datetime]]:
    return [
        (20, datetime.fromisoformat("2024-09-30T16:37:36.891349")),
        (21, datetime.fromisoformat("2024-09-30T16:37:36.990262")),
        (22, datetime.fromisoformat("2024-09-30T16:37:37.089310")),
        (19, datetime.fromisoformat("2024-09-30T16:37:37.190015")),
        (23, datetime.fromisoformat("2024-09-30T16:37:37.289585")),
        (22, datetime.fromisoformat("2024-09-30T16:37:37.388941")),
        (28, datetime.fromisoformat("2024-09-30T16:37:37.491115")),
        (24, datetime.fromisoformat("2024-09-30T16:37:37.590826")),
    ]


def _create_csv_data(data: list[tuple[int, datetime]], formatter: Callable[[int, datetime], str]) -> bytes:
    return ("temperature,timestamp\n" + "\n".join(formatter(temp, ts) for temp, ts in data)).encode()


# def test_iso_8601_str(request, temperature_data):
#     nm.upload_csv("temperature.csv", "Exterior Temps", timestamp_column="timestamp", timestamp_type="iso_8601")
#     nm.upload_csv("temperature.csv", "Exterior Temps", timestamp_column="timestamp", timestamp_type=nm.ts.Iso8601())


def test_iso_8601_str(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    csv_data = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.isoformat()}Z")

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", "iso_8601", desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_iso_8601_typed(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    csv_data = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.isoformat()}Z")
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", nm.ts.ISO_8601, desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_epoch_seconds_str(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    csv_data = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp()}")
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", "epoch_seconds", desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_epoch_seconds_typed(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    csv_data = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp()}")
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", nm.ts.EPOCH_SECONDS, desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_epoch_milliseconds_str(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    csv_data = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp() * 1000}")
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", "epoch_milliseconds", desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_relative_microseconds(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"
    start = temperature_data[0][1]

    def fmt(temp: int, ts: datetime) -> str:
        delta = ts - start
        micros = 1_000_000 + int(delta.total_seconds()) + delta.microseconds
        return f"{temp},{micros}"

    csv_data = _create_csv_data(temperature_data, fmt)
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", nm.ts.Relative("microseconds", start), desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_custom_ctime(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    def fmt(temp: int, ts: datetime) -> str:
        ctime = ts.ctime()
        ctime = ctime[:-5] + f".{ts.microsecond:06d}" + ctime[-5:]
        return f"{temp},{ctime}"

    csv_data = _create_csv_data(temperature_data, fmt)
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", nm.ts.Custom(r"EEE MMM dd HH:mm:ss.SSSSSS yyyy"), desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_custom_irig(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    def fmt(temp: int, ts: datetime) -> str:
        return f"{temp},{ts.strftime(r'%j:%H:%M:%S.%f')}"

    csv_data = _create_csv_data(temperature_data, fmt)
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv(
            "fake_path.csv", name, "timestamp", nm.ts.Custom(r"DDD:HH:mm:ss.SSSSSS", default_year=2024), desc
        )
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc


def test_custom_day_of_year(request, temperature_data):
    name = f"dataset-{uuid4()}"
    desc = f"timestamp test {request.node.name} {uuid4()}"

    def fmt(temp: int, ts: datetime) -> str:
        return f"{temp},{ts.strftime(r'%H:%M:%S.%f')}"

    csv_data = _create_csv_data(temperature_data, fmt)
    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv(
            "fake_path.csv",
            name,
            "timestamp",
            nm.ts.Custom(r"HH:mm:ss.SSSSSS", default_year=2024, default_day_of_year=1),
            desc,
        )
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.name == name
    assert ds.description == desc
