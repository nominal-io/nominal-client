from datetime import datetime, timedelta
from io import BytesIO
from typing import Callable

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.ts import EPOCH_SECONDS, ISO_8601, Custom, Relative


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


def _upload_and_assert(client: NominalClient, name: str, desc: str, csv_bytes: bytes, timestamp_type) -> Dataset:
    ds = client.create_dataset(name, description=desc)
    ds.add_from_io(BytesIO(csv_bytes), "timestamp", timestamp_type).poll_until_ingestion_completed(
        interval=timedelta(seconds=0.1)
    )
    assert ds.name == name
    assert ds.description == desc
    return ds


def test_iso_8601_str(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.isoformat()}Z")
    archive(_upload_and_assert(client, name, desc, csv_bytes, "iso_8601"))


def test_iso_8601_typed(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.isoformat()}Z")
    archive(_upload_and_assert(client, name, desc, csv_bytes, ISO_8601))


def test_epoch_seconds_str(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp()}")
    archive(_upload_and_assert(client, name, desc, csv_bytes, "epoch_seconds"))


def test_epoch_seconds_typed(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp()}")
    archive(_upload_and_assert(client, name, desc, csv_bytes, EPOCH_SECONDS))


def test_epoch_milliseconds_str(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    csv_bytes = _create_csv_data(temperature_data, lambda temp, ts: f"{temp},{ts.timestamp() * 1000}")
    archive(_upload_and_assert(client, name, desc, csv_bytes, "epoch_milliseconds"))


def test_relative_microseconds(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"
    start = temperature_data[0][1]

    def fmt(temp: int, ts: datetime) -> str:
        delta = ts - start
        micros = 1_000_000 + int(delta.total_seconds()) + delta.microseconds
        return f"{temp},{micros}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    archive(_upload_and_assert(client, name, desc, csv_bytes, Relative("microseconds", start)))


def test_custom_ctime(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        ctime = ts.ctime()
        ctime = ctime[:-5] + f".{ts.microsecond:06d}" + ctime[-5:]
        return f"{temp},{ctime}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    archive(_upload_and_assert(client, name, desc, csv_bytes, Custom(r"EEE MMM dd HH:mm:ss.SSSSSS yyyy")))


def test_custom_irig(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        return f"{temp},{ts.strftime(r'%j:%H:%M:%S.%f')}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    archive(_upload_and_assert(client, name, desc, csv_bytes, Custom(r"DDD:HH:mm:ss.SSSSSS", default_year=2024)))


def test_custom_day_of_year(request, client: NominalClient, temperature_data, archive: Callable):
    name = f"dataset-{request.node.name}"
    desc = f"timestamp test {request.node.name}"

    def fmt(temp: int, ts: datetime) -> str:
        return f"{temp},{ts.strftime(r'%H:%M:%S.%f')}"

    csv_bytes = _create_csv_data(temperature_data, fmt)
    archive(
        _upload_and_assert(
            client,
            name,
            desc,
            csv_bytes,
            Custom(r"HH:mm:ss.SSSSSS", default_year=2024, default_day_of_year=1),
        )
    )
