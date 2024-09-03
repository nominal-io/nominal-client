from __future__ import annotations

"""
Internal utility functions for Nominal Python client
"""

import random
import string
from datetime import datetime
from typing import Type, TypeVar

import requests
from conjure_python_client import ServiceConfiguration
from requests.utils import CaseInsensitiveDict


def default_filename(nominal_file_class):
    if nominal_file_class not in ["DATASET", "RUN"]:
        raise Exception("Unrecognized Nominal class", nominal_file_class)
    rand_str = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))
    ts = datetime.today().strftime("%Y-%m-%d")
    filename = "_".join([nominal_file_class, ts, rand_str])
    if nominal_file_class == "DATASET":
        filename = filename.rstrip("_") + ".csv"
    return filename


T = TypeVar("T")


def create_service(service_class: Type[T], uri: str) -> T:
    config = ServiceConfiguration()

    session = requests.Session()
    session.headers = CaseInsensitiveDict({"User-Agent": "nominal-python"})

    return service_class(session, [uri], config.connect_timeout, config.read_timeout, None, False)


class PayloadFactory:
    """
    Given a Nominal Python object, generate JSON payload
    for REST API to instantiate on Nominal platform.
    """

    @staticmethod
    def dataset_trigger_ingest(ds) -> dict:
        return {
            "source": {
                "type": "s3",
                "s3": {
                    "path": ds.s3_path,
                },
            },
            "properties": ds.properties,
            "datasetName": ds.filename,
            "datasetDescription": ds.description,
            "timestampMetadata": {
                # "seriesName": "_iso_8601",
                "seriesName": "_python_datetime",
                "timestampType": {
                    "type": "absolute",
                    "absolute": {
                        # "type": "iso8601",
                        "type": "customFormat",
                        # "iso8601": {}
                        "customFormat": {"format": "yyyy-MM-dd['T']HH:mm:ss.SSSSSS", "defaultYear": 0},
                    },
                },
            },
        }

    @staticmethod
    def create_unix_datasource(ds) -> dict:
        return {
            "dataSource": {  # IngestRunDataSource
                "type": "newDataSourceV2",
                "newDataSourceV2": {
                    "source": {"type": "s3", "s3": {"path": ds.s3_path}},
                    "name": ds.filename,
                    "properties": {},
                    "timeColumnSpec": {
                        "seriesName": "_unix",
                        "timestampType": {
                            "type": "absolute",
                            "absolute": {
                                "type": "epochOfTimeUnit",
                                "epochOfTimeUnit": {"timeUnit": "SECONDS"},
                            },
                        },
                    },
                },
            },
            "timeOffsetSpec": {"type": "nanos", "nanos": {"seconds": 0, "nanos": 0}},
        }

    @staticmethod
    def run_upload(r, datasets_payload={}) -> dict:
        return {
            "title": r.title,
            "description": r.description,
            "startTime": {
                "secondsSinceEpoch": r._domain["START"]["SECONDS"],
                "offsetNanoseconds": r._domain["START"]["NANOS"],
            },
            "endTime": {
                "secondsSinceEpoch": r._domain["END"]["SECONDS"],
                "offsetNanoseconds": r._domain["END"]["NANOS"],
            },
            "dataSources": datasets_payload,
            "properties": r.properties,
        }


import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Union
from ._api.combined import scout_run_api
from ._api.ingest import ingest_api

if sys.version_info >= (3, 11):
    from typing import Self as Self
else:
    from typing_extensions import Self as Self


if sys.version_info >= (3, 10):
    from typing import TypeAlias as TypeAlias
else:
    from typing_extensions import TypeAlias as TypeAlias

logger = logging.getLogger(__name__)

IntegralNanosecondsUTC = int


@dataclass
class CustomTimestampFormat:
    format: str
    default_year: int = 0


# Using Union rather than the "|" operator due to https://github.com/python/mypy/issues/11665.
TimestampColumnType: TypeAlias = Union[
    Literal[
        "iso_8601",
        "epoch_days",
        "epoch_hours",
        "epoch_minutes",
        "epoch_seconds",
        "epoch_milliseconds",
        "epoch_microseconds",
        "epoch_nanoseconds",
        "relative_days",
        "relative_hours",
        "relative_minutes",
        "relative_seconds",
        "relative_milliseconds",
        "relative_microseconds",
        "relative_nanoseconds",
    ],
    CustomTimestampFormat,
]


def _timestamp_type_to_conjure_ingest_api(
    ts_type: TimestampColumnType,
) -> ingest_api.TimestampType:
    if isinstance(ts_type, CustomTimestampFormat):
        return ingest_api.TimestampType(
            absolute=ingest_api.AbsoluteTimestamp(
                custom_format=ingest_api.CustomTimestamp(format=ts_type.format, default_year=ts_type.default_year)
            )
        )
    elif ts_type == "iso_8601":
        return ingest_api.TimestampType(absolute=ingest_api.AbsoluteTimestamp(iso8601=ingest_api.Iso8601Timestamp()))
    relation, unit = ts_type.split("_", 1)
    time_unit = ingest_api.TimeUnit[unit.upper()]
    if relation == "epoch":
        return ingest_api.TimestampType(
            absolute=ingest_api.AbsoluteTimestamp(epoch_of_time_unit=ingest_api.EpochTimestamp(time_unit=time_unit))
        )
    elif relation == "relative":
        return ingest_api.TimestampType(relative=ingest_api.RelativeTimestamp(time_unit=time_unit))
    raise ValueError(f"invalid timestamp type: {ts_type}")


def _flexible_time_to_conjure_scout_run_api(
    timestamp: datetime | IntegralNanosecondsUTC,
) -> scout_run_api.UtcTimestamp:
    if isinstance(timestamp, datetime):
        seconds, nanos = _datetime_to_seconds_nanos(timestamp)
        return scout_run_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)
    elif isinstance(timestamp, IntegralNanosecondsUTC):
        seconds, nanos = divmod(timestamp, 1_000_000_000)
        return scout_run_api.UtcTimestamp(seconds_since_epoch=seconds, offset_nanoseconds=nanos)
    raise TypeError(f"expected {datetime} or {IntegralNanosecondsUTC}, got {type(timestamp)}")


def _conjure_time_to_integral_nanoseconds(
    ts: scout_run_api.UtcTimestamp,
) -> IntegralNanosecondsUTC:
    return ts.seconds_since_epoch * 1_000_000_000 + (ts.offset_nanoseconds or 0)


def _datetime_to_seconds_nanos(dt: datetime) -> tuple[int, int]:
    dt = dt.astimezone(timezone.utc)
    seconds = int(dt.timestamp())
    nanos = dt.microsecond * 1000
    return seconds, nanos


def construct_user_agent_string() -> str:
    """Constructs a user-agent string with system & Python metadata.
    E.g.: nominal-python/1.0.0b0 (macOS-14.4-arm64-arm-64bit) cpython/3.12.4
    """
    import importlib.metadata
    import platform
    import sys

    try:
        v = importlib.metadata.version("nominal")
        p = platform.platform()
        impl = sys.implementation
        py = platform.python_version()
        return f"nominal-python/{v} ({p}) {impl.name}/{py}"
    except Exception as e:
        # I believe all of the above are cross-platform, but just in-case...
        logger.error("failed to construct user-agent string", exc_info=e)
        return "nominal-python/unknown"
