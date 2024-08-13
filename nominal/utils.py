"""
Internal utility functions for Nominal Python client
"""

import random
import string
from datetime import datetime


def default_filename(nominal_file_class):
    if nominal_file_class not in ["DATASET", "RUN"]:
        raise Exception("Unrecognized Nominal class", nominal_file_class)
    rand_str = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))
    ts = datetime.today().strftime("%Y-%m-%d")
    return "_".join([nominal_file_class, ts, rand_str])


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
    def run_upload(r, datasets_payload = {}) -> dict:
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
            "properties": {}
        }
