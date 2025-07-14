import logging
import time
from math import sin
from random import random

import pandas as pd

import nominal
from nominal.experimental.pandas_data_handler.pandas_import_handler import PandasImportHandler

logging.basicConfig(level=logging.INFO)

# import multiprocessing
# multiprocessing.log_to_stderr()
# multiprocessing.log_to_stderr(level=logging.getLogger().level)

if __name__ == "__main__":
    client = nominal.NominalClient.from_profile("staging")
    ds = client.get_dataset("ri.catalog.gov-staging.dataset.a34d6af2-6726-485b-8b6f-a2d77e25037d")
    rows = 2_500_000
    df = pd.DataFrame(
        {
            "timestamps": list(range(rows * 15, rows * 16)),
            "apples": list(range(rows)),
            "bananas": [sin(idx) for idx in range(rows)],
        }
    )

    import_handler = PandasImportHandler.from_datasource(
        ds,
        timestamp_column="timestamps",
        compression_level=3,
        num_encode_workers=8,
        num_upload_workers=64,
        upload_queue_size=4096,
    )
    import_handler.start()
    start = time.monotonic()
    for idx in range(64):
        import_handler.ingest(df)
    import_handler.stop()
    end = time.monotonic()
    diff = end - start
    print("Total seconds:", diff)
    print("Points encoded:", import_handler.points_encoded, import_handler.points_encoded / diff, "per second")
    print("Bytes uploaded:", import_handler.bytes_uploaded, import_handler.bytes_uploaded / diff, "per second")
