from nominal.experimental.pandas_data_handler.pandas_import_handler import PandasImportHandler
import nominal
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)

# import multiprocessing
# multiprocessing.log_to_stderr()
# multiprocessing.log_to_stderr(level=logging.getLogger().level)

if __name__ == "__main__":
    client = nominal.NominalClient.from_profile("staging")
    ds = client.get_dataset("ri.catalog.gov-staging.dataset.a34d6af2-6726-485b-8b6f-a2d77e25037d")
    rows = 10_000_000
    df = pd.DataFrame(
        {"timestamps": list(range(rows)), "apples": list(range(rows)), "bananas": [idx + 1234 for idx in range(rows)]}
    )

    import_handler = PandasImportHandler.from_datasource(
        ds,
        timestamp_column="timestamps",
        compression_level=3,
        num_encode_workers=8,
        num_upload_workers=64,
        upload_queue_size=20000,
    )
    import_handler.start()
    start = time.monotonic()
    for idx in range(16):
        import_handler.ingest(df)
    import_handler.stop()
    end = time.monotonic()
    diff = end - start
    print("Points encoded:", import_handler.points_encoded, import_handler.points_encoded / diff, "per second")
    print("Bytes uploaded:", import_handler.bytes_uploaded, import_handler.bytes_uploaded / diff, "per second")
