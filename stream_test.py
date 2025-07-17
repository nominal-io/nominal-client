from asyncio import as_completed
import logging
import multiprocessing

import pandas as pd

import nominal
from nominal.experimental.pandas_data_handler.pandas_import_handler import PandasImportHandler
import concurrent.futures

logging.basicConfig(level=logging.DEBUG)


def add_to_queue(q):
    q.put(5)


if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as pool, multiprocessing.Manager() as manager:
        q = manager.Queue(maxsize=1)
        manager.Condition(manager.Lock())
        futures = [pool.submit(add_to_queue, q) for idx in range(4)]
        for future in concurrent.futures.as_completed(futures):
            print(future)

    # # multiprocessing.log_to_stderr(level=logging.getLogger().level)

    # client = nominal.NominalClient.from_profile("staging")
    # ds = client.get_dataset("ri.catalog.gov-staging.dataset.a34d6af2-6726-485b-8b6f-a2d77e25037d")
    # import_handler = PandasImportHandler.from_datasource(ds, timestamp_column="timestamps")
    # import_handler.start()

    # for idx in range(100):
    #     rows = 100_000
    #     import_handler.ingest(
    #         pd.DataFrame(
    #             {"timestamps": list(range(rows)), "apples": list(range(rows)), "bananas": [str(f) for f in range(rows)]}
    #         )
    #     )

    # import_handler.teardown()

    # print("Tore down handler")
    # print(import_handler.points_encoded, import_handler.bytes_uploaded)
