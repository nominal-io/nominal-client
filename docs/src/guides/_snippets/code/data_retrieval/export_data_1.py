import re
from datetime import timedelta

import polars as pl
from nominal.core import NominalClient
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

client = NominalClient.from_profile("default")

runs = client.search_runs(labels=["Failure"])

for run in runs:
    channels = []
    for _, dataset in run.list_datasets():
        channels.extend(list(dataset.search_channels(exact_match=["temperature"])))
        channels.extend(list(dataset.search_channels(exact_match=["speed"])))

    exporter = PolarsExportHandler(client)

    for idx, df in enumerate(
        exporter.export(
            channels,
            start=run.start,
            end=run.end,
            batch_duration=timedelta(seconds=600),
        )
    ):
        if isinstance(df, pl.Series):
            df = df.to_frame()

        safe_name = re.sub(r"[^\w\-_.]", "_", run.name)
        df.write_csv(f"{safe_name}_{run.run_number}_{idx}.csv")
