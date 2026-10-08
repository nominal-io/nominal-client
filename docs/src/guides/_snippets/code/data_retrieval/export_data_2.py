from nominal.core import NominalClient
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

client = NominalClient.from_profile("default")

run = client.get_run("<run rid>")

tags = {"ingest_uuid": "your-ingest-uuid"}

channels = []
for _, dataset in run.list_datasets():
    channels.extend(list(dataset.search_channels(exact_match=["temperature"])))

exporter = PolarsExportHandler(client)

for df in exporter.export(
    channels,
    start=run.start,
    end=run.end,
    tags=tags,
):
    print(df.head())
