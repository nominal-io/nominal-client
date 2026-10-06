from pathlib import Path

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run("<run rid>")

output_dir = Path("downloaded_files")
output_dir.mkdir(exist_ok=True)

for _, dataset in run.list_datasets():
    for file in dataset.list_files(start=run.start, end=run.end):
        path = file.download(output_dir)
        print(f"Downloaded {file.name} to {path}")
