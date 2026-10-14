from pathlib import Path
from time import time_ns

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run("<run rid>")
end = run.end if run.end is not None else time_ns()

output_dir = Path("downloaded_files")
output_dir.mkdir(exist_ok=True)

for _, dataset in run.list_datasets():
    for file in dataset.search_files(start=run.start, end=end):
        path = file.download(output_dir)
        print(f"Downloaded {file.name} to {path}")
