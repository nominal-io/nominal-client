from nominal.core import NominalClient

client = NominalClient.from_profile("default")

extractor = client.get_containerized_extractor("<EXTRACTOR_RID>")
dataset = client.get_dataset("<DATASET_RID>")

job = dataset.add_containerized(
    extractor,
    sources={"INPUT_FILE": "path/to/data.bin"},
    arguments={"MODE": "fast"},
    tags={"vehicle_id": "sn-001"},
)

for dataset_file in job.as_files_ingested():
    print(f"ingested {dataset_file.name}")
