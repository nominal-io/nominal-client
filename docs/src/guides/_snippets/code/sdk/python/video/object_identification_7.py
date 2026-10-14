from nominal.core import NominalClient

client = NominalClient.from_profile("default")

dataset = client.create_dataset("Computer Vision Features: Drone Festival Flight")
feature_file = dataset.add_tabular_data(
    metadata_path,
    timestamp_column="timestamps",
    timestamp_type="iso_8601",
)
feature_file.poll_until_ingestion_completed()

print("Ingested dataset:", dataset.rid)
