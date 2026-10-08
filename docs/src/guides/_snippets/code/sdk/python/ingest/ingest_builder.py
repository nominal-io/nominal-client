from nominal.core import NominalClient
from nominal.experimental.ingest import IngestBuilder

client = NominalClient.from_profile("default")

dataset = client.get_dataset("<DATASET_RID>")

builder = (
    IngestBuilder(client, dataset)
    .add_tabular_data(
        "flight_data.csv",
        timestamp_column="source_time",
        timestamp_type="iso_8601",
        units={"altitude": "m", "airspeed": "m/s"},
        tag_columns={"vehicle_id": "veh_id"},
    )
    .add_mcap("recording.mcap", exclude_topics=["/diagnostics"])
    .add_journal_json("system.jsonl")
    .add_video("nose_camera.mp4", "nose_camera", start="2026-09-10T14:38:19Z")
    .add_tags({"test_event": "ft-114"})
)

# Uploads every file in parallel, then triggers one ingest job
job = builder.submit()

print(f"submitted {job.rid} ({job.ingest_type})")
