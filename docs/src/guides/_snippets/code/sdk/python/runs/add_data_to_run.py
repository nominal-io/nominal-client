from huggingface_hub import hf_hub_download

csv_path = hf_hub_download(
    repo_id="nominal-io/frosty-flight",
    filename="frosty_flight_1k_rows.csv",
    repo_type="dataset",
)

csv_dataset = client.create_dataset("Frosty Flight")
dataset_file = csv_dataset.add_tabular_data(
    csv_path,
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)
uploaded_files = [dataset_file]

flight_simulator_run.add_dataset(
    dataset=csv_dataset, ref_name="high-precipitation-flight"
)
