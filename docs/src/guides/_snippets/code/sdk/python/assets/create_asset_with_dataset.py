import polars as pl
from huggingface_hub import hf_hub_download
from nominal.core import NominalClient

# Download the file using huggingface_hub, then read with Polars
csv_path = hf_hub_download(
    repo_id="nominal-io/frosty-flight",
    filename="frosty_flight_1k_rows.csv",
    repo_type="dataset",
)
df = pl.read_csv(csv_path)
df.write_csv("frosty_flight_1k_rows.csv")

client = NominalClient.from_profile("default")  # replace with your profile name

dataset = client.create_dataset(
    name="Frosty Flight measurements",
)
dataset.add_tabular_data(
    "frosty_flight_1k_rows.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)

uav_drone_asset = client.create_asset(name="Frosty Flight")
uav_drone_asset.add_dataset(dataset=dataset, data_scope_name="dataset")

print(uav_drone_asset.rid)
