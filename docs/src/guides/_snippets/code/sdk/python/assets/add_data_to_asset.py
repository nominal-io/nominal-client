import polars as pl
from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

df = pl.read_csv("hf://datasets/nominal-io/frosty-flight/frosty_flight_1k_rows.csv")
df.write_csv("frosty_flight_1k_rows.csv")

csv_dataset = client.create_dataset("Frosty Flight")

csv_dataset.add_tabular_data(
    "frosty_flight_1k_rows.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)

uav_drone_asset.add_dataset(
    dataset=csv_dataset, data_scope_name="high-precipitation-flight"
)
