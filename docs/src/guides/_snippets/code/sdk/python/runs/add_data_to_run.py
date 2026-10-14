import polars as pl

df = pl.read_csv("hf://datasets/nominal-io/frosty-flight/frosty_flight_1k_rows.csv")
df.write_csv("frosty_flight_1k_rows.csv")

csv_dataset = client.create_dataset("Frosty Flight")
csv_dataset.add_tabular_data(
    "frosty_flight_1k_rows.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)

flight_simulator_run.add_dataset(
    dataset=csv_dataset, ref_name="high-precipitation-flight"
)
