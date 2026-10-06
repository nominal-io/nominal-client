import polars as pl
from huggingface_hub import hf_hub_download

# Download the file using huggingface_hub, then read with Polars
csv_path = hf_hub_download(
    repo_id="nominal-io/frosty-flight",
    filename="frosty_flight_1k_rows.csv",
    repo_type="dataset",
)
df = pl.read_csv(csv_path)
df.write_csv("frosty_flight_1k_rows.csv")
print(df.head())
