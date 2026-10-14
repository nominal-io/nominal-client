import polars as pl
from huggingface_hub import hf_hub_download

metadata_path = hf_hub_download(
    repo_id="nominal-io/drone-flight-object-identification",
    filename="object_detection_metadata.csv",
    repo_type="dataset",
)
df_computer_vision = pl.read_csv(metadata_path)
df_computer_vision.head().select(df_computer_vision.columns[:7])
