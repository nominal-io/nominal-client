import polars as pl

df_computer_vision = pl.read_csv(
    "hf://datasets/nominal-io/drone-flight-object-identification/object_detection_metadata.csv"
)
df_computer_vision.head().select(df_computer_vision.columns[:7])
