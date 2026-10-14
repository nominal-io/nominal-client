import polars as pl

# Read the downloaded parts and sort by timestamp.
nominal_data = pl.read_parquet(
    "/Users/yourname/flight_data/abc123-part_*.parquet"
).sort("timestamp")
