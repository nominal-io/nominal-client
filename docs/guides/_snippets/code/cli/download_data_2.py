from pathlib import Path

import pandas as pd

# Read all parquet files
data_files = Path("/Users/yourname/flight_data").glob("abc123-part_*.parquet")
dfs = [pd.read_parquet(f) for f in sorted(data_files)]

# Combine and sort by timestamp
nominal_data = pd.concat(dfs).sort_values("timestamp")
