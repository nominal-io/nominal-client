from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

INPUT_ENV = "INPUT_FILE"
OUTPUT_ENV = "OUTPUT_DIR"
TIMESTAMP_COLUMN = "timestamps-nanos"
OUTPUT_FILENAME = "simulated_telemetry.csv"


def main() -> None:
    """Unpickle simulated telemetry and emit CSV output."""
    input_file = os.environ.get(INPUT_ENV)
    if not input_file:
        raise ValueError(f"{INPUT_ENV} environment variable not set")

    output_dir = os.environ.get(OUTPUT_ENV)
    if not output_dir:
        raise ValueError(f"{OUTPUT_ENV} environment variable not set")

    dataframe = pd.read_pickle(input_file)
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Pickle input must contain a pandas DataFrame")
    if TIMESTAMP_COLUMN not in dataframe.columns:
        raise ValueError(f"Expected {TIMESTAMP_COLUMN!r} column in simulated telemetry DataFrame")

    output_path = Path(output_dir) / OUTPUT_FILENAME
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
