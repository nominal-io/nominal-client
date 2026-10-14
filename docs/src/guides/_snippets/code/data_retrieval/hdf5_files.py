import io

import h5py
import pandas as pd
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

num_rows = 0
with h5py.File(local_path, "r") as file:
    data = file["data"]["1"]["meshes"]["B"]

    batch_size = 5
    total_size = data["x"].shape[0]  # Assuming the same shape for 'x', 'y', and 'z'
    if total_size == 0:
        raise ValueError("No rows to upload")

    dataset = client.create_dataset("h5_flattened")

    for start_idx in range(0, total_size, batch_size):
        end_idx = min(start_idx + batch_size, total_size)

        batch = {}
        for col in ["x", "y", "z"]:
            for col_idx in [0, 1, 2]:
                # Fetch the batch using slicing (start_idx:end_idx)
                batch[f"{col}{col_idx + 1}"] = data[col][
                    start_idx:end_idx, :, col_idx
                ].ravel()

        df = pd.DataFrame(batch)
        df["ts"] = list(range(num_rows, num_rows + len(df)))
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        dataset.add_from_io(
            dataset=buffer, timestamp_column="ts", timestamp_type="epoch_seconds"
        ).poll_until_ingestion_completed()
        num_rows += len(df)
