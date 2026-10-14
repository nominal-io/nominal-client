import nominal.thirdparty.pandas as npm
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

dataset = npm.upload_dataframe(
    client,
    df_computer_vision.to_pandas(),
    name="Computer Vision Features: Drone Festival Flight",
    timestamp_column="timestamps",
    timestamp_type="iso_8601",
)

print("Uploaded dataset:", dataset.rid)
