from nominal.core import NominalClient
from nominal.thirdparty.pandas import upload_dataframe

client = NominalClient.from_profile("default")

dataset = upload_dataframe(
    client, df_gen, dataset_name, "Time", timestamp_type="iso_8601"
)
dataset
