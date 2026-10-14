from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

asset = client.get_asset(
    "ri.scout.cerulean-staging.asset.6c41d7fd-a48f-4d60-baa7-f34492264156"
)

asset.remove_data_scopes(
    scopes=["ri.catalog.cerulean-staging.dataset.d4f413c4-4787-4259-a142-1286587b50af"]
)
