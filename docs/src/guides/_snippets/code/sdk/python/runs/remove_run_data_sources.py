from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run(
    "ri.scout.cerulean-staging.run.6c41d7fd-a48f-4d60-baa7-f34492264156"
)

run.remove_data_sources(
    data_sources=[
        "ri.catalog.cerulean-staging.dataset.d4f413c4-4787-4259-a142-1286587b50af"
    ]
)
