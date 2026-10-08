from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run(
    "ri.scout.cerulean-staging.run.22697726-5454-4fad-a3ea-fe45e9fa9f09"
)
run.update(labels=["X-PLANE"])
