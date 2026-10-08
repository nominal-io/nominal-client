from nominal.core import NominalClient

client = NominalClient.from_profile("default")

x_plane_runs = client.search_runs(labels=["X-PLANE"])
