from nominal.core import NominalClient

client = NominalClient.from_profile("default")  # replace with your profile name

found_assets = client.search_assets(search_text="000-123")

if len(found_assets) == 0:
    uav_drone_asset = client.create_asset(
        name="Quadcopter-000-123",
    )
