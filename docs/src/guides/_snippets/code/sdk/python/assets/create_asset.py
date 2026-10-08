from nominal.core import NominalClient

client = NominalClient.from_profile("default")
uav_drone_asset = client.create_asset(
    name="Quadcopter-456",
    description="Airborne observation platform",
    properties={"rotors": "4"},
    labels=("camera", "airborne"),
)

print("Created asset:", uav_drone_asset.rid)
