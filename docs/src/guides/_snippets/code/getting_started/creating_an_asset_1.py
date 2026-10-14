from nominal.core import NominalClient

# Get a client to interact with Nominal.
# Assumes authentication has already been performed.
# Replace "default" with your profile name.
client = NominalClient.from_profile("default")

asset = client.get_or_create_asset_by_properties(
    # Human readable name used when creating the asset
    name="Shimmer SN-001",
    # Optional description, useful if you have notes about this glider.
    description="Our very first shimmer glider produced",
    # Properties used to find this glider or create it if none matches.
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
)
