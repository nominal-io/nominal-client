from nominal.core import NominalClient

# Get a client to interact with Nominal.
# Assumes authentication has already been performed.
# Replace "default" with your profile name.
client = NominalClient.from_profile("default")

asset = client.create_asset(
    # Human readable name for the asset
    name="Shimmer SN-001",
    # Optional description, useful if you have notes about this glider.
    description="Our very first shimmer glider produced",
    # Properties which can help us find this asset later.
    # Nominal supports unique identification of any physical asset
    # using some combination of these properties.
    properties={
        "platform": "shimmer",
        "serial_num": "SN-001",
    },
)
