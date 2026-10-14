from extractor_manifest import extract
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

# An extractor carries only identity. How it runs lives on its images.
extractor = client.create_containerized_extractor(
    name="bespoke-file-parser",
    description="Parses bespoke binary logs into telemetry, logs, and video",
)

# Uploads the `docker save` tarball and registers it as an image in one call.
# Build for linux/amd64: registration does not check the architecture, so an
# arm64 tarball registers and activates, then fails at ingest.
image = extractor.register_image(
    "bespoke-file-parser.tar",
    tag="v1",
    **extract.registration_kwargs(),
)

# Waits for the image to finish pushing (PENDING -> READY), then activates it.
# Activation is atomic, so this is also how you ship a new version.
extractor.set_active_image(image)

print(f"Extractor {extractor.rid} now runs image {image.rid}")
