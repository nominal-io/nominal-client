from nominal.core import NominalClient
from nominal.core.container_image import FileExtractionInput, FileExtractionParameter

client = NominalClient.from_profile("default")

# An extractor carries only identity. How it runs lives on its images.
extractor = client.create_containerized_extractor(
    name="bespoke-file-parser",
    description="Parses bespoke binary logs into parquet",
)

# Uploads the `docker save` tarball and registers it as an image in one call.
# Build for linux/amd64: registration does not check the architecture, so an
# arm64 tarball registers and activates, then fails at ingest.
image = extractor.register_image(
    "bespoke-file-parser.tar",
    tag="v1",
    inputs=[
        FileExtractionInput(
            name="Input file",
            # Your container reads the file path from this environment variable
            environment_variable="INPUT_FILE",
            file_suffixes=[".bin"],
            required=True,
        ),
    ],
    parameters=[
        FileExtractionParameter(
            name="Parsing mode",
            environment_variable="MODE",
            required=False,
        ),
    ],
    # How rows in the output are indexed in time. Ingests can override this.
    default_timestamp_column="timestamp",
    default_timestamp_type="iso_8601",
)

# Waits for the image to finish pushing (PENDING -> READY), then activates it.
# Activation is atomic, so this is also how you ship a new version.
extractor.set_active_image(image)

print(f"Extractor {extractor.rid} now runs image {image.rid}")
