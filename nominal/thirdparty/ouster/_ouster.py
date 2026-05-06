from __future__ import annotations

import logging
from pathlib import Path
from typing import Mapping, Sequence

from nominal.core._types import PathLike
from nominal.core.client import NominalClient
from nominal.thirdparty.ouster._convert import convert_ouster_dataset

logger = logging.getLogger(__name__)


def upload_ouster_point_cloud(
    client: NominalClient,
    dataset_dir: PathLike,
    name: str | None = None,
    *,
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    apply_nav: bool = True,
    max_scans: int | None = None,
    sensor_model: str | None = "Ouster",
    coordinate_system: str | None = None,
    scan_pattern: str = "ROTATING",
) -> list[str]:
    """End-to-end: Ouster PCAP dataset -> preprocess -> upload -> spatial import.

    Converts each sensor's PCAP data into a CSV point cloud, uploads it, and
    triggers a Dagger spatial import via the Nominal spatial service.

    Args:
        client: Connected NominalClient instance.
        dataset_dir: Path to Ouster dataset directory containing data.yaml.
        name: Base name for spatial assets. Defaults to dataset directory name.
            Each sensor's spatial is named "{name} - {sensor_name}".
        description: Optional description for created spatial assets.
        labels: Labels to apply to all created spatial assets.
        properties: Key-value properties to apply.
        apply_nav: Whether to apply nav pose corrections during preprocessing.
        max_scans: Limit scans per sensor (useful for testing).
        sensor_model: Sensor model name for metadata, e.g. "Ouster OS1-128".
        coordinate_system: CRS identifier, e.g. "EPSG:4326".
        scan_pattern: Scan pattern: "ROTATING", "SOLID_STATE", "FLASH", or "MECHANICAL".

    Returns:
        List of spatial asset RIDs, one per sensor CSV.
    """
    dataset_dir = Path(dataset_dir)
    if name is None:
        name = dataset_dir.name

    logger.info("Converting Ouster dataset: %s", dataset_dir)
    csv_paths = convert_ouster_dataset(
        dataset_dir,
        apply_nav=apply_nav,
        max_scans=max_scans,
    )

    spatial_rids: list[str] = []
    for csv_path in csv_paths:
        sensor_name = csv_path.stem
        spatial_name = f"{name} - {sensor_name}"

        logger.info("Uploading and importing: %s", spatial_name)
        spatial_rid = client.upload_point_cloud(
            csv_path,
            name=spatial_name,
            description=description,
            labels=labels,
            properties=properties,
            sensor_model=sensor_model,
            coordinate_system=coordinate_system,
            scan_pattern=scan_pattern,
        )
        spatial_rids.append(spatial_rid)
        logger.info("Created spatial %s: %s", spatial_name, spatial_rid)

    return spatial_rids
