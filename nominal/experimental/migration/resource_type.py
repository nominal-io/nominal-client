from __future__ import annotations

from enum import Enum


class ResourceType(Enum):
    ASSET = "ASSET"
    ATTACHMENT = "ATTACHMENT"
    DATASET = "DATASET"
    WORKBOOK_TEMPLATE = "WORKBOOK_TEMPLATE"
    RUN = "RUN"
    EVENT = "EVENT"
    VIDEO = "VIDEO"
    CHECKLIST = "CHECKLIST"
    DATA_REVIEW = "DATA_REVIEW"
    WORKBOOK = "WORKBOOK"
    DATASET_FILE = "DATASET_FILE"
    VIDEO_FILE = "VIDEO_FILE"
    ASSET_DATA_SCOPE = "ASSET_DATA_SCOPE"
    DATASET_CHANNEL = "DATASET_CHANNEL"
    DATASET_BOUNDS = "DATASET_BOUNDS"


def resource_label(resource_type: ResourceType) -> str:
    """Human-readable label for a resource type, as used in log messages (e.g. "workbook template")."""
    return resource_type.value.lower().replace("_", " ")
