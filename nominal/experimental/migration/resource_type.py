from __future__ import annotations

from enum import Enum


class ResourceType(Enum):
    ASSET = "ASSET"
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
