from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from nominal.core.asset import Asset
from nominal.core.workbook_template import WorkbookTemplate


@dataclass(frozen=True)
class AssetResources:
    asset: Asset
    source_workbook_templates: Sequence[WorkbookTemplate]


@dataclass(frozen=True)
class MigrationResources:
    source_assets: Sequence[AssetResources]
