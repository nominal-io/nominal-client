from __future__ import annotations

from unittest.mock import MagicMock

from nominal.core.workbook import WorkbookType
from nominal.core.workbook_template import WorkbookTemplate


def test_template_is_a_standard_workbook() -> None:
    template = WorkbookTemplate._from_template_summary(MagicMock(), MagicMock())

    assert template.workbook_type == WorkbookType.WORKBOOK
