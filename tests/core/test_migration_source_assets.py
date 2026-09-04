from unittest.mock import MagicMock

import pytest
from click import UsageError

from nominal.experimental.migration.migration_cli import _load_asset_resources


def test_load_asset_resources_none_returns_empty() -> None:
    """An absent source_asset_rids should yield no assets rather than erroring."""
    assert _load_asset_resources(MagicMock(), None, None) == {}


def test_load_asset_resources_empty_list_returns_empty() -> None:
    """An explicitly empty source_asset_rids should yield no assets rather than erroring."""
    assert _load_asset_resources(MagicMock(), [], None) == {}


def test_load_asset_resources_non_list_rejected() -> None:
    """A non-list, non-empty source_asset_rids is still a config error."""
    with pytest.raises(UsageError, match="must be a list"):
        _load_asset_resources(MagicMock(), "ri.asset", None)


def test_load_asset_resources_rejects_both_provided() -> None:
    """Providing both source_asset_rids and source_assets is ambiguous."""
    with pytest.raises(UsageError, match="only one of"):
        _load_asset_resources(MagicMock(), ["ri.asset"], {"ri.asset": {}})
