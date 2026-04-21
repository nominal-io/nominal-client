from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core.client import NominalClient, WorkspaceSearchType


def _make_client() -> NominalClient:
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    return NominalClient(_clients=clients)


def _asset(name: str) -> MagicMock:
    asset = MagicMock()
    asset.name = name
    return asset


def test_returns_single_property_match_without_disambiguation() -> None:
    """When exactly one asset matches the properties, it is returned directly."""
    client = _make_client()
    properties = {"serial_num": "SN-001"}
    match = _asset("streaming_demo_asset")

    with patch.object(NominalClient, "search_assets", return_value=[match]) as search_assets:
        assert client.get_or_create_asset_by_properties(properties, name="streaming_demo_asset") is match

    search_assets.assert_called_once_with(properties=properties, workspace=WorkspaceSearchType.DEFAULT)


def test_creates_when_no_property_match() -> None:
    """When no asset matches the properties, a new one is created."""
    client = _make_client()
    properties = {"serial_num": "SN-001"}
    created = MagicMock()

    with (
        patch.object(NominalClient, "search_assets", return_value=[]),
        patch.object(NominalClient, "create_asset", return_value=created) as create_asset,
    ):
        assert (
            client.get_or_create_asset_by_properties(
                properties, name="streaming_demo_asset", description="desc", labels=["l"]
            )
            is created
        )

    create_asset.assert_called_once_with(
        name="streaming_demo_asset", description="desc", properties=properties, labels=["l"]
    )


def test_disambiguates_multiple_property_matches_by_exact_name() -> None:
    """When multiple assets share the properties, the one whose name matches exactly is returned."""
    client = _make_client()
    properties = {"team": "Michigan", "car_number": "8"}
    match = _asset("FSAE CT8 Vehicle")
    dupe = _asset("FSAE CT8 Vehicle2")

    # First call returns both property matches; second call (with exact_match) returns only `match`.
    with patch.object(NominalClient, "search_assets", side_effect=[[match, dupe], [match]]) as search_assets:
        assert client.get_or_create_asset_by_properties(properties, name="FSAE CT8 Vehicle") is match

    assert search_assets.call_count == 2
    search_assets.assert_any_call(properties=properties, workspace=WorkspaceSearchType.DEFAULT)
    search_assets.assert_any_call(
        properties=properties, exact_match="FSAE CT8 Vehicle", workspace=WorkspaceSearchType.DEFAULT
    )


def test_raises_when_disambiguation_still_ambiguous() -> None:
    """Multiple property matches AND multiple name matches is genuinely ambiguous."""
    client = _make_client()
    properties = {"team": "Michigan", "car_number": "8"}
    match_a = _asset("FSAE CT8 Vehicle")
    match_b = _asset("FSAE CT8 Vehicle")

    with patch.object(NominalClient, "search_assets", side_effect=[[match_a, match_b], [match_a, match_b]]):
        with pytest.raises(ValueError, match="cannot uniquely identify one asset"):
            client.get_or_create_asset_by_properties(properties, name="FSAE CT8 Vehicle")


def test_raises_when_multiple_property_matches_and_no_name_match() -> None:
    """Multiple property matches but none match the caller's name is still ambiguous."""
    client = _make_client()
    properties = {"team": "Michigan", "car_number": "8"}
    match_a = _asset("FSAE CT8 Vehicle2")
    match_b = _asset("FSAE CT8 Vehicle3")

    with patch.object(NominalClient, "search_assets", side_effect=[[match_a, match_b], []]):
        with pytest.raises(ValueError, match="cannot uniquely identify one asset"):
            client.get_or_create_asset_by_properties(properties, name="FSAE CT8 Vehicle")


def test_search_assets_exact_match_filters_client_side() -> None:
    """search_assets(exact_match=...) filters results to assets whose name matches exactly."""
    client = _make_client()
    match = _asset("FSAE CT8 Vehicle")
    other = _asset("FSAE CT8 Vehicle2")

    with patch.object(NominalClient, "_iter_search_assets", return_value=iter([match, other])):
        results = client.search_assets(exact_match="FSAE CT8 Vehicle")

    assert results == [match]
