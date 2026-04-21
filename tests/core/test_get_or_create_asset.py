from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core.client import NominalClient, WorkspaceSearchType, filter_assets_by_exact_name


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

    with patch.object(NominalClient, "search_assets", return_value=[match, dupe]) as search_assets:
        assert client.get_or_create_asset_by_properties(properties, name="FSAE CT8 Vehicle") is match

    # Disambiguation filters the already-fetched list locally; no extra server call.
    search_assets.assert_called_once_with(properties=properties, workspace=WorkspaceSearchType.DEFAULT)


def test_raises_when_disambiguation_still_ambiguous() -> None:
    """Multiple property matches AND multiple name matches is genuinely ambiguous."""
    client = _make_client()
    properties = {"team": "Michigan", "car_number": "8"}
    match_a = _asset("FSAE CT8 Vehicle")
    match_b = _asset("FSAE CT8 Vehicle")

    with patch.object(NominalClient, "search_assets", return_value=[match_a, match_b]):
        with pytest.raises(ValueError, match="cannot uniquely identify one asset"):
            client.get_or_create_asset_by_properties(properties, name="FSAE CT8 Vehicle")


def test_raises_when_multiple_property_matches_and_no_name_match() -> None:
    """Multiple property matches but none match the caller's name is still ambiguous."""
    client = _make_client()
    properties = {"team": "Michigan", "car_number": "8"}
    match_a = _asset("FSAE CT8 Vehicle2")
    match_b = _asset("FSAE CT8 Vehicle3")

    with patch.object(NominalClient, "search_assets", return_value=[match_a, match_b]):
        with pytest.raises(ValueError, match="cannot uniquely identify one asset"):
            client.get_or_create_asset_by_properties(properties, name="FSAE CT8 Vehicle")


def test_filter_assets_by_exact_name_returns_only_matches() -> None:
    """The filter keeps only assets whose name equals the requested string."""
    match = _asset("FSAE CT8 Vehicle")
    other = _asset("FSAE CT8 Vehicle2")

    assert filter_assets_by_exact_name([match, other], "FSAE CT8 Vehicle") == [match]


def test_filter_assets_by_exact_name_ignores_iteration_order() -> None:
    """Filtering must match on exact name, not position in the input.

    The non-target asset is alphanumerically less than the target, so a naive
    implementation that returned the first result (or the alphabetically first)
    would incorrectly pick the non-target.
    """
    non_target = _asset("a_non_target_asset")
    target = _asset("target_asset")

    assert filter_assets_by_exact_name([non_target, target], "target_asset") == [target]
