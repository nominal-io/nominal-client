from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.core.dataset import Dataset
from nominal.experimental.migration.migration_cli import (
    ImpersonatingDestinationClientResolver,
    ImpersonationConfig,
    get_source_user_rid,
)


def _dataset() -> Dataset:
    return Dataset(
        rid="ri.catalog.main.dataset.00000000-0000-0000-0000-000000000001",
        name="Source dataset",
        description=None,
        properties={},
        labels=[],
        bounds=None,
        is_archived=False,
        _clients=MagicMock(),
    )


def test_get_source_user_rid_reads_dataset_owner_via_role_service() -> None:
    dataset = _dataset()
    dataset._clients.catalog.get_enriched_datasets.side_effect = RuntimeError("catalog unavailable")  # type: ignore[attr-defined]

    with patch(
        "nominal.experimental.migration.migration_cli.get_dataset_owner_rid",
        return_value="ri.authn.source.user.owner",
    ) as get_owner:
        assert get_source_user_rid(dataset) == "ri.authn.source.user.owner"

    get_owner.assert_called_once_with(dataset)


def test_get_source_user_rid_falls_back_when_dataset_owner_lookup_fails() -> None:
    dataset = _dataset()

    with patch(
        "nominal.experimental.migration.migration_cli.get_dataset_owner_rid",
        side_effect=ValueError("owner not found"),
    ):
        assert get_source_user_rid(dataset) is None


def test_dataset_owner_selects_impersonated_destination_client() -> None:
    dataset = _dataset()
    destination_client = MagicMock(name="destination_client")
    impersonated_client = MagicMock(name="impersonated_client")
    resolver = ImpersonatingDestinationClientResolver(
        destination_client,
        ImpersonationConfig(
            enabled=True,
            source_to_destination_user_rids={
                "ri.authn.source.user.owner": "ri.authn.destination.user.owner",
            },
        ),
    )

    with (
        patch(
            "nominal.experimental.migration.migration_cli.get_dataset_owner_rid",
            return_value="ri.authn.source.user.owner",
        ),
        patch(
            "nominal.experimental.migration.migration_cli.as_user",
            return_value=impersonated_client,
        ) as as_user,
    ):
        assert resolver(dataset) is impersonated_client

    as_user.assert_called_once_with(destination_client, "ri.authn.destination.user.owner")
