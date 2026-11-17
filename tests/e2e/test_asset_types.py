from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Barrier
from typing import Callable
from uuid import uuid4

import pytest

from nominal.core import (
    ArchiveStatusFilter,
    Asset,
    AssetType,
    AssetTypeProperty,
    AssetTypePropertyValueType,
    NominalClient,
)
from nominal.exceptions import NominalInvalidArgumentError


@pytest.fixture
def identity_type(client: NominalClient, archive: Callable[[object], None]) -> AssetType:
    """A fresh identity namespace for each test, archived even when an assertion fails."""
    kind = client.create_asset_type(f"SDK identity verification {uuid4()}", primary_key="serial")
    archive(kind)
    return kind


def test_asset_type_identity_and_lifecycle(client: NominalClient, archive) -> None:
    """Type rules, numeric properties, identity resolution and archive lifecycle work together."""
    name = f"SDK asset-type verification {uuid4()}"
    kind = client.create_asset_type(
        name,
        primary_key="serial_number",
        properties={"power": AssetTypeProperty(value_type=AssetTypePropertyValueType.NUMBER, required=True)},
        description="Temporary SDK integration test; archived on completion.",
    )
    archive(kind)
    assert client.get_asset_type(kind.rid).properties == kind.properties
    assert kind.rid in {value.rid for value in client.search_asset_types(name)}
    kind.update(description="Updated integration-test definition")
    assert kind.description == "Updated integration-test definition"
    kind.archive()
    assert kind.refresh().is_archived
    assert kind.rid in {
        value.rid for value in client.search_asset_types(name, archive_status=ArchiveStatusFilter.ARCHIVED)
    }
    kind.unarchive()
    assert not kind.refresh().is_archived

    asset = client.create_asset(
        name,
        types=[kind],
        properties={"serial_number": "one", "power": 12.5},
        labels=["sdk-verification"],
    )
    archive(asset)
    assert asset.properties["power"] == 12.5
    assert asset.type_rids == (kind.rid,)

    assert [value.rid for value in asset.get_types()] == [kind.rid]
    assert asset.rid in {value.rid for value in client.search_assets(types=[kind])}
    assert client.search_assets(types=[]) == []
    with pytest.raises(NominalInvalidArgumentError):
        asset.update(properties={"serial_number": "one"})

    existing = client.create_or_update_asset_by_primary_key(
        kind,
        name="Ignored",
        properties={"serial_number": "one", "power": 20.0},
    )
    assert existing.rid == asset.rid
    assert existing.name == name
    assert existing.properties["power"] == 12.5
    replaced = client.create_or_update_asset_by_primary_key(
        kind,
        name=name,
        properties={"serial_number": "one", "power": 20.0},
        update_if_exists=True,
    )
    assert replaced.rid == asset.rid
    assert replaced.properties["power"] == 20.0
    assert not replaced.labels
    asset.update(types=[])
    assert asset.type_rids == ()
    asset.update(types=[kind])
    assert asset.type_rids == (kind.rid,)


@pytest.mark.parametrize("update_if_exists", [False, True])
def test_concurrent_identity_calls_resolve_one_asset(
    client: NominalClient,
    identity_type: AssetType,
    archive: Callable[[object], None],
    update_if_exists: bool,
) -> None:
    """Four simultaneous creates for one identity resolve to one active asset under either policy."""
    start = Barrier(4, timeout=30)

    def resolve() -> Asset:
        start.wait()
        return client.create_or_update_asset_by_primary_key(
            identity_type, name="Concurrent pump", properties={"serial": "one"}, update_if_exists=update_if_exists
        )

    futures = []
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(resolve) for _ in range(4)]
            assets = [future.result() for future in futures]
        assert len({asset.rid for asset in assets}) == 1
        assert {asset.rid for asset in client.search_assets(types=[identity_type])} == {assets[0].rid}
    finally:
        # The executor settles every call before teardown, including successful calls
        # after another call failed. Register each distinct resource exactly once.
        completed = {
            future.result().rid: future.result()
            for future in futures
            if not future.cancelled() and future.exception() is None
        }
        for asset in completed.values():
            archive(asset)


def test_archived_asset_identity_can_be_reused(
    client: NominalClient, identity_type: AssetType, archive: Callable[[object], None]
) -> None:
    """An archived identity creates a new active asset while retaining the archived resource."""
    original = client.create_or_update_asset_by_primary_key(
        identity_type, name="Original", properties={"serial": "one"}
    )
    archive(original)
    original.archive()
    replacement = client.create_or_update_asset_by_primary_key(
        identity_type, name="Replacement", properties={"serial": "one"}
    )
    archive(replacement)
    assert replacement.rid != original.rid
    assert original.refresh().is_archived
    assert not replacement.is_archived
    assert {asset.rid for asset in client.search_assets(types=[identity_type])} == {replacement.rid}
    assert {
        asset.rid for asset in client.search_assets(types=[identity_type], archive_status=ArchiveStatusFilter.ANY)
    } == {
        original.rid,
        replacement.rid,
    }


def test_membership_validation_is_atomic_and_archived_definitions_remain_readable(
    client: NominalClient, identity_type: AssetType, archive: Callable[[object], None]
) -> None:
    """Rejected membership edits leave the asset unchanged; archived definitions remain on existing members."""
    rated_type = client.create_asset_type(
        f"SDK rated verification {uuid4()}",
        primary_key="serial",
        properties={"power": AssetTypeProperty(AssetTypePropertyValueType.NUMBER, required=True)},
    )
    archive(rated_type)
    asset = client.create_asset("Pump", types=[identity_type], properties={"serial": "one"})
    archive(asset)

    with pytest.raises(NominalInvalidArgumentError):
        asset.update(types=[identity_type, rated_type])
    asset.refresh()
    assert asset.type_rids == (identity_type.rid,)
    assert asset.properties == {"serial": "one"}

    asset.update(types=[identity_type, rated_type], properties={"serial": "one", "power": 12.5})
    assert set(asset.type_rids) == {identity_type.rid, rated_type.rid}
    with pytest.raises(NominalInvalidArgumentError):
        asset.update(properties={"serial": "one", "power": "not numeric"})
    assert asset.refresh().properties["power"] == 12.5

    rated_type.archive()
    definitions = {kind.rid: kind for kind in asset.get_types()}
    assert definitions[rated_type.rid].is_archived
    assert set(definitions) == {identity_type.rid, rated_type.rid}
    asset.update(types=[identity_type], properties={"serial": "one"})
    assert asset.type_rids == (identity_type.rid,)
    assert asset.properties == {"serial": "one"}


def test_identity_policies_preserve_or_replace_metadata_without_losing_attachments(
    client: NominalClient, identity_type: AssetType, archive: Callable[[object], None]
) -> None:
    """Return preserves metadata and memberships; update replaces them while preserving attachments."""
    other_type = client.create_asset_type(f"SDK secondary verification {uuid4()}", primary_key="serial")
    archive(other_type)
    attachment = client.create_attachment_from_io(BytesIO(b"verification evidence"), "asset-type-evidence.txt")
    archive(attachment)
    asset = client.create_asset(
        "Original",
        description="Original description",
        types=[identity_type, other_type],
        properties={"serial": "one", "retired": "value"},
        labels=["original"],
    )
    archive(asset)
    asset.add_attachments([attachment])

    existing = client.create_or_update_asset_by_primary_key(
        identity_type, name="Replacement", properties={"serial": "one", "power": 20.0}
    )
    assert existing.rid == asset.rid
    assert existing.name == "Original"
    assert existing.description == "Original description"
    assert existing.properties == {"serial": "one", "retired": "value"}
    assert existing.labels == ("original",)
    assert set(existing.type_rids) == {identity_type.rid, other_type.rid}

    replaced = client.create_or_update_asset_by_primary_key(
        identity_type, name="Replacement", properties={"serial": "one", "power": 20.0}, update_if_exists=True
    )
    assert replaced.rid == asset.rid
    assert replaced.name == "Replacement"
    assert replaced.description == ""
    assert replaced.properties == {"serial": "one", "power": 20.0}
    assert not replaced.labels
    assert replaced.type_rids == (identity_type.rid,)
    assert [value.rid for value in replaced.list_attachments()] == [attachment.rid]
