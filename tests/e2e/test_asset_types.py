from uuid import uuid4

import pytest

from nominal.core import ArchiveStatusFilter, AssetTypeProperty, NominalClient
from nominal.exceptions import NominalInvalidArgumentError


def test_asset_type_identity_and_lifecycle(client: NominalClient, archive) -> None:
    """Type rules, numeric properties, identity resolution and archive lifecycle work together."""
    name = f"SDK asset-type verification {uuid4()}"
    kind = client.create_asset_type(
        name,
        primary_key="serial_number",
        properties={"power": AssetTypeProperty(value_type="number", required=True)},
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
        if_exists="return",
    )
    assert existing.rid == asset.rid
    assert existing.name == name
    assert existing.properties["power"] == 12.5
    replaced = client.create_or_update_asset_by_primary_key(
        kind,
        name=name,
        properties={"serial_number": "one", "power": 20.0},
        if_exists="update",
    )
    assert replaced.rid == asset.rid
    assert replaced.properties["power"] == 20.0
    assert not replaced.labels
    asset.update(types=[])
    assert asset.type_rids == ()
    asset.update(types=[kind])
    assert asset.type_rids == (kind.rid,)
