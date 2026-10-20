# Asset types

An asset type defines an identity key and property rules for a class of assets.
Assets can belong to multiple types. Every member must satisfy all of its types:
the identity key is a nonempty string, and additional properties can be required
or suggested, with string or numeric values.

## Define a type and create an asset

```python
from nominal.core import AssetTypeProperty, NominalClient

client = NominalClient.from_profile("production")
pump_type = client.create_asset_type(
    "Pump",
    primary_key="serial_number",
    properties={
        "model": AssetTypeProperty(required=True),
        "rated_power_kw": AssetTypeProperty(value_type="number"),
    },
)
pump = client.create_asset(
    "Pump P-001",
    types=[pump_type],
    properties={
        "serial_number": "P-001",
        "model": "Centrifugal",
        "rated_power_kw": 12.5,
    },
)
print(pump.nominal_url)
print(pump_type.nominal_url)
```

Creation uses the client's default workspace. Each `create_asset_type` call creates
a new resource; a name is not an identity. Save its RID and reuse it with
`client.get_asset_type(rid)` on subsequent runs. Type objects and RIDs are both
accepted wherever a type is assigned or selected.

## Find types and their members

```python
available_types = client.search_asset_types("Pump")
pumps = client.search_assets(types=[pump_type])
assert pump_type.rid in pump.type_rids
current_definitions = pump.get_types()
```

Searches default to active resources in the client's default workspace. Pass
`workspace=WorkspaceSearchType.ALL` (imported from `nominal.core`) to search all
accessible workspaces. `search_assets(types=[...])` matches **any** selected type;
an empty list matches no assets, while `None` omits the filter.

`type_rids` is a snapshot. `get_types()` fetches definitions for those memberships;
call `pump.refresh()` first if memberships may have changed remotely.

## Resolve an asset by identity

```python
same_pump = client.create_or_update_asset_by_primary_key(
    pump_type,
    name="Pump P-001",
    properties={"serial_number": "P-001", "model": "Centrifugal"},
    if_exists="return",
)
assert same_pump.rid == pump.rid
```

The server resolves concurrent requests with the same type and identity to one
active asset. This differs from `get_or_create_asset_by_properties`, which searches
before creating and does not provide an atomic identity guarantee. Archived assets
are ignored: an identity held only by an archived asset can create a new active asset.

You must choose an `if_exists` policy. `"return"` leaves existing assets unchanged.
**`"update"` replaces metadata and memberships; it is not a patch.** It replaces the
name, description, properties and labels, clears links and legacy data scopes, and
keeps only the identity type plus the additional `types` you supply. The server
preserves attachments and attached data. Prefer `pump.update(...)` for partial edits.

## Update definitions and memberships

```python
pump_type.update(description="Pumps tracked by manufacturer serial number")
pump.update(types=[pump_type])
```

Omitting `types` leaves memberships unchanged; `types=[]` removes them all. The
server validates the resulting properties and memberships together, so pass both
in one `update` when adding a type whose required properties are not yet present.

For type definitions, omitted properties leave the rules unchanged, while
`pump_type.update(properties={})` removes all additional property rules. The identity
key is separate. The server rejects changes incompatible with active members and
primary-key renames once memberships exist.

`pump_type.archive()` hides a type from default searches without removing existing
memberships. Archived types cannot be assigned to new members. Use `unarchive()` to
restore one and `refresh()` to update the local snapshot after either operation.
Type search accepts `ArchiveStatusFilter.ANY` or `.ARCHIVED` from `nominal.core`.

The SDK exposes identity, property rules and membership. Required-series definitions
and inter-type links are outside this workflow. UI controls may require feature
flag enablement even when the API is available.
