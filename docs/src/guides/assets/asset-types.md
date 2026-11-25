# Asset types

An asset type defines an identity key and property rules for a class of assets.
Assets can belong to multiple types. Every member must satisfy all of its types:
the identity key is a nonempty string, and additional properties can be required
or suggested, with string or numeric values.

```{include} /guides/_snippets/connect-the-client.md
```

## Define a type and create an asset

Use {py:meth}`~nominal.core.NominalClient.create_asset_type` to define the identity
and {py:class}`~nominal.core.AssetTypeProperty` for additional property rules.

```python
from nominal.core import AssetTypeProperty, AssetTypePropertyValueType

pump_type = client.create_asset_type(
    "Pump",
    primary_key="serial_number",
    properties={
        "model": AssetTypeProperty(required=True),
        "rated_power_kw": AssetTypeProperty(
            value_type=AssetTypePropertyValueType.NUMBER,
        ),
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

`AssetTypeProperty` defaults to `value_type=AssetTypePropertyValueType.STRING`
and `required=False`. Suggested properties may be omitted; required properties
must be present. The primary key is always required and must not be repeated in
the additional property rules.

## Find types and their members

{py:meth}`~nominal.core.NominalClient.search_asset_types` finds definitions;
{py:meth}`~nominal.core.NominalClient.search_assets` finds their members.

```python
available_types = client.search_asset_types("Pump", property_names=["model"])
pumps = client.search_assets(types=[pump_type])
assert pump_type.rid in pump.type_rids
current_definitions = pump.get_types()
```

The `property_names` filter searches additional property rules. A type’s primary key is stored separately and does not match this filter.

Searches default to active resources in the client's default workspace. Pass
`workspace=WorkspaceSearchType.ALL` (imported from `nominal.core`) to search all
accessible workspaces. `search_assets(types=[...])` matches **any** selected type;
an empty list matches no assets, while `None` omits the filter.

The type filter is ANDed with other filters. For example,
`client.search_assets(types=[pump_type], labels=["production", "validated"])`
requires membership in the pump type **and both labels**. With multiple types,
membership in any one of them is sufficient; all supplied labels and property
equalities must still match.

`type_rids` is a snapshot. `get_types()` fetches definitions for those memberships;
call `pump.refresh()` first if memberships may have changed remotely.

## Resolve an asset by identity

Use {py:meth}`~nominal.core.NominalClient.create_or_update_asset_by_primary_key`
when repeated calls must resolve to the same active asset.

```python
same_pump = client.create_or_update_asset_by_primary_key(
    pump_type,
    name="Pump P-001",
    properties={
        "serial_number": "P-001",
        "model": "Centrifugal",
    },
)
assert same_pump.rid == pump.rid
```

The server resolves concurrent requests with the same type and identity to one
active asset. This differs from `get_or_create_asset_by_properties`, which searches
before creating and does not provide an atomic identity guarantee. Archived assets
are ignored: an identity held only by an archived asset can create a new active asset.

Existing assets are returned unchanged by default (`update_if_exists=False`).
```{warning}
`update_if_exists=True` replaces metadata and memberships; it is not a patch.
It replaces the name, description, properties and labels, clears links and legacy
data scopes, and keeps only the identity type plus the additional `types` you
supply. Omitting `description` clears the existing description; omitting `labels`
removes all labels, and omitting `types` removes other memberships. Properties
absent from the supplied mapping are removed.

The server preserves attachments and attached data. Prefer
{py:meth}`~nominal.core.Asset.update` for partial metadata edits.
```

Both creation and replacement use the client's default workspace. Supply every
required property for the identity type and any additional types. The returned
object is a fresh snapshot; an earlier `pump` object is not refreshed by this call.

## Update definitions and memberships

Use {py:meth}`~nominal.core.AssetType.update` to change a definition and
{py:meth}`~nominal.core.Asset.update` to change memberships.

```python
pump_type.update(description="Pumps tracked by manufacturer serial number")
pump.update(types=[pump_type])
```

Omitting `types` leaves memberships unchanged; `types=[]` removes them all. The
server validates the resulting properties and memberships together, so pass both
in one `update` when adding a type whose required properties are not yet present.

For type definitions, omitted properties leave the rules unchanged, while
`pump_type.update(properties={})` removes all additional property rules. The identity
key is separate. The server rejects property changes incompatible with active members and
primary-key renames once memberships exist.

`pump_type.archive()` hides a type from default searches without removing existing
memberships. Archived types cannot be assigned to new members. Use `unarchive()` to
restore one and `refresh()` to update the local snapshot after either operation.
Type search accepts `ArchiveStatusFilter.ANY` or `.ARCHIVED` from `nominal.core`.
Use {py:meth}`~nominal.core.AssetType.delete` to permanently delete an unused type.
Any member, including an archived asset, or a checklist referencing the type prevents deletion.

## Require telemetry on member assets

Use `required_series` when creating or updating a type. Each key is an exact channel
name. The value specifies the kind of telemetry. For example, integer and
floating-point channels both satisfy a numeric requirement.

```python
from nominal.core import SeriesType

pump_type.update(required_series={"outlet_pressure": SeriesType.NUMERIC})
```

For a new pump, replace the placeholder below with the RID of a dataset that
contains `outlet_pressure`. Retrieve it before creating the asset, then attach
it before assigning the type. The search for matching series respects the data scope's tag filters.

```python
dataset = client.get_dataset("<dataset-rid>")
pump = client.create_asset("Pump P-002", properties={"serial_number": "P-002", "model": "Centrifugal"})
pump.add_dataset("telemetry", dataset)
pump.update(types=[pump_type])
```

Changing the type's required series leaves existing members in place. Omit
`required_series` on an update to keep the current requirements, or pass `{}`
to clear them all.

## Link assets by identity

A link definition belongs to the source type. In this example, each pump's
`motor_serial` property identifies a motor by its primary key.

```python
from nominal.core import AssetTypeLink, ResolvedAssetLink

motor_type = client.create_asset_type("Motor", primary_key="serial_number")
pump_type.update(
    properties={**pump_type.properties, "motor_serial": AssetTypeProperty()},
    outgoing_links=[
        *pump_type.outgoing_links,
        AssetTypeLink(target_type_rid=motor_type.rid, source_property="motor_serial"),
    ],
)
motor = client.create_asset("Motor M-01", types=[motor_type], properties={"serial_number": "M-01"})
pump.update(properties={**pump.properties, "motor_serial": "M-01"})

link = next(link for link in pump_type.outgoing_links if link.target_type_rid == motor_type.rid)
for edge in client.traverse_asset_links([pump], link):
    if isinstance(edge, ResolvedAssetLink):
        print(edge.linked_asset_rid)
    else:
        print(f"No visible motor matches {edge.unresolved_value!r}")

pumps_for_motor = client.traverse_asset_links([motor], link, direction="inverse")
```

Read `incoming_links` on the target type after refreshing it. Edit links on the
source type. Supplying `outgoing_links` replaces its full list, so include the
existing link objects you want to keep. An empty list removes all outgoing links.

## Find runs and checklists by asset type

```python
runs = client.search_runs(asset_types=[pump_type, motor_type])
checklists = client.search_checklists(asset_types=[pump_type, motor_type])
```

A run matches when any associated asset has any requested type. A checklist
matches when it declares every requested type. Both methods accept type objects
or RIDs and combine the type filter with the other search arguments. An empty
type list matches no results. Omit the filter to search across all types.
