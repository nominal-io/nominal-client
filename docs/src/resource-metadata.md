# Resource metadata discovery

Use `MetadataResourceType` with `NominalClient.list_labels`, `list_property_keys`, and
`list_property_values` to discover metadata already used on a resource family:

```python
from nominal.core import MetadataResourceType, WorkspaceSearchType

labels = client.list_labels(MetadataResourceType.ASSET)
keys = client.list_property_keys(MetadataResourceType.RUN)
tail_numbers = client.list_property_values(
    MetadataResourceType.DATASET,
    "tail_number",
    workspace=WorkspaceSearchType.ALL,
)
```

All three methods return distinct ordinary strings in alphabetical order. They resolve the
client's default workspace when `workspace` is omitted, `None`, or `WorkspaceSearchType.DEFAULT`.
Pass a `Workspace` or workspace RID to select one workspace, or `WorkspaceSearchType.ALL` to
search every permitted workspace.

Assets, runs, datasets, events, and videos use paginated metadata searches across active indexed
documents. Every page is retrieved, including results beyond the aggregate endpoint's 500-item
limit. Workbooks, workbook templates, and checklists use their dedicated backend metadata
routes scoped to permitted workspaces. These include draft and archived workbooks, unpublished
and archived workbook templates, and retained metadata for archived or deleted checklists.

Property key discovery may include numeric properties. Property value discovery returns
string values only; it returns an empty list when a key has no string values. Discovery is
available for the eight resource families in `MetadataResourceType`.
