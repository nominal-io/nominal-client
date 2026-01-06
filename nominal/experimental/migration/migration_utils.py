import json
import logging
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Mapping, Sequence, TypeVar, Union, cast, overload

import requests
from conjure_python_client import ConjureBeanType, ConjureEnumType, ConjureUnionType
from conjure_python_client._serde.decoder import ConjureDecoder
from conjure_python_client._serde.encoder import ConjureEncoder
from nominal_api import scout_layout_api, scout_template_api, scout_workbookcommon_api

from nominal.core import (
    Asset,
    Dataset,
    DatasetFile,
    Event,
    FileType,
    NominalClient,
    Workbook,
    WorkbookTemplate,
)
from nominal.core._event_types import EventType, SearchEventOriginType
from nominal.core._utils.api_tools import Link, LinkDict
from nominal.core.attachment import Attachment
from nominal.core.run import Run
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
)

logger = logging.getLogger(__name__)

ConjureType = Union[ConjureBeanType, ConjureUnionType, ConjureEnumType]

# Regex pattern to match strings that have a UUID format with a prefix.
UUID_PATTERN = re.compile(r"^(.*)([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")

# Keeping tight control over ids we consider to be UUIDs.
UUID_KEYS = ("id", "rid", "functionUuid", "plotId", "yAxisId", "chartRid")


def _convert_if_json(s: str) -> tuple[Any, bool]:
    """If the string is a valid JSON, convert it. Otherwise, return original object.

    Args:
        s: The string to to convert or return as is.

    Returns:
        The parsed JSON object if s is valid JSON, else the original string. Also returns the action taken in a boolean.
    """
    try:
        return (json.loads(s), True)
    except (ValueError, TypeError):
        return (s, False)


def _check_and_add_uuid_to_mapping(input_str: str, mapping: dict[str, str]) -> None:
    """Check if a string matches the UUID pattern and add to mapping if not already present.

    Args:
        input_str: The string to check for UUID pattern.
        mapping: The mapping dictionary to add to if a new UUID is found.
    """
    match = UUID_PATTERN.search(input_str)
    if match and input_str not in mapping:
        mapping[input_str] = f"{match.group(1)}{str(uuid.uuid4())}"
        logger.debug("Found UUID and added to mapping: %s -> %s", input_str, mapping[input_str])


def _extract_uuids_from_obj(obj: Any, mapping: dict[str, str]) -> None:
    """Recursively extract UUIDs from a nested JSON object, and populate the mapping.

    Searches for UUIDs in:
    - Values of specific keys (defined in UUID_KEYS)
    - Dictionary keys that match the UUID pattern
    - Nested JSON strings that are parsed and searched recursively

    Args:
        obj: The object to search (dict, list, or primitive).
        mapping: Dictionary to populate with found UUIDs as keys.
    """
    # TODO (Sean): Refactor to remove expensive recursion strategy.
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in UUID_KEYS and isinstance(value, str):
                _check_and_add_uuid_to_mapping(value, mapping)
            else:
                _check_and_add_uuid_to_mapping(key, mapping)
            # Some values may be JSON strings that need to be parsed.
            _extract_uuids_from_obj(_convert_if_json(value)[0] if isinstance(value, str) else value, mapping)
    elif isinstance(obj, list):
        for item in obj:
            _extract_uuids_from_obj(item, mapping)


def _generate_uuid_mapping(objs: list[Any]) -> dict[str, str]:
    """Search for all UUIDs in a list of objects and generate a mapping to new UUIDs.

    Args:
        objs: List of objects to search for UUIDs.

    Returns:
        A mapping of all UUIDs found in the objects with their new generated UUIDs.
    """
    mapping: dict[str, str] = {}
    for obj in objs:
        _extract_uuids_from_obj(obj, mapping)
    return mapping


def create_workbook_template_with_content_and_layout(
    client: NominalClient,
    title: str,
    layout: scout_layout_api.WorkbookLayout,
    content: scout_workbookcommon_api.WorkbookContent,
    workspace_rid: str,
    *,
    description: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
    commit_message: str | None = None,
) -> WorkbookTemplate:
    """Create a workbook template with specified content and layout.

    This is a helper method that constructs and creates a workbook template
    request with the provided parameters, including layout and content.  Method is considered experimental and may
    change in future releases. The template is created in the target workspace and is not discoverable by default.

    Args:
        client: The NominalClient to use for creating the template.
        title: The title of the template.
        layout: The workbook layout to use.
        content: The workbook content to use.
        workspace_rid: The resource ID of the workspace to create the template in.
        description: The description of the template.
        labels: List of labels to apply to the template.
        properties: Dictionary of properties for the template.
        commit_message: The commit message for the template creation.

    Returns:
        The newly created WorkbookTemplate.
    """
    request = scout_template_api.CreateTemplateRequest(
        title=title,
        description=description if description is not None else "",
        labels=list(labels) if labels is not None else [],
        properties=dict(properties) if properties is not None else {},
        is_published=False,
        layout=layout,
        content=content,
        message=commit_message if commit_message is not None else "",
        workspace=client._workspace_rid_for_search(workspace_rid),
    )

    template = client._clients.template.create(client._clients.auth_header, request)
    return WorkbookTemplate._from_conjure(client._clients, template)


def _replace_uuids_in_obj(obj: Any, mapping: dict[str, str]) -> Any:
    """Recursively replace UUIDs in a nested JSON object.

    Replaces UUIDs found in:
    - Dictionary keys that match UUID pattern and are in the mapping
    - String values that are in the mapping
    - Nested JSON strings that are parsed, processed, and re-serialized

    Args:
        obj: The object to process (dict, list, or primitive).
        mapping: Dictionary mapping old UUIDs to new UUIDs.

    Returns:
        A new object with all UUIDs replaced according to the mapping.
        Primitive values are returned unchanged if they don't match any UUIDs.
    """
    if isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            if isinstance(key, str) and re.search(UUID_PATTERN, key) and key in mapping:
                new_key = mapping[key]
                new_obj[new_key] = _replace_uuids_in_obj(value, mapping)
            elif isinstance(value, str) and value in mapping:
                new_obj[key] = mapping[value]
            elif isinstance(value, str):
                parsed_value, was_json = _convert_if_json(value)
                if was_json:
                    new_obj[key] = json.dumps(_replace_uuids_in_obj(parsed_value, mapping), separators=(",", ":"))
                else:
                    new_obj[key] = _replace_uuids_in_obj(value, mapping)
            else:
                new_obj[key] = _replace_uuids_in_obj(value, mapping)
        return new_obj
    elif isinstance(obj, list):
        return [_replace_uuids_in_obj(item, mapping) for item in obj]
    else:
        return obj


T1 = TypeVar("T1", bound=ConjureType)
T2 = TypeVar("T2", bound=ConjureType)


@overload
def _clone_conjure_objects_with_new_uuids(
    objs: tuple[T1, T2],
) -> tuple[T1, T2]: ...


@overload
def _clone_conjure_objects_with_new_uuids(objs: list[ConjureType]) -> list[ConjureType]: ...


def _clone_conjure_objects_with_new_uuids(
    objs: tuple[ConjureType, ...] | list[ConjureType],
) -> tuple[ConjureType, ...] | list[ConjureType]:
    """Clone Conjure objects by replacing all UUIDs with new ones.

    This function:
    1. Converts Conjure objects to JSON
    2. Finds all UUIDs in the JSON structures, according to defined keys
    3. Generates new UUIDs for each old UUID, maintaining prefixes
    4. Replaces all UUIDs in the objects
    5. Returns the cloned objects with new UUIDs

    Args:
        objs: List of Conjure objects to clone.

    Returns:
        List of cloned Conjure objects with new UUIDs. The structure and content
        are identical to the originals, but all UUIDs have been replaced.
    """
    original_types = [type(obj) for obj in objs]

    json_objs = [ConjureEncoder.do_encode(obj) for obj in objs]

    mapping = _generate_uuid_mapping(json_objs)

    new_json_objs = [_replace_uuids_in_obj(json_obj, mapping) for json_obj in json_objs]

    # Deserialize each dict back to its original type
    decoder = ConjureDecoder()
    result = [
        decoder.do_decode(new_json_obj, obj_type) for new_json_obj, obj_type in zip(new_json_objs, original_types)
    ]

    return tuple(result) if isinstance(objs, tuple) else result


# TODO (Sean): Once we move this out of experimental, make clone/copy_resource_from abstract methods in the HasRid class
def clone_workbook_template(
    source_template: WorkbookTemplate,
    destination_client: NominalClient,
) -> WorkbookTemplate:
    """Clones a workbook template, maintaining all properties and content.

    Args:
        source_template (WorkbookTemplate): The template to copy
        destination_client (NominalClient): The client to copy to
    Returns:
        The cloned template.
    """
    return copy_workbook_template_from(
        source_template=source_template,
        destination_client=destination_client,
        include_content_and_layout=True,
    )


def copy_workbook_template_from(
    source_template: WorkbookTemplate,
    destination_client: NominalClient,
    *,
    new_template_title: str | None = None,
    new_template_description: str | None = None,
    new_template_labels: Sequence[str] | None = None,
    new_template_properties: Mapping[str, str] | None = None,
    include_content_and_layout: bool = False,
) -> WorkbookTemplate:
    """Clone a workbook template from the source to the target workspace.

    Retrieves the source template, clones its layout and content.  For safety, we replace
    all unique identifiers with new UUIDs. We then creates a new template in the target workspace.
    The cloned template maintains all metadata (title, description, labels, properties).

    Args:
        source_template: The source WorkbookTemplate to clone.
        destination_client: The NominalClient to create the cloned template in.
        new_template_title: Optional new name for the cloned template. If not provided, the original is used.
        new_template_description: Optional new name for the cloned template. If not provided, the original is used.
        new_template_labels: Optional new labels for the cloned template. If not provided, the original is used.
        new_template_properties: Optional new properties for the cloned template. If not provided, the original is used.
        include_content_and_layout: If True, copy layout and content from template. Otherwise, use blank content.

    Returns:
        The newly created WorkbookTemplate in the target workspace.
    """
    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid
    }
    logger.debug(
        "Cloning workbook template: %s (rid: %s)", source_template.title, source_template.rid, extra=log_extras
    )
    raw_source_template = source_template._clients.template.get(
        source_template._clients.auth_header, source_template.rid
    )

    if include_content_and_layout:
        template_layout = raw_source_template.layout
        template_content = raw_source_template.content
        (new_template_layout, new_workbook_content) = _clone_conjure_objects_with_new_uuids(
            (template_layout, template_content)
        )
    else:
        new_template_layout = scout_layout_api.WorkbookLayout(
            v1=scout_layout_api.WorkbookLayoutV1(
                root_panel=scout_layout_api.Panel(
                    tabbed=scout_layout_api.TabbedPanel(
                        v1=scout_layout_api.TabbedPanelV1(
                            id=str(uuid.uuid4()),
                            tabs=[],
                        )
                    )
                )
            )
        )
        new_workbook_content = scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={})
    new_workbook_template = create_workbook_template_with_content_and_layout(
        client=destination_client,
        title=new_template_title or raw_source_template.metadata.title,
        description=new_template_description or raw_source_template.metadata.description,
        labels=new_template_labels or raw_source_template.metadata.labels,
        properties=new_template_properties or raw_source_template.metadata.properties,
        layout=new_template_layout,
        content=new_workbook_content,
        commit_message="Cloned from template",
        workspace_rid=destination_client.get_workspace(destination_client._clients.workspace_rid).rid,
    )
    logger.debug(
        "New workbook template created %s from %s (rid: %s)",
        new_workbook_template.title,
        source_template.title,
        source_template.rid,
        extra=log_extras,
    )
    return new_workbook_template


def copy_file_to_dataset(
    source_file: DatasetFile,
    destination_dataset: Dataset,
) -> DatasetFile:
    """Copy a dataset file from the source to the destination dataset.

    Args:
        source_file: The source DatasetFile to copy.
        destination_dataset: The Dataset to create the copied file in.

    Returns:
        The dataset file in the new dataset.
    """
    log_extras = {"destination_client_workspace": destination_dataset._clients.workspace_rid}
    logger.debug("Copying dataset file: %s", source_file.name, extra=log_extras)
    source_api_file = source_file._get_latest_api()
    if (
        source_api_file.handle.s3 is not None
        and source_file.timestamp_channel is not None
        and source_file.timestamp_type is not None
    ):
        old_file_uri = source_file._clients.catalog.get_dataset_file_uri(
            source_file._clients.auth_header, source_file.dataset_rid, source_file.id
        ).uri

        response = requests.get(old_file_uri, stream=True)
        response.raise_for_status()

        file_name = source_api_file.handle.s3.key.split("/")[-1]
        file_type = FileType.from_path(file_name)
        file_stem = Path(file_name).stem

        new_file = destination_dataset.add_from_io(
            dataset=cast(BinaryIO, response.raw),
            timestamp_column=source_file.timestamp_channel,
            timestamp_type=source_file.timestamp_type,
            file_type=file_type,
            file_name=file_stem,
            tag_columns=source_file.tag_columns,
            tags=source_file.file_tags,
        )
        logger.debug(
            "New file created %s in dataset: %s (rid: %s)",
            new_file.name,
            destination_dataset.name,
            destination_dataset.rid,
        )
        return new_file
    else:  # Because these fields are optional, need to check for None. We shouldn't ever run into this.
        raise ValueError("Unsupported file handle type or missing timestamp information.")


def clone_dataset(source_dataset: Dataset, destination_client: NominalClient) -> Dataset:
    """Clones a dataset, maintaining all properties and files.

    Args:
        source_dataset (Dataset): The dataset to copy from.
        destination_client (NominalClient): The destination client.

    Returns:
        The cloned dataset.
    """
    return copy_dataset_from(source_dataset=source_dataset, destination_client=destination_client, include_files=True)


def copy_dataset_from(
    source_dataset: Dataset,
    destination_client: NominalClient,
    *,
    new_dataset_name: str | None = None,
    new_dataset_description: str | None = None,
    new_dataset_properties: dict[str, Any] | None = None,
    new_dataset_labels: Sequence[str] | None = None,
    include_files: bool = False,
) -> Dataset:
    """Copy a dataset from the source to the destination client.

    Args:
        source_dataset: The source Dataset to copy.
        destination_client: The NominalClient to create the copied dataset in.
        new_dataset_name: Optional new name for the copied dataset. If not provided, the original name is used.
        new_dataset_description: Optional new description for the copied dataset.
            If not provided, the original description is used.
        new_dataset_properties: Optional new properties for the copied dataset. If not provided, the original
            properties are used.
        new_dataset_labels: Optional new labels for the copied dataset. If not provided, the original labels are used.
        include_files: Whether to include files in the copied dataset.

    Returns:
        The newly created Dataset in the destination client.
    """
    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid
    }
    logger.debug(
        "Copying dataset %s (rid: %s)",
        source_dataset.name,
        source_dataset.rid,
        extra=log_extras,
    )
    new_dataset = destination_client.create_dataset(
        name=new_dataset_name if new_dataset_name is not None else source_dataset.name,
        description=new_dataset_description if new_dataset_description is not None else source_dataset.description,
        properties=new_dataset_properties if new_dataset_properties is not None else source_dataset.properties,
        labels=new_dataset_labels if new_dataset_labels is not None else source_dataset.labels,
    )
    if include_files:
        for source_file in source_dataset.list_files():
            copy_file_to_dataset(source_file, new_dataset)
    logger.debug("New dataset created: %s (rid: %s)", new_dataset.name, new_dataset.rid, extra=log_extras)
    return new_dataset


def clone_event(source_event: Event, destination_client: NominalClient) -> Event:
    """Clones an event, maintaining all properties and linked assets.

    Args:
        source_event (Event): The event to copy from.
        destination_client (NominalClient): The destination client.

    Returns:
        The cloned event.
    """
    return copy_event_from(source_event=source_event, destination_client=destination_client)


def copy_event_from(
    source_event: Event,
    destination_client: NominalClient,
    *,
    new_name: str | None = None,
    new_type: EventType | None = None,
    new_start: datetime | IntegralNanosecondsUTC | None = None,
    new_duration: timedelta | IntegralNanosecondsDuration = timedelta(),
    new_description: str | None = None,
    new_assets: Iterable[Asset | str] = (),
    new_properties: Mapping[str, str] | None = None,
    new_labels: Iterable[str] = (),
) -> Event:
    """Copy an event from the source to the destination client.

    Args:
        source_event: The source Event to copy.
        destination_client: The NominalClient to create the copied event in.
        new_name: Optional new name for the copied event. If not provided, the original name is used.
        new_type: Optional new type for the copied event. If not provided, the original type is used.
        new_start: Optional new start time for the copied event. If not provided, the original start time is used.
        new_duration: Optional new duration for the copied event. If not provided, the original duration is used.
        new_description: Optional new description for the copied event. If not provided, the original description used.
        new_assets: Optional new assets for the copied event. If not provided, the original assets are used.
        new_properties: Optional new properties for the copied event. If not provided, the original properties are used.
        new_labels: Optional new labels for the copied event. If not provided, the original labels are used.

    Returns:
        The newly created Event in the destination client.
    """
    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid
    }
    logger.debug(
        "Copying event %s (rid: %s)",
        source_event.name,
        source_event.rid,
        extra=log_extras,
    )
    new_event = destination_client.create_event(
        name=new_name or source_event.name,
        type=new_type or source_event.type,
        start=new_start or source_event.start,
        duration=new_duration or source_event.duration,
        description=new_description or source_event.description,
        assets=new_assets or source_event.asset_rids,
        properties=new_properties or source_event.properties,
        labels=new_labels or source_event.labels,
    )
    logger.debug("New event created: %s (rid: %s)", new_event.name, new_event.rid, extra=log_extras)
    return new_event


def clone_run(source_run: Run, destination_client: NominalClient) -> Run:
    """Clones a run, maintaining all properties, linked assets, and attachments.

    Args:
        source_run: The run to copy from.
        destination_client: The destination client.

    Returns:
        The cloned run.
    """
    return copy_run_from(source_run=source_run, destination_client=destination_client)


def copy_run_from(
    source_run: Run,
    destination_client: NominalClient,
    *,
    new_name: str | None = None,
    new_start: datetime | IntegralNanosecondsUTC | None = None,
    new_end: datetime | IntegralNanosecondsUTC | None = None,
    new_description: str | None = None,
    new_properties: Mapping[str, str] | None = None,
    new_labels: Sequence[str] = (),
    new_links: Sequence[str | Link | LinkDict] = (),
    new_attachments: Iterable[Attachment] | Iterable[str] = (),
    new_assets: Sequence[Asset | str] = (),
) -> Run:
    """Copy a run from the source to the destination client.

    Args:
        source_run: The source Run to copy.
        destination_client: The NominalClient to create the copied run in.
        new_name: Optionally override the name of the copied run. Defaults to original name.
        new_start: Optionally override the start time of the copied run. Defaults to original start time.
        new_end: Optionally override the end time of the copied run. Defaults to original end time.
        new_description: Optionally override the description of the copied run. Defaults to original description.
        new_properties: Optionally override the properties of the copied run. Defaults to original properties.
        new_labels: Optionally override the labels of the copied run. Defaults to original labels.
        new_links: Optionally override the links of the copied run. Defaults to original links.
        new_attachments: Optionally override the attachments of the copied run. Defaults to original attachments.
        new_assets: Optionally override the linked assets of the copied run. Defaults to original linked assets.

    Returns:
        The newly created Run in the destination client.
    """
    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid
    }
    logger.debug(
        "Copying run %s (rid: %s)",
        source_run.name,
        source_run.rid,
        extra=log_extras,
    )
    new_run = destination_client.create_run(
        name=new_name or source_run.name,
        start=new_start or source_run.start,
        end=new_end or source_run.end,
        description=new_description or source_run.description,
        properties=new_properties or source_run.properties,
        labels=new_labels or source_run.labels,
        assets=new_assets or source_run.assets,
        links=new_links or source_run.links,
        attachments=new_attachments or source_run.list_attachments(),
    )
    logger.debug("New run created: %s (rid: %s)", new_run.name, new_run.rid, extra=log_extras)
    return new_run


def clone_asset(
    source_asset: Asset,
    destination_client: NominalClient,
) -> Asset:
    """Clone an asset from the source to the target client.

    Args:
        source_asset: The source Asset to clone.
        destination_client: The NominalClient to create the cloned asset in.

    Returns:
        The newly created Asset in the target client.
    """
    return copy_asset_from(
        source_asset=source_asset,
        destination_client=destination_client,
        include_data=True,
        include_events=True,
        include_runs=True,
    )


def copy_asset_from(
    source_asset: Asset,
    destination_client: NominalClient,
    *,
    new_asset_name: str | None = None,
    new_asset_description: str | None = None,
    new_asset_properties: dict[str, Any] | None = None,
    new_asset_labels: Sequence[str] | None = None,
    include_data: bool = False,
    include_events: bool = False,
    include_runs: bool = False,
) -> Asset:
    """Copy an asset from the source to the destination client.

    Args:
        source_asset: The source Asset to copy.
        destination_client: The NominalClient to create the copied asset in.
        new_asset_name: Optional new name for the copied asset. If not provided, the original name is used.
        new_asset_description: Optional new description for the copied asset. If not provided, original description used
        new_asset_properties: Optional new properties for the copied asset. If not provided, original properties used.
        new_asset_labels: Optional new labels for the copied asset. If not provided, the original labels are used.
        include_data: Whether to include data in the copied asset.
        include_events: Whether to include events in the copied dataset.
        include_runs: Whether to include runs in the copied asset.

    Returns:
        The new asset created.
    """
    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid
    }
    logger.debug("Copying asset %s (rid: %s)", source_asset.name, source_asset.rid, extra=log_extras)
    new_asset = destination_client.create_asset(
        name=new_asset_name if new_asset_name is not None else source_asset.name,
        description=new_asset_description if new_asset_description is not None else source_asset.description,
        properties=new_asset_properties if new_asset_properties is not None else source_asset.properties,
        labels=new_asset_labels if new_asset_labels is not None else source_asset.labels,
    )
    if include_data:
        source_datasets = source_asset.list_datasets()
        for data_scope, source_dataset in source_datasets:
            new_dataset = clone_dataset(
                source_dataset=source_dataset,
                destination_client=destination_client,
            )
            new_asset.add_dataset(data_scope, new_dataset)
    source_asset._list_dataset_scopes

    if include_events:
        source_events = source_asset.search_events(origin_types=SearchEventOriginType.get_manual_origin_types())
        for source_event in source_events:
            copy_event_from(source_event, destination_client, new_assets=[new_asset])

    if include_runs:
        source_runs = source_asset.list_runs()
        for source_run in source_runs:
            copy_run_from(source_run, destination_client, new_assets=[new_asset])

    logger.debug("New asset created: %s (rid: %s)", new_asset, new_asset.rid, extra=log_extras)
    return new_asset


def copy_resources_to_destination_client(
    destination_client: NominalClient,
    source_assets: Sequence[Asset],
    source_workbook_templates: Sequence[WorkbookTemplate],
) -> tuple[Sequence[tuple[str, Dataset]], Sequence[Asset], Sequence[WorkbookTemplate], Sequence[Workbook]]:
    """Based on a list of assets and workbook templates, copy resources to destination client, creating
       new datasets, datafiles, and workbooks along the way.

    Args:
        destination_client (NominalClient): client of the tenant/workspace to copy resources to.
        source_assets (Sequence[Asset]): a list of assets to copy (with data)
        source_workbook_templates (Sequence[WorkbookTemplate]): a list of workbook templates to clone
        and create workbooks from.

    Returns:
        All of the created resources.
    """
    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid,
    }

    if len(source_assets) != 1:
        raise ValueError("Currently, only single asset can be used to create workbook from template")

    new_assets = []
    new_data_scopes_and_datasets: list[tuple[str, Dataset]] = []
    for source_asset in source_assets:
        new_asset = clone_asset(source_asset, destination_client)
        new_assets.append(new_asset)
        new_data_scopes_and_datasets.extend(new_asset.list_datasets())
    new_templates = []
    new_workbooks = []

    for source_workbook_template in source_workbook_templates:
        new_template = clone_workbook_template(source_workbook_template, destination_client)
        new_templates.append(new_template)
        new_workbook = new_template.create_workbook(
            title=new_template.title, description=new_template.description, asset=new_assets[0]
        )
        logger.debug(
            "Created new workbook %s (rid: %s) from template %s (rid: %s)",
            new_workbook.title,
            new_workbook.rid,
            new_template.title,
            new_template.rid,
            extra=log_extras,
        )
        new_workbooks.append(new_workbook)

    return (new_data_scopes_and_datasets, new_assets, new_templates, new_workbooks)
