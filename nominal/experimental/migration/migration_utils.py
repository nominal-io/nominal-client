import json
import logging
import re
import uuid
from typing import Any, Union, cast

from conjure_python_client import ConjureBeanType, ConjureEnumType, ConjureUnionType
from conjure_python_client._serde.encoder import ConjureEncoder
from nominal_api import scout_layout_api, scout_template_api, scout_workbookcommon_api

from nominal.core import NominalClient, WorkbookTemplate

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
    labels: list[str] | None = None,
    properties: dict[str, str] | None = None,
    commit_message: str | None = None,
) -> WorkbookTemplate:
    """Create a workbook template with specified content and layout.

    This is a helper method that constructs and creates a workbook template
    request with the provided parameters, including layout and content.  We are keeping this
    in the experimental folder for now because the layout and content properties should stay under
    tight control. The template is created in the target workspace and is not published by default.

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
    request: scout_template_api.CreateTemplateRequest = scout_template_api.CreateTemplateRequest(
        title=title,
        description=description if description is not None else "",
        labels=labels if labels is not None else [],
        properties=properties if properties is not None else {},
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
                logger.debug("Replacing key %s with %s", key, new_key)
                new_obj[new_key] = _replace_uuids_in_obj(value, mapping)
            elif isinstance(value, str) and value in mapping:
                logger.debug("Replacing value %s with %s", value, mapping[value])
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


def _clone_conjure_objects_with_new_uuids(objs: list[ConjureType]) -> list[ConjureType]:
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
    json_objs: list[Any] = [ConjureEncoder.do_encode(obj) for obj in objs]

    mapping = _generate_uuid_mapping(json_objs)
    logger.debug("Generated id mapping: ")
    for old_id, new_id in mapping.items():
        logger.debug("%s -> %s", old_id, new_id)

    new_json_objs = [_replace_uuids_in_obj(json_obj, mapping) for json_obj in json_objs]

    return [cast(ConjureType, new_json_obj) for new_json_obj in new_json_objs]


def clone_workbook_template(
    source_template: WorkbookTemplate,
    target_client: NominalClient,
    workspace_rid: str,
    new_template_name: str | None = None,
) -> WorkbookTemplate:
    """Clone a workbook template from the source to the target workspace.

    Retrieves the source template, clones its layout and content.  For safety, we replace
    all unique identifiers with new UUIDs. We then creates a new template in the target workspace.
    The cloned template maintains all metadata (title, description, labels, properties).

    Args:
        source_template: The source WorkbookTemplate to clone.
        target_client: The NominalClient to create the cloned template in.
        workspace_rid: The resource ID of the target workspace.
        new_template_name: Optional new name for the cloned template. If not provided, the original name is used.

    Returns:
        The newly created WorkbookTemplate in the target workspace.
    """
    raw_source_template: scout_template_api.Template = source_template._clients.template.get(
        source_template._clients.auth_header, source_template.rid
    )

    template_layout: scout_layout_api.WorkbookLayout = raw_source_template.layout
    template_content: scout_workbookcommon_api.WorkbookContent = raw_source_template.content
    [new_template_layout_generic, new_workbook_content_generic] = _clone_conjure_objects_with_new_uuids(
        [template_layout, template_content]
    )
    new_template_layout = cast(scout_layout_api.WorkbookLayout, new_template_layout_generic)
    new_workbook_content = cast(scout_workbookcommon_api.WorkbookContent, new_workbook_content_generic)

    return create_workbook_template_with_content_and_layout(
        client=target_client,
        title=new_template_name if new_template_name is not None else raw_source_template.metadata.title,
        description=raw_source_template.metadata.description,
        labels=raw_source_template.metadata.labels,
        properties=raw_source_template.metadata.properties,
        layout=new_template_layout,
        content=new_workbook_content,
        commit_message="Cloned from template",
        workspace_rid=workspace_rid,
    )
