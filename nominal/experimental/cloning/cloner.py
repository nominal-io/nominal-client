import json
import logging
import re
import uuid
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Union, cast

from conjure_python_client import ConjureBeanType, ConjureEnumType, ConjureUnionType
from conjure_python_client._serde.encoder import ConjureEncoder
from nominal_api import scout_layout_api, scout_template_api, scout_workbookcommon_api

from nominal.core import NominalClient, WorkbookTemplate

logger = logging.getLogger(__name__)

ConjureType = Union[ConjureBeanType, ConjureUnionType, ConjureEnumType]

UUID_PATTERN = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

# Keeping tight control over ids we consider to be UUIDs.
UUID_KEYS = ["id", "rid", "functionUuid", "plotId", "yAxisId", "chartRid"]


def _is_json(s: str) -> bool:
    """Check if a string is valid JSON.

    Args:
        s: The string to validate.

    Returns:
        True if the string is valid JSON, False otherwise.
    """
    try:
        json.loads(s)
        return True
    except (ValueError, TypeError):
        return False


def _extract_uuids_from_obj(obj: Any, uuids: set[str]) -> None:
    """Recursively unique UUIDs from a nested JSON object.

    Searches for UUIDs in:
    - Values of specific keys (defined in UUID_KEYS)
    - Dictionary keys that match the UUID pattern
    - Nested JSON strings that are parsed and searched recursively

    Args:
        obj: The object to search (dict, list, or primitive).
        uuids: A set to accumulate found UUIDs (modified in place).
    """
    if isinstance(obj, dict):
        for key in UUID_KEYS:
            if key in obj and isinstance(obj[key], str):
                uuids.add(obj[key])
        for key, value in obj.items():
            # Some keys are also UUIDs and should be re-mapped.
            if isinstance(key, str) and re.search(UUID_PATTERN, key):
                uuids.add(key)
            # Some values may be JSON strings that need to be parsed.
            if _is_json(value):
                json_value = json.loads(value)
                _extract_uuids_from_obj(json_value, uuids)
            else:
                _extract_uuids_from_obj(value, uuids)
    elif isinstance(obj, list):
        for item in obj:
            _extract_uuids_from_obj(item, uuids)


def _search_for_uuids(objs: list[Any]) -> set[str]:
    """Search for all UUIDs in a list of objects.

    Args:
        objs: List of objects to search for UUIDs.

    Returns:
        A set of all UUIDs found in the objects.
    """
    uuids: set[str] = set()
    for obj in objs:
        _extract_uuids_from_obj(obj, uuids)
    return uuids


def _split_prefix(old_uuid: str) -> tuple[str, str]:
    """Split a UUID string into prefix and UUID parts.

    Some UUIDs may have prefixes (e.g., "prefix-12345678-1234-1234-1234-123456789abc").
    This function separates the prefix from the actual UUID and maintains the prefix.

    Args:
        old_uuid: The UUID string that may contain a prefix.

    Returns:
        A tuple of (prefix, uuid) where prefix is everything before the UUID pattern,
        and uuid is the UUID portion. If no UUID pattern is found, returns ("", old_uuid).
    """
    match = re.search(UUID_PATTERN, old_uuid)
    if match:
        return (old_uuid[: match.start()], old_uuid[match.start() :])
    return ("", old_uuid)


def _generate_new_uuid_mapping(old_ids: set[str]) -> dict[str, str]:
    """Generate a mapping from old UUIDs to new UUIDs.

    Creates new UUIDs for each old UUID while preserving any prefixes.
    For example, if old_id is "prefix-12345678-...", the new mapping will be
    "prefix-<new-uuid>" where the prefix is preserved.

    Args:
        old_ids: Set of old UUID strings to map.

    Returns:
        A dictionary mapping each old UUID to a new UUID, preserving prefixes.
    """
    mapping: dict[str, str] = {}
    for old_id in old_ids:
        (prefix, old_uuid) = _split_prefix(old_id)
        new_uuid = str(uuid.uuid4())
        mapping[old_id] = prefix + new_uuid
    return mapping


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
            elif _is_json(value):
                json_value = json.loads(value)
                new_obj[key] = json.dumps(_replace_uuids_in_obj(json_value, mapping), separators=(",", ":"))
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
    json_objs: list[Any] = [deepcopy(ConjureEncoder.do_encode(obj)) for obj in objs]

    old_ids = _search_for_uuids(json_objs)
    mapping: dict[str, str] = _generate_new_uuid_mapping(old_ids)
    logger.debug("Generated id mapping: ")
    for old_id, new_id in mapping.items():
        logger.debug("%s -> %s", old_id, new_id)

    new_json_objs: list[Any] = [_replace_uuids_in_obj(json_obj, mapping) for json_obj in json_objs]

    logger.debug("old vs new jsons:")
    for old_json, new_json in zip(json_objs, new_json_objs):
        logger.debug("OLD: %s", old_json)
        logger.debug("NEW: %s", new_json)

    return [cast(ConjureType, new_json_obj) for new_json_obj in new_json_objs]


@dataclass(frozen=True)
class Cloner:
    """Clones workbook templates from one workspace to another.

    This class provides functionality to clone workbook templates, replacing all
    UUIDs with new ones to ensure the cloned template is independent from the
    original. The cloning process preserves the structure, content, and layout
    of the template while generating new identifiers.

    Attributes:
        source_client: The NominalClient to read the source template from.
        target_client: The NominalClient to create the cloned template in.
    """

    source_client: NominalClient
    target_client: NominalClient

    def clone_workbook_template(self, template_rid: str, workspace_rid: str) -> WorkbookTemplate:
        """Clone a workbook template from the source to the target workspace.

        Retrieves the source template, clones its layout and content.  For safety, we replace
        all unique identifiers with new UUIDs. We then creates a new template in the target workspace.
        The cloned template maintains all metadata (title, description, labels, properties).

        Args:
            template_rid: The resource ID of the template to clone.
            workspace_rid: The resource ID of the target workspace.

        Returns:
            The newly created WorkbookTemplate in the target workspace.
        """
        raw_source_template: scout_template_api.Template = self.source_client._clients.template.get(
            self.source_client._clients.auth_header, template_rid
        )

        template_layout: scout_layout_api.WorkbookLayout = raw_source_template.layout
        template_content: scout_workbookcommon_api.WorkbookContent = raw_source_template.content
        [new_template_layout_generic, new_workbook_content_generic] = _clone_conjure_objects_with_new_uuids(
            [template_layout, template_content]
        )
        new_template_layout = cast(scout_layout_api.WorkbookLayout, new_template_layout_generic)
        new_workbook_content = cast(scout_workbookcommon_api.WorkbookContent, new_workbook_content_generic)

        return self._create_workbook_template_with_content_and_layout(
            title=raw_source_template.metadata.title,
            description=raw_source_template.metadata.description,
            labels=raw_source_template.metadata.labels,
            properties=raw_source_template.metadata.properties,
            layout=new_template_layout,
            content=new_workbook_content,
            commit_message="Cloned from template",
            workspace_rid=workspace_rid,
        )

    def _create_workbook_template_with_content_and_layout(
        self,
        title: str,
        description: str,
        labels: list[str],
        properties: dict[str, str],
        layout: scout_layout_api.WorkbookLayout,
        content: scout_workbookcommon_api.WorkbookContent,
        commit_message: str,
        workspace_rid: str,
    ) -> WorkbookTemplate:
        """Create a workbook template with specified content and layout.

        This is a helper method that constructs and creates a workbook template
        request with the provided parameters, including layout and content.  We are keeping this
        in the experimental folder for now because the layout and content properties should stay under
        tight control. The template is created in the target workspace and is not published by default.

        Args:
            title: The title of the template.
            description: The description of the template.
            labels: List of labels to apply to the template.
            properties: Dictionary of properties for the template.
            layout: The workbook layout to use.
            content: The workbook content to use.
            commit_message: The commit message for the template creation.
            workspace_rid: The resource ID of the workspace to create the template in.

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
            message=commit_message if commit_message is not None else "Initial blank workbook template",
            workspace=self.target_client._workspace_rid_for_search(workspace_rid),
        )

        template = self.target_client._clients.template.create(self.target_client._clients.auth_header, request)
        return WorkbookTemplate._from_conjure(self.target_client._clients, template)
