import json
import logging
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Dict,
    Iterable,
    Mapping,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)

import requests
from conjure_python_client import ConjureBeanType, ConjureEnumType, ConjureUnionType
from conjure_python_client._serde.decoder import ConjureDecoder
from conjure_python_client._serde.encoder import ConjureEncoder
from nominal_api import scout_checks_api, scout_layout_api, scout_workbookcommon_api

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
from nominal.core.checklist import Checklist
from nominal.core.filetype import FileTypes
from nominal.core.run import Run
from nominal.core.video import Video
from nominal.core.video_file import VideoFile
from nominal.core.workbook_template import _create_workbook_template_with_content_and_layout
from nominal.experimental.checklist_utils.checklist_utils import (
    _create_checklist_with_content,
    _to_create_checklist_entries,
    _to_unresolved_checklist_variables,
)
from nominal.experimental.dataset_utils import create_dataset_with_uuid
from nominal.experimental.id_utils.id_utils import UUID_KEYS, UUID_PATTERN
from nominal.experimental.migration.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.migration_resources import MigrationResources
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
)

logger = logging.getLogger(__name__)

ConjureType = Union[ConjureBeanType, ConjureUnionType, ConjureEnumType]


def _install_migration_file_logger(
    log_path: str | Path | None = None,
    *,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
    formatter: logging.Formatter | None = None,
    mode: str = "a",
) -> logging.FileHandler:
    """Install a file handler that only writes log records with extra={"to_file": True}.

    Args:
        log_path: File path to write filtered logs to. If None (or a directory), a timestamped
            file named "migration_utils_output_YYYY-MM-DD-HH-MM-SS.txt" is created.
        logger: Logger to attach the handler to. Defaults to the root logger.
        level: Minimum log level to write to the file.
        formatter: Optional formatter to apply to the file handler.
        mode: File open mode for the handler.

    Returns:
        The attached FileHandler instance.
    """
    if logger is None:
        logger = logging.getLogger()

    if log_path is None:
        log_path_obj = Path.cwd()
    else:
        log_path_obj = Path(log_path)

    if log_path_obj.is_dir():
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        log_path_obj = log_path_obj / f"migration_utils_output_{timestamp}.txt"

    handler = logging.FileHandler(log_path_obj, mode=mode, encoding="utf-8")
    handler.setLevel(level)
    if formatter is not None:
        handler.setFormatter(formatter)

    filter_obj = logging.Filter()

    def _filter(record: logging.LogRecord) -> bool:
        return bool(getattr(record, "to_file", False))

    filter_obj.filter = _filter  # type: ignore[method-assign]
    handler.addFilter(filter_obj)
    logger.addHandler(handler)
    return handler


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
                    new_obj[key] = json.dumps(
                        _replace_uuids_in_obj(parsed_value, mapping),
                        separators=(",", ":"),
                    )
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
def _clone_conjure_objects_with_new_uuids(
    objs: list[ConjureType],
) -> list[ConjureType]: ...


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
        "Cloning workbook template: %s (rid: %s)",
        source_template.title,
        source_template.rid,
        extra=log_extras,
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
    new_workbook_template = _create_workbook_template_with_content_and_layout(
        clients=destination_client._clients,
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
    logger.info(
        "WORKBOOK_TEMPLATE: Old RID: %s, New RID: %s",
        source_template.rid,
        new_workbook_template.rid,
        extra={"to_file": True},
    )
    return new_workbook_template


def copy_checklist_from(
    source_checklist: Checklist,
    destination_client: NominalClient,
    *,
    new_title: str | None = None,
    new_commit_message: str | None = None,
    new_assignee_rid: str | None = None,
    new_description: str | None = None,
    new_checks: list[scout_checks_api.CreateChecklistEntryRequest] | None = None,
    new_properties: dict[str, str] | None = None,
    new_labels: list[str] | None = None,
    new_checklist_variables: list[scout_checks_api.UnresolvedChecklistVariable] | None = None,
    new_is_published: bool | None = False,
) -> Checklist:
    log_extras = {"destination_client_workspace": destination_client._clients.workspace_rid}
    logger.debug("Copying checklist: %s", source_checklist.name, extra=log_extras)

    api_source_checklist = source_checklist._get_latest_api()

    new_checklist = _create_checklist_with_content(
        client=destination_client,
        commit_message=new_commit_message or api_source_checklist.commit.message,
        title=new_title or source_checklist.name,
        description=new_description or source_checklist.description,
        checks=new_checks or _to_create_checklist_entries(api_source_checklist.checks),
        properties=new_properties or api_source_checklist.metadata.properties,
        labels=new_labels or api_source_checklist.metadata.labels,
        checklist_variables=new_checklist_variables
        or _to_unresolved_checklist_variables(api_source_checklist.checklist_variables),
        is_published=new_is_published or api_source_checklist.metadata.is_published,
        workspace=destination_client.get_workspace(destination_client._clients.workspace_rid).rid,
    )

    logger.debug(
        "New checklist created %s: (rid: %s)",
        new_checklist.name,
        new_checklist.rid,
    )
    logger.info(
        "CHECKLIST: Old RID: %s, New RID: %s",
        source_checklist.rid,
        new_checklist.rid,
        extra={"to_file": True},
    )

    return new_checklist


def copy_video_file_to_video_dataset(
    source_video_file: VideoFile,
    destination_video_dataset: Video,
) -> VideoFile | None:
    """Copy a video dataset file from the source to the destination dataset.

    This method is specifically designed to handle video files, which may require special handling
    due to their size and streaming nature. It retrieves the video file from the source dataset,
    streams it, and uploads it to the destination dataset while maintaining all associated metadata.

    Args:
        source_video_file: The source VideoFile to copy. Must be a video file with S3 handle.
        destination_video_dataset: The Video dataset to create the copied file in.

    Returns:
        The dataset file in the new dataset.
    """
    log_extras = {"destination_client_workspace": destination_video_dataset._clients.workspace_rid}
    logger.debug("Copying video file: %s", source_video_file.name, extra=log_extras)

    (mcap_video_details, timestamp_options) = source_video_file._get_file_ingest_options()

    old_file_uri = source_video_file._clients.catalog.get_video_file_uri(
        source_video_file._clients.auth_header, source_video_file.rid
    ).uri

    response = requests.get(old_file_uri, stream=True)
    response.raise_for_status()

    file_name = source_video_file.name
    file_stem = Path(file_name).stem
    if timestamp_options is not None:
        new_file = destination_video_dataset.add_from_io(
            video=cast(BinaryIO, response.raw),
            name=file_stem,
            start=timestamp_options.starting_timestamp,
            description=source_video_file.description,
        )
        new_file.update(
            starting_timestamp=timestamp_options.starting_timestamp,
            ending_timestamp=timestamp_options.ending_timestamp,
        )
    elif mcap_video_details is not None:
        new_file = destination_video_dataset.add_mcap_from_io(
            mcap=cast(BinaryIO, response.raw),
            name=file_stem,
            topic=mcap_video_details.mcap_channel_locator_topic,
            description=source_video_file.description,
            file_type=FileTypes.MCAP,
        )
    else:
        raise ValueError(
            "Unsupported video file ingest options for copying video file. "
            "Expected either _mcap_video_details or _timestamp_options to be set."
        )
    logger.debug(
        "New video file created %s in video dataset: %s (rid: %s)",
        new_file.name,
        destination_video_dataset.name,
        destination_video_dataset.rid,
    )
    logger.info(
        "VIDEO_FILE: Old RID: %s, New RID: %s",
        source_video_file.rid,
        new_file.rid,
        extra={"to_file": True},
    )
    return new_file


def clone_video(source_video: Video, destination_client: NominalClient) -> Video:
    """Clones a video, maintaining all properties and files.

    Args:
        source_video (Video): The video to copy from.
        destination_client (NominalClient): The destination client.

    Returns:
        The cloned video.
    """
    return copy_video_from(
        source_video=source_video,
        destination_client=destination_client,
        include_files=True,
    )


def copy_video_from(
    source_video: Video,
    destination_client: NominalClient,
    *,
    new_video_name: str | None = None,
    new_video_description: str | None = None,
    new_video_properties: dict[str, Any] | None = None,
    new_video_labels: Sequence[str] | None = None,
    include_files: bool = False,
) -> Video:
    """Copy a video from the source to the destination client.

    Args:
        source_video: The source Video to copy.
        destination_client: The NominalClient to create the copied video in.
        new_video_name: Optional new name for the copied video. If not provided, the original name is used.
        new_video_description: Optional new description for the copied video.
            If not provided, the original description is used.
        new_video_properties: Optional new properties for the copied video. If not provided, the original
            properties are used.
        new_video_labels: Optional new labels for the copied video. If not provided, the original labels are used.
        include_files: Whether to include files in the copied video.

    Returns:
        The newly created Video in the destination client.
    """
    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid
    }
    logger.debug(
        "Copying dataset %s (rid: %s)",
        source_video.name,
        source_video.rid,
        extra=log_extras,
    )
    new_video = destination_client.create_video(
        name=new_video_name if new_video_name is not None else source_video.name,
        description=new_video_description if new_video_description is not None else source_video.description,
        properties=new_video_properties if new_video_properties is not None else source_video.properties,
        labels=new_video_labels if new_video_labels is not None else source_video.labels,
    )
    if include_files:
        for source_file in source_video.list_files():
            copy_video_file_to_video_dataset(source_file, new_video)
    logger.debug(
        "New video created: %s (rid: %s)",
        new_video.name,
        new_video.rid,
        extra=log_extras,
    )
    logger.info(
        "VIDEO: Old RID: %s, New RID: %s",
        source_video.rid,
        new_video.rid,
        extra={"to_file": True},
    )
    return new_video


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
        logger.info(
            "DATASET_FILE: Old RID: %s, New RID: %s",
            source_file.id,
            new_file.id,
            extra={"to_file": True},
        )
        return new_file
    else:  # Because these fields are optional, need to check for None. We shouldn't ever run into this.
        raise ValueError("Unsupported file handle type or missing timestamp information.")


def clone_dataset(source_dataset: Dataset, destination_client: NominalClient) -> Dataset:
    """Clones a dataset, maintaining all properties, files, and channels.

    Args:
        source_dataset (Dataset): The dataset to copy from.
        destination_client (NominalClient): The destination client.

    Returns:
        The cloned dataset.
    """
    return copy_dataset_from(
        source_dataset=source_dataset,
        destination_client=destination_client,
        include_files=True,
    )


def copy_dataset_from(
    source_dataset: Dataset,
    destination_client: NominalClient,
    *,
    new_dataset_name: str | None = None,
    new_dataset_description: str | None = None,
    new_dataset_properties: dict[str, Any] | None = None,
    new_dataset_labels: Sequence[str] | None = None,
    include_files: bool = False,
    preserve_uuid: bool = False,
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
        preserve_uuid: If True, create the dataset with the same UUID as the source dataset.
            This is useful for migrations where references to datasets must be preserved.
            Throws a conflict error if a dataset with the UUID already exists.

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

    dataset_name = new_dataset_name if new_dataset_name is not None else source_dataset.name
    dataset_description = new_dataset_description if new_dataset_description is not None else source_dataset.description
    dataset_properties = new_dataset_properties if new_dataset_properties is not None else source_dataset.properties
    dataset_labels = new_dataset_labels if new_dataset_labels is not None else source_dataset.labels

    if preserve_uuid:
        # Extract the UUID from the source dataset's rid
        match = UUID_PATTERN.search(source_dataset.rid)
        if not match:
            raise ValueError(f"Could not extract UUID from dataset rid: {source_dataset.rid}")
        source_uuid = match.group(2)
        new_dataset = create_dataset_with_uuid(
            client=destination_client,
            dataset_uuid=source_uuid,
            name=dataset_name,
            description=dataset_description,
            labels=dataset_labels,
            properties=dataset_properties,
        )
    else:
        new_dataset = destination_client.create_dataset(
            name=dataset_name,
            description=dataset_description,
            properties=dataset_properties,
            labels=dataset_labels,
        )

    if preserve_uuid:
        channels_copied_count = 0
        for source_channel in source_dataset.search_channels():
            if source_channel.data_type is None:
                logger.warning("Skipping channel %s: unknown data type", source_channel.name, extra=log_extras)
                continue
            new_dataset.add_channel(
                name=source_channel.name,
                data_type=source_channel.data_type,
                description=source_channel.description,
                unit=source_channel.unit,
            )
            channels_copied_count += 1
        logger.info("Copied %d channels from dataset %s", channels_copied_count, source_dataset.name, extra=log_extras)
    if include_files:
        for source_file in source_dataset.list_files():
            copy_file_to_dataset(source_file, new_dataset)

    # Copy bounds from source dataset if they exist
    if source_dataset.bounds is not None:
        new_dataset = new_dataset.update_bounds(
            start=source_dataset.bounds.start,
            end=source_dataset.bounds.end,
        )

    logger.debug(
        "New dataset created: %s (rid: %s)",
        new_dataset.name,
        new_dataset.rid,
        extra=log_extras,
    )
    logger.info(
        "DATASET: Old RID: %s, New RID: %s",
        source_dataset.rid,
        new_dataset.rid,
        extra={"to_file": True},
    )
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
    logger.debug(
        "New event created: %s (rid: %s)",
        new_event.name,
        new_event.rid,
        extra=log_extras,
    )
    logger.info(
        "EVENT: Old RID: %s, New RID: %s",
        source_event.rid,
        new_event.rid,
        extra={"to_file": True},
    )
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
    logger.info(
        "RUN: Old RID: %s, New RID: %s",
        source_run.rid,
        new_run.rid,
        extra={"to_file": True},
    )
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
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=True),
        include_events=True,
        include_runs=True,
        include_video=True,
    )


def _copy_asset_events(
    source_asset: Asset,
    destination_client: NominalClient,
    new_asset: Asset,
) -> None:
    source_events = source_asset.search_events(origin_types=SearchEventOriginType.get_manual_origin_types())
    for source_event in source_events:
        copy_event_from(source_event, destination_client, new_assets=[new_asset])


def _copy_asset_runs(
    source_asset: Asset,
    destination_client: NominalClient,
    new_asset: Asset,
) -> Dict[str, str]:
    run_mapping: Dict[str, str] = {}
    source_runs = source_asset.list_runs()
    for source_run in source_runs:
        new_run = copy_run_from(source_run, destination_client, new_assets=[new_asset])
        run_mapping[source_run.rid] = new_run.rid
    return run_mapping


def _copy_asset_checklists(
    source_asset: Asset,
    destination_client: NominalClient,
    run_mapping: Dict[str, str],
) -> None:
    source_checklist_rid_to_destination_checklist_map: Dict[str, Checklist] = {}
    for source_data_review in source_asset.search_data_reviews():
        source_checklist = source_data_review.get_checklist()
        logger.debug("Found Data Review %s", source_checklist.rid)
        if source_checklist.rid not in source_checklist_rid_to_destination_checklist_map:
            destination_checklist = copy_checklist_from(source_checklist, destination_client)
            source_checklist_rid_to_destination_checklist_map[source_checklist.rid] = destination_checklist
        else:
            destination_checklist = source_checklist_rid_to_destination_checklist_map[source_checklist.rid]
        destination_checklist.execute(run_mapping[source_data_review.run_rid])


def _copy_asset_videos(
    source_asset: Asset,
    destination_client: NominalClient,
    new_asset: Asset,
) -> None:
    for data_scope, video_dataset in source_asset.list_videos():
        new_video_dataset = destination_client.create_video(
            name=video_dataset.name,
            description=video_dataset.description,
            properties=video_dataset.properties,
            labels=video_dataset.labels,
        )
        new_asset.add_video(data_scope, new_video_dataset)
        for source_video_file in video_dataset.list_files():
            copy_video_file_to_video_dataset(source_video_file, new_video_dataset)


def copy_asset_from(
    source_asset: Asset,
    destination_client: NominalClient,
    *,
    new_asset_name: str | None = None,
    new_asset_description: str | None = None,
    new_asset_properties: dict[str, Any] | None = None,
    new_asset_labels: Sequence[str] | None = None,
    dataset_config: MigrationDatasetConfig | None = None,
    old_to_new_dataset_rid_mapping: dict[str, str] = {},
    include_events: bool = False,
    include_runs: bool = False,
    include_video: bool = False,
    include_checklists: bool = False,
) -> Asset:
    """Copy an asset from the source to the destination client.

    Args:
        source_asset: The source Asset to copy.
        destination_client: The NominalClient to create the copied asset in.
        new_asset_name: Optional new name for the copied asset. If not provided, the original name is used.
        new_asset_description: Optional new description for the copied asset. If not provided, original description used
        new_asset_properties: Optional new properties for the copied asset. If not provided, original properties used.
        new_asset_labels: Optional new labels for the copied asset. If not provided, the original labels are used.
        dataset_config: Configuration for dataset migration.
        old_to_new_dataset_rid_mapping: Mapping of old dataset RIDs to new dataset RIDs to avoid duplicate copies.
        include_events: Whether to include events in the copied dataset.
        include_runs: Whether to include runs in the copied asset.
        include_video: Whether to include video in the copied asset.
        include_checklists: Whether to include and execute checklists in the copied asset.

    Returns:
        The new asset created.
    """
    if include_checklists and not include_runs:
        raise ValueError("include_checklists set to True requires include_runs to be set to True.")

    log_extras = {
        "destination_client_workspace": destination_client.get_workspace(destination_client._clients.workspace_rid).rid
    }

    logger.debug(
        "Copying asset %s (rid: %s)",
        source_asset.name,
        source_asset.rid,
        extra=log_extras,
    )
    new_asset = destination_client.create_asset(
        name=new_asset_name if new_asset_name is not None else source_asset.name,
        description=new_asset_description if new_asset_description is not None else source_asset.description,
        properties=new_asset_properties if new_asset_properties is not None else source_asset.properties,
        labels=new_asset_labels if new_asset_labels is not None else source_asset.labels,
    )

    if dataset_config is not None:
        source_datasets = source_asset.list_datasets()
        for data_scope, source_dataset in source_datasets:
            if source_dataset.rid in old_to_new_dataset_rid_mapping.keys():
                new_dataset_rid = old_to_new_dataset_rid_mapping[source_dataset.rid]
                new_dataset = destination_client.get_dataset(new_dataset_rid)
            else:
                new_dataset = copy_dataset_from(
                    source_dataset=source_dataset,
                    destination_client=destination_client,
                    preserve_uuid=dataset_config.preserve_dataset_uuid,
                    include_files=dataset_config.include_dataset_files,
                )
            old_to_new_dataset_rid_mapping[source_dataset.rid] = new_dataset.rid
            new_asset.add_dataset(data_scope, new_dataset)

    run_mapping: Dict[str, str] = {}

    if include_events:
        _copy_asset_events(source_asset, destination_client, new_asset)

    if include_runs:
        run_mapping = _copy_asset_runs(source_asset, destination_client, new_asset)

    if include_checklists:
        _copy_asset_checklists(source_asset, destination_client, run_mapping)

    if include_video:
        _copy_asset_videos(source_asset, destination_client, new_asset)

    logger.debug("New asset created: %s (rid: %s)", new_asset, new_asset.rid, extra=log_extras)
    logger.info(
        "ASSET: Old RID: %s, New RID: %s",
        source_asset.rid,
        new_asset.rid,
        extra={"to_file": True},
    )
    return new_asset


def copy_resources_to_destination_client(
    destination_client: NominalClient,
    migration_resources: MigrationResources,
    dataset_config: MigrationDatasetConfig | None = None,
) -> tuple[Sequence[tuple[str, Dataset]], Sequence[Asset], Sequence[WorkbookTemplate], Sequence[Workbook]]:
    """Based on a list of assets and workbook templates, copy resources to destination client, creating
       new datasets, datafiles, and workbooks along the way. Standalone templates are cloned without
       creating workbooks.

    Args:
        destination_client (NominalClient): client of the tenant/workspace to copy resources to.
        migration_resources (MigrationResources): resources to copy.
        dataset_config (MigrationDataConfig | None): Configuration for dataset migration.

    Returns:
        All of the created resources.
    """
    file_handler = _install_migration_file_logger()
    try:
        log_extras = {
            "destination_client_workspace": destination_client.get_workspace(
                destination_client._clients.workspace_rid
            ).rid,
        }

        new_assets = []
        new_templates = []
        new_workbooks = []

        new_data_scopes_and_datasets: list[tuple[str, Dataset]] = []
        old_to_new_dataset_rid_mapping: dict[str, str] = {}
        for asset_resources in migration_resources.source_assets.values():
            source_asset = asset_resources.asset
            new_asset = copy_asset_from(
                source_asset,
                destination_client,
                dataset_config=dataset_config,
                old_to_new_dataset_rid_mapping=old_to_new_dataset_rid_mapping,
                include_events=True,
                include_runs=True,
                include_video=True,
                include_checklists=True,
            )
            new_assets.append(new_asset)
            new_data_scopes_and_datasets.extend(new_asset.list_datasets())

            for source_workbook_template in asset_resources.source_workbook_templates:
                new_template = clone_workbook_template(source_workbook_template, destination_client)
                new_templates.append(new_template)
                new_workbook = new_template.create_workbook(
                    title=new_template.title, description=new_template.description, asset=new_asset
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

        for source_template in migration_resources.source_standalone_templates:
            new_template = clone_workbook_template(source_template, destination_client)
            new_templates.append(new_template)
    finally:
        file_handler.close()
        logger.removeHandler(file_handler)
    return (new_data_scopes_and_datasets, new_assets, new_templates, new_workbooks)
