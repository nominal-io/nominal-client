from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any, TypeVar, Union, overload

from conjure_python_client import ConjureBeanType, ConjureEnumType, ConjureUnionType
from conjure_python_client._serde.decoder import ConjureDecoder
from conjure_python_client._serde.encoder import ConjureEncoder

from nominal.experimental.id_utils.id_utils import UUID_KEYS, UUID_PATTERN

logger = logging.getLogger(__name__)

ConjureType = Union[ConjureBeanType, ConjureUnionType, ConjureEnumType]
T1 = TypeVar("T1", bound=ConjureType)
T2 = TypeVar("T2", bound=ConjureType)


def _convert_if_json(s: str) -> tuple[Any, bool]:
    try:
        return (json.loads(s), True)
    except (ValueError, TypeError):
        return (s, False)


def _check_and_add_uuid_to_mapping(input_str: str, mapping: dict[str, str]) -> None:
    match = UUID_PATTERN.search(input_str)
    if match and input_str not in mapping:
        mapping[input_str] = f"{match.group(1)}{str(uuid.uuid4())}"
        logger.debug("Found UUID and added to mapping: %s -> %s", input_str, mapping[input_str])


def _extract_uuids_from_obj(obj: Any, mapping: dict[str, str]) -> None:
    # TODO (Sean): Refactor to remove expensive recursion strategy.
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in UUID_KEYS and isinstance(value, str):
                _check_and_add_uuid_to_mapping(value, mapping)
            else:
                _check_and_add_uuid_to_mapping(key, mapping)
            _extract_uuids_from_obj(_convert_if_json(value)[0] if isinstance(value, str) else value, mapping)
    elif isinstance(obj, list):
        for item in obj:
            _extract_uuids_from_obj(item, mapping)


def _generate_uuid_mapping(objs: list[Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for obj in objs:
        _extract_uuids_from_obj(obj, mapping)
    return mapping


def _replace_uuids_in_obj(obj: Any, mapping: dict[str, str]) -> Any:
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
    if isinstance(obj, list):
        return [_replace_uuids_in_obj(item, mapping) for item in obj]
    return obj


@overload
def clone_conjure_objects_with_new_uuids(
    objs: tuple[T1, T2],
) -> tuple[T1, T2]: ...


@overload
def clone_conjure_objects_with_new_uuids(
    objs: list[ConjureType],
) -> list[ConjureType]: ...


def clone_conjure_objects_with_new_uuids(
    objs: tuple[ConjureType, ...] | list[ConjureType],
) -> tuple[ConjureType, ...] | list[ConjureType]:
    original_types = [type(obj) for obj in objs]
    json_objs = [ConjureEncoder.do_encode(obj) for obj in objs]
    mapping = _generate_uuid_mapping(json_objs)
    new_json_objs = [_replace_uuids_in_obj(json_obj, mapping) for json_obj in json_objs]

    decoder = ConjureDecoder()
    result = [
        decoder.do_decode(new_json_obj, obj_type) for new_json_obj, obj_type in zip(new_json_objs, original_types)
    ]
    return tuple(result) if isinstance(objs, tuple) else result


@overload
def clone_conjure_objects_with_rid_overrides(
    objs: tuple[T1, T2],
    rid_overrides: dict[str, str],
) -> tuple[T1, T2]: ...


@overload
def clone_conjure_objects_with_rid_overrides(
    objs: list[ConjureType],
    rid_overrides: dict[str, str],
) -> list[ConjureType]: ...


def clone_conjure_objects_with_rid_overrides(
    objs: tuple[ConjureType, ...] | list[ConjureType],
    rid_overrides: dict[str, str],
) -> tuple[ConjureType, ...] | list[ConjureType]:
    """Clone conjure objects with fresh internal UUIDs, applying explicit RID substitutions.

    Behaves like clone_conjure_objects_with_new_uuids but merges rid_overrides into the UUID
    mapping before replacement. This replaces specific RIDs (e.g. source asset/run RIDs) with
    their known destination counterparts, while all other internal UUIDs are regenerated as usual.

    rid_overrides entries take precedence: any auto-generated mapping for an override key is
    removed before the overrides are merged in, ensuring the explicit value is always used.
    """
    original_types = [type(obj) for obj in objs]
    json_objs = [ConjureEncoder.do_encode(obj) for obj in objs]
    mapping = _generate_uuid_mapping(json_objs)
    for rid in rid_overrides:
        mapping.pop(rid, None)
    mapping.update(rid_overrides)
    new_json_objs = [_replace_uuids_in_obj(json_obj, mapping) for json_obj in json_objs]

    decoder = ConjureDecoder()
    result = [
        decoder.do_decode(new_json_obj, obj_type) for new_json_obj, obj_type in zip(new_json_objs, original_types)
    ]
    return tuple(result) if isinstance(objs, tuple) else result
