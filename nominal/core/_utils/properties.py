from __future__ import annotations

import logging
from types import MappingProxyType
from typing import Mapping, overload

from nominal_api import api

from nominal.core._utils.api_types import NominalProperties, PropertyValue
from nominal.protos.types import types_pb2

logger = logging.getLogger(__name__)


def check_property_value(value: object, *, name: str) -> PropertyValue:
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise TypeError(f"property {name!r} must be str, int, or float, got {type(value).__name__}")


def typed_property_value_to_conjure(value: object, *, name: str) -> api.TypedPropertyValue:
    checked = check_property_value(value, name=name)
    if isinstance(checked, str):
        return api.TypedPropertyValue(string_value=checked)
    return api.TypedPropertyValue(numeric_value=checked)


@overload
def typed_properties_to_conjure(properties: None) -> None: ...
@overload
def typed_properties_to_conjure(properties: NominalProperties) -> dict[str, api.TypedPropertyValue]: ...
@overload
def typed_properties_to_conjure(properties: NominalProperties | None) -> dict[str, api.TypedPropertyValue] | None: ...
def typed_properties_to_conjure(
    properties: NominalProperties | None,
) -> dict[str, api.TypedPropertyValue] | None:
    if properties is None:
        return None
    return {name: typed_property_value_to_conjure(value, name=name) for name, value in properties.items()}


def typed_properties_from_conjure(
    typed_properties: Mapping[str, api.TypedPropertyValue] | None,
) -> dict[str, PropertyValue]:
    if not typed_properties:
        return {}
    result: dict[str, PropertyValue] = {}
    for name, value in typed_properties.items():
        if value.type == "numericValue":
            if value.numeric_value is None:
                raise ValueError(f"typed property {name!r} has numericValue with no value")
            result[name] = value.numeric_value
        elif value.type == "stringValue":
            if value.string_value is None:
                raise ValueError(f"typed property {name!r} has stringValue with no value")
            result[name] = value.string_value
        else:
            logger.warning("Skipping typed property %r with unknown type %r", name, value.type)
    return result


def properties_from_conjure(
    typed_properties: Mapping[str, api.TypedPropertyValue] | None,
) -> NominalProperties:
    return MappingProxyType(typed_properties_from_conjure(typed_properties))


def typed_property_value_to_proto(value: object, *, name: str) -> types_pb2.TypedPropertyValue:
    checked = check_property_value(value, name=name)
    if isinstance(checked, str):
        return types_pb2.TypedPropertyValue(string_value=checked)
    return types_pb2.TypedPropertyValue(numeric_value=checked)


@overload
def typed_properties_to_proto(properties: None) -> None: ...
@overload
def typed_properties_to_proto(properties: NominalProperties) -> dict[str, types_pb2.TypedPropertyValue]: ...
@overload
def typed_properties_to_proto(
    properties: NominalProperties | None,
) -> dict[str, types_pb2.TypedPropertyValue] | None: ...
def typed_properties_to_proto(
    properties: NominalProperties | None,
) -> dict[str, types_pb2.TypedPropertyValue] | None:
    if properties is None:
        return None
    return {name: typed_property_value_to_proto(value, name=name) for name, value in properties.items()}


def properties_from_proto(typed_properties: Mapping[str, types_pb2.TypedPropertyValue]) -> NominalProperties:
    result: dict[str, PropertyValue] = {}
    for name, value in typed_properties.items():
        value_type = value.WhichOneof("typed_property_value")
        if value_type == "numeric_value":
            result[name] = value.numeric_value
        elif value_type == "string_value":
            result[name] = value.string_value
        else:
            logger.warning("Skipping typed property %r with unknown type %r", name, value_type)
    return MappingProxyType(result)
