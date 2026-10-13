from __future__ import annotations

import logging
from types import MappingProxyType
from typing import Mapping

from nominal_api import api

from nominal.core._utils.api_types import NominalProperties, PropertyValue
from nominal.protos.types import types_pb2

logger = logging.getLogger(__name__)


def check_property_value(value: object, *, name: str) -> PropertyValue:
    match value:
        case str():
            return value
        # bool subclasses int, so it would otherwise be stored as 1.0 or 0.0.
        case int() | float() if not isinstance(value, bool):
            return float(value)
        case _:
            raise TypeError(f"property {name!r} must be str, int, or float, got {type(value).__name__}")


def typed_properties_to_conjure(properties: NominalProperties | None) -> dict[str, api.TypedPropertyValue] | None:
    if properties is None:
        return None
    result: dict[str, api.TypedPropertyValue] = {}
    for name, value in properties.items():
        match check_property_value(value, name=name):
            case str() as text:
                result[name] = api.TypedPropertyValue(string_value=text)
            case float() as number:
                result[name] = api.TypedPropertyValue(numeric_value=number)
    return result


def properties_from_conjure(typed_properties: Mapping[str, api.TypedPropertyValue]) -> NominalProperties:
    result: dict[str, PropertyValue] = {}
    for name, value in typed_properties.items():
        match value:
            case api.TypedPropertyValue(numeric_value=float() as number):
                result[name] = number
            case api.TypedPropertyValue(string_value=str() as text):
                result[name] = text
            case _:
                logger.warning("Skipping typed property %r with unknown type %r", name, value.type)
    return MappingProxyType(result)


def typed_properties_to_proto(properties: NominalProperties | None) -> dict[str, types_pb2.TypedPropertyValue] | None:
    if properties is None:
        return None
    result: dict[str, types_pb2.TypedPropertyValue] = {}
    for name, value in properties.items():
        match check_property_value(value, name=name):
            case str() as text:
                result[name] = types_pb2.TypedPropertyValue(string_value=text)
            case float() as number:
                result[name] = types_pb2.TypedPropertyValue(numeric_value=number)
    return result


def properties_from_proto(typed_properties: Mapping[str, types_pb2.TypedPropertyValue]) -> NominalProperties:
    result: dict[str, PropertyValue] = {}
    for name, value in typed_properties.items():
        match value.WhichOneof("typed_property_value"):
            case "numeric_value":
                result[name] = value.numeric_value
            case "string_value":
                result[name] = value.string_value
            case unknown:
                logger.warning("Skipping typed property %r with unknown type %r", name, unknown)
    return MappingProxyType(result)
