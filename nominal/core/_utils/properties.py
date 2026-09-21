from __future__ import annotations

import logging
import warnings
from types import MappingProxyType
from typing import Callable, Iterator, Mapping, Sequence, TypeVar, overload

from nominal_api import api

from nominal.core.exceptions import SearchPropertiesDeprecationWarning
from nominal.core.properties import (
    PropertyFilter,
    PropertyValue,
    TypedProperties,
    _as_numeric_value,
    _NumericComparisonFilter,
    _NumericRangeFilter,
    _StringInFilter,
)
from nominal.protos.types import types_pb2

logger = logging.getLogger(__name__)

_QueryT = TypeVar("_QueryT")

_SEARCH_PROPERTIES_DEPRECATION = (
    "Passing properties= to search_assets, search_runs, or search_datasets is deprecated. "
    "Use property_filters with PropertyFilter.eq() instead, for example: "
    "property_filters=[PropertyFilter.eq('serial', 'A1'), PropertyFilter.eq('mass_kg', 12.0)]."
)


def warn_deprecated_search_properties(properties: TypedProperties | None) -> None:
    """Warn at the user call site. Call only from public ``search_*`` methods."""
    if properties is not None:
        warnings.warn(_SEARCH_PROPERTIES_DEPRECATION, SearchPropertiesDeprecationWarning, stacklevel=3)


def check_property_value(value: object, *, name: str) -> PropertyValue:
    if isinstance(value, str):
        return value
    try:
        return _as_numeric_value(value, what=f"property {name!r}")
    except TypeError as exc:
        raise TypeError(f"property {name!r} must be str, int, or float, got {type(value).__name__}") from exc


def typed_property_value_to_conjure(value: object, *, name: str) -> api.TypedPropertyValue:
    checked = check_property_value(value, name=name)
    if isinstance(checked, str):
        return api.TypedPropertyValue(string_value=checked)
    return api.TypedPropertyValue(numeric_value=checked)


@overload
def typed_properties_to_conjure(properties: None) -> None: ...
@overload
def typed_properties_to_conjure(properties: TypedProperties) -> dict[str, api.TypedPropertyValue]: ...
@overload
def typed_properties_to_conjure(properties: TypedProperties | None) -> dict[str, api.TypedPropertyValue] | None: ...
def typed_properties_to_conjure(
    properties: TypedProperties | None,
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
) -> TypedProperties:
    return MappingProxyType(typed_properties_from_conjure(typed_properties))


def typed_property_value_to_proto(value: object, *, name: str) -> types_pb2.TypedPropertyValue:
    checked = check_property_value(value, name=name)
    if isinstance(checked, str):
        return types_pb2.TypedPropertyValue(string_value=checked)
    return types_pb2.TypedPropertyValue(numeric_value=checked)


@overload
def typed_properties_to_proto(properties: None) -> None: ...
@overload
def typed_properties_to_proto(properties: TypedProperties) -> dict[str, types_pb2.TypedPropertyValue]: ...
@overload
def typed_properties_to_proto(
    properties: TypedProperties | None,
) -> dict[str, types_pb2.TypedPropertyValue] | None: ...
def typed_properties_to_proto(
    properties: TypedProperties | None,
) -> dict[str, types_pb2.TypedPropertyValue] | None:
    if properties is None:
        return None
    return {name: typed_property_value_to_proto(value, name=name) for name, value in properties.items()}


def typed_properties_from_proto(
    typed_properties: Mapping[str, types_pb2.TypedPropertyValue] | None,
) -> dict[str, PropertyValue]:
    if not typed_properties:
        return {}
    result: dict[str, PropertyValue] = {}
    for name, value in typed_properties.items():
        which = value.WhichOneof("typed_property_value")
        if which == "numeric_value":
            result[name] = value.numeric_value
        elif which == "string_value":
            result[name] = value.string_value
        else:
            logger.warning("Skipping typed property %r with unknown type %r", name, which)
    return result


def properties_from_proto(
    typed_properties: Mapping[str, types_pb2.TypedPropertyValue] | None,
) -> TypedProperties:
    return MappingProxyType(typed_properties_from_proto(typed_properties))


def iter_property_filter_clauses(
    properties: TypedProperties | None,
    property_filters: Sequence[PropertyFilter] | None,
    *,
    query_cls: Callable[..., _QueryT],
    string_in_clause: Callable[[str, Sequence[str]], _QueryT],
) -> Iterator[_QueryT]:
    filters: list[PropertyFilter] = []
    if properties is not None:
        filters.extend(PropertyFilter.eq(name, value) for name, value in properties.items())
    if property_filters:
        filters.extend(property_filters)
    for filt in filters:
        if isinstance(filt, _StringInFilter):
            yield filt.to_query_clause(query_cls, string_in_clause=string_in_clause)
        elif isinstance(filt, (_NumericComparisonFilter, _NumericRangeFilter)):
            yield filt.to_query_clause(query_cls)
        else:
            raise TypeError(f"unsupported property filter: {type(filt).__name__}")
