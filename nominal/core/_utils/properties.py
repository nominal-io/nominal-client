from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Callable, Iterator, Mapping, Sequence, TypeAlias, TypeVar, overload

from nominal_api import api

logger = logging.getLogger(__name__)

PropertyValue: TypeAlias = str | float
TypedProperties: TypeAlias = Mapping[str, PropertyValue]

_QueryT = TypeVar("_QueryT")

_SEARCH_PROPERTIES_DEPRECATION = (
    "Passing properties= to search_assets, search_runs, or search_datasets is deprecated. "
    "Use property_filters with eq() instead, for example: "
    "property_filters=[eq('serial', 'A1'), eq('mass_kg', 12.0)]."
)


class PropertyComparisonOperator(Enum):
    """Comparison operator for typed property filters, wrapping the conjure enum."""

    EQ = "EQ"
    NEQ = "NEQ"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"

    def _to_conjure(self) -> api.PropertyComparisonOperator:
        return _PROPERTY_COMPARISON_OPERATOR_TO_CONJURE[self]

    @classmethod
    def _from_conjure(cls, value: api.PropertyComparisonOperator) -> PropertyComparisonOperator:
        return _PROPERTY_COMPARISON_OPERATOR_FROM_CONJURE[value]


_PROPERTY_COMPARISON_OPERATOR_TO_CONJURE: Mapping[PropertyComparisonOperator, api.PropertyComparisonOperator] = {
    PropertyComparisonOperator.EQ: api.PropertyComparisonOperator.EQ,
    PropertyComparisonOperator.NEQ: api.PropertyComparisonOperator.NEQ,
    PropertyComparisonOperator.GT: api.PropertyComparisonOperator.GT,
    PropertyComparisonOperator.GTE: api.PropertyComparisonOperator.GTE,
    PropertyComparisonOperator.LT: api.PropertyComparisonOperator.LT,
    PropertyComparisonOperator.LTE: api.PropertyComparisonOperator.LTE,
}
_PROPERTY_COMPARISON_OPERATOR_FROM_CONJURE: Mapping[api.PropertyComparisonOperator, PropertyComparisonOperator] = {
    v: k for k, v in _PROPERTY_COMPARISON_OPERATOR_TO_CONJURE.items()
}


class NumericPropertyRangeOperator(Enum):
    """Range operator for typed numeric property filters, wrapping the conjure enum."""

    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT_BETWEEN"

    def _to_conjure(self) -> api.NumericPropertyRangeOperator:
        return _NUMERIC_PROPERTY_RANGE_OPERATOR_TO_CONJURE[self]

    @classmethod
    def _from_conjure(cls, value: api.NumericPropertyRangeOperator) -> NumericPropertyRangeOperator:
        return _NUMERIC_PROPERTY_RANGE_OPERATOR_FROM_CONJURE[value]


_NUMERIC_PROPERTY_RANGE_OPERATOR_TO_CONJURE: Mapping[NumericPropertyRangeOperator, api.NumericPropertyRangeOperator] = {
    NumericPropertyRangeOperator.BETWEEN: api.NumericPropertyRangeOperator.BETWEEN,
    NumericPropertyRangeOperator.NOT_BETWEEN: api.NumericPropertyRangeOperator.NOT_BETWEEN,
}
_NUMERIC_PROPERTY_RANGE_OPERATOR_FROM_CONJURE: Mapping[
    api.NumericPropertyRangeOperator, NumericPropertyRangeOperator
] = {v: k for k, v in _NUMERIC_PROPERTY_RANGE_OPERATOR_TO_CONJURE.items()}


def check_property_value(value: object, *, name: str) -> PropertyValue:
    if isinstance(value, (str, float)):
        return value
    raise TypeError(f"property {name!r} must be str or float, got {type(value).__name__}")


def _as_numeric_value(value: object, *, what: str) -> float:
    if isinstance(value, float):
        return value
    raise TypeError(f"{what} must be float, got {type(value).__name__}")


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


def properties_for_update(
    properties: TypedProperties | None,
) -> tuple[dict[str, str] | None, dict[str, api.TypedPropertyValue] | None]:
    """Return ``(legacy, typed)`` fields for an update request.

    ``None`` leaves both maps unchanged. A provided mapping writes typed values
    and clears the legacy string map.
    """
    if properties is None:
        return None, None
    return {}, typed_properties_to_conjure(properties)


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


def resource_properties_from_conjure(
    typed_properties: Mapping[str, api.TypedPropertyValue] | None,
    legacy_properties: Mapping[str, str] | None,
) -> TypedProperties:
    """Hydrate properties from the typed map, falling back to legacy string properties.

    When both maps contain the same key, the typed value wins.
    """
    result: dict[str, PropertyValue] = dict(legacy_properties or {})
    result.update(typed_properties_from_conjure(typed_properties))
    return MappingProxyType(result)


def string_properties_for_ingest(properties: TypedProperties | None) -> dict[str, str]:
    """Return string properties for ingest destinations that cannot accept typed/numeric values."""
    if not properties:
        return {}
    strings: dict[str, str] = {}
    for name, value in properties.items():
        if not isinstance(value, str):
            raise TypeError(
                "numeric properties are not supported when creating a dataset via ingest; "
                "set them via Dataset.update after ingest"
            )
        strings[name] = value
    return strings


@dataclass(frozen=True)
class StringEqualityFilter:
    """A string equality filter on a typed property.

    Construct via :func:`eq` rather than this class directly.
    """

    name: str
    value: str


@dataclass(frozen=True)
class NumericComparisonFilter:
    """A numeric comparison filter (eq/neq/gt/gte/lt/lte) on a typed property.

    Construct via factory functions such as :func:`gt` rather than this class directly.
    """

    name: str
    operator: PropertyComparisonOperator
    value: float

    def to_query_clause(self, query_cls: Callable[..., _QueryT]) -> _QueryT:
        return query_cls(
            numeric_property=api.NumericPropertyPredicate(
                name=self.name,
                operator=self.operator._to_conjure(),
                value=self.value,
            )
        )


@dataclass(frozen=True)
class NumericRangeFilter:
    """An inclusive numeric range filter (between/not_between) on a typed property.

    Construct via :func:`between` or :func:`not_between` rather than this class directly.
    Both bounds are inclusive.
    """

    name: str
    operator: NumericPropertyRangeOperator
    min_value: float
    max_value: float

    def to_query_clause(self, query_cls: Callable[..., _QueryT]) -> _QueryT:
        return query_cls(
            numeric_property_range=api.NumericPropertyRangePredicate(
                name=self.name,
                operator=self.operator._to_conjure(),
                min=self.min_value,
                max=self.max_value,
            )
        )


PropertyFilter: TypeAlias = StringEqualityFilter | NumericComparisonFilter | NumericRangeFilter


def iter_property_filter_clauses(
    properties: TypedProperties | None,
    property_filters: Sequence[PropertyFilter] | None,
    *,
    query_cls: Callable[..., _QueryT],
    string_clause: Callable[[str, str], _QueryT],
) -> Iterator[_QueryT]:
    filters: list[PropertyFilter] = []
    if properties is not None:
        warnings.warn(_SEARCH_PROPERTIES_DEPRECATION, DeprecationWarning, stacklevel=3)
        filters.extend(eq(name, value) for name, value in properties.items())
    if property_filters:
        filters.extend(property_filters)
    for filt in filters:
        if isinstance(filt, StringEqualityFilter):
            yield string_clause(filt.name, filt.value)
        else:
            yield filt.to_query_clause(query_cls)


def _comparison(name: str, operator: PropertyComparisonOperator, value: float) -> NumericComparisonFilter:
    return NumericComparisonFilter(
        name=name,
        operator=operator,
        value=_as_numeric_value(value, what="value"),
    )


def _range(
    name: str,
    operator: NumericPropertyRangeOperator,
    min_value: float,
    max_value: float,
) -> NumericRangeFilter:
    return NumericRangeFilter(
        name=name,
        operator=operator,
        min_value=_as_numeric_value(min_value, what="min_value"),
        max_value=_as_numeric_value(max_value, what="max_value"),
    )


def eq(name: str, value: str | float) -> StringEqualityFilter | NumericComparisonFilter:
    """Equality filter on a string or numeric property."""
    if isinstance(value, str):
        return StringEqualityFilter(name=name, value=value)
    return _comparison(name, PropertyComparisonOperator.EQ, value)


def neq(name: str, value: float) -> NumericComparisonFilter:
    """Inequality filter on a numeric property."""
    return _comparison(name, PropertyComparisonOperator.NEQ, value)


def gt(name: str, value: float) -> NumericComparisonFilter:
    """Greater-than filter on a numeric property."""
    return _comparison(name, PropertyComparisonOperator.GT, value)


def gte(name: str, value: float) -> NumericComparisonFilter:
    """Greater-than-or-equal filter on a numeric property."""
    return _comparison(name, PropertyComparisonOperator.GTE, value)


def lt(name: str, value: float) -> NumericComparisonFilter:
    """Less-than filter on a numeric property."""
    return _comparison(name, PropertyComparisonOperator.LT, value)


def lte(name: str, value: float) -> NumericComparisonFilter:
    """Less-than-or-equal filter on a numeric property."""
    return _comparison(name, PropertyComparisonOperator.LTE, value)


def between(name: str, min_value: float, max_value: float) -> NumericRangeFilter:
    """Inclusive range filter: ``min_value <= property <= max_value``."""
    return _range(name, NumericPropertyRangeOperator.BETWEEN, min_value, max_value)


def not_between(name: str, min_value: float, max_value: float) -> NumericRangeFilter:
    """Filter matching values outside the inclusive range ``[min_value, max_value]``."""
    return _range(name, NumericPropertyRangeOperator.NOT_BETWEEN, min_value, max_value)
