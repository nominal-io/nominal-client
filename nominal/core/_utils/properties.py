from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, Mapping, TypeAlias

from nominal_api import api

PropertyValue: TypeAlias = str | int | float

_COMPARISON_OPERATORS: dict[str, api.PropertyComparisonOperator] = {
    "eq": api.PropertyComparisonOperator.EQ,
    "neq": api.PropertyComparisonOperator.NEQ,
    "gt": api.PropertyComparisonOperator.GT,
    "gte": api.PropertyComparisonOperator.GTE,
    "lt": api.PropertyComparisonOperator.LT,
    "lte": api.PropertyComparisonOperator.LTE,
}

_RANGE_OPERATORS: dict[str, api.NumericPropertyRangeOperator] = {
    "between": api.NumericPropertyRangeOperator.BETWEEN,
    "not_between": api.NumericPropertyRangeOperator.NOT_BETWEEN,
}


def check_property_value(value: object, *, name: str) -> PropertyValue:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise TypeError(f"property {name!r} must be str, int, or float, got {type(value).__name__}")
    return value


def _as_numeric_value(value: object, *, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{what} must be int or float, got {type(value).__name__}")
    return float(value)


def typed_property_value_to_conjure(value: object, *, name: str) -> api.TypedPropertyValue:
    checked = check_property_value(value, name=name)
    if isinstance(checked, str):
        return api.TypedPropertyValue(string_value=checked)
    return api.TypedPropertyValue(numeric_value=float(checked))


def typed_properties_to_conjure(
    properties: Mapping[str, PropertyValue] | None,
) -> dict[str, api.TypedPropertyValue]:
    if not properties:
        return {}
    return {name: typed_property_value_to_conjure(value, name=name) for name, value in properties.items()}


def typed_properties_from_conjure(
    typed_properties: Mapping[str, api.TypedPropertyValue] | None,
) -> Mapping[str, str | float]:
    if not typed_properties:
        return MappingProxyType({})
    result: dict[str, str | float] = {}
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
            raise ValueError(f"typed property {name!r} has unknown type {value.type!r}")
    return MappingProxyType(result)


def string_properties_for_ingest(properties: Mapping[str, PropertyValue] | None) -> dict[str, str]:
    """Return string properties for ingest destinations that cannot accept typed/numeric values."""
    if not properties:
        return {}
    strings: dict[str, str] = {}
    for name, value in properties.items():
        if isinstance(value, bool) or not isinstance(value, str):
            raise TypeError(
                "numeric properties are not supported when creating a dataset via ingest; "
                "set them via Dataset.update after ingest"
            )
        strings[name] = value
    return strings


@dataclass(frozen=True)
class NumericPropertyFilter:
    """A numeric comparison or inclusive range filter on a typed property."""

    name: str
    operator: Literal["eq", "neq", "gt", "gte", "lt", "lte", "between", "not_between"]
    value: float | None = None
    min_value: float | None = None
    max_value: float | None = None

    def is_range(self) -> bool:
        return self.operator in _RANGE_OPERATORS

    def to_comparison_predicate(self) -> api.NumericPropertyPredicate:
        if self.is_range() or self.value is None:
            raise ValueError(f"{self.operator} is not a comparison operator")
        return api.NumericPropertyPredicate(
            name=self.name,
            operator=_COMPARISON_OPERATORS[self.operator],
            value=self.value,
        )

    def to_range_predicate(self) -> api.NumericPropertyRangePredicate:
        if not self.is_range():
            raise ValueError(f"{self.operator} is not a range operator")
        return api.NumericPropertyRangePredicate(
            name=self.name,
            operator=_RANGE_OPERATORS[self.operator],
            min=self.min_value,
            max=self.max_value,
        )


def eq(name: str, value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(name=name, operator="eq", value=_as_numeric_value(value, what="value"))


def neq(name: str, value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(name=name, operator="neq", value=_as_numeric_value(value, what="value"))


def gt(name: str, value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(name=name, operator="gt", value=_as_numeric_value(value, what="value"))


def gte(name: str, value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(name=name, operator="gte", value=_as_numeric_value(value, what="value"))


def lt(name: str, value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(name=name, operator="lt", value=_as_numeric_value(value, what="value"))


def lte(name: str, value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(name=name, operator="lte", value=_as_numeric_value(value, what="value"))


def between(name: str, min_value: int | float, max_value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(
        name=name,
        operator="between",
        min_value=_as_numeric_value(min_value, what="min_value"),
        max_value=_as_numeric_value(max_value, what="max_value"),
    )


def not_between(name: str, min_value: int | float, max_value: int | float) -> NumericPropertyFilter:
    return NumericPropertyFilter(
        name=name,
        operator="not_between",
        min_value=_as_numeric_value(min_value, what="min_value"),
        max_value=_as_numeric_value(max_value, what="max_value"),
    )
