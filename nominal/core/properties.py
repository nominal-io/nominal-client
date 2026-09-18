"""Property helpers for assets, runs, datasets, and events.

Filters live here so they stay namespaced:

    from nominal.core import properties as props

    client.search_runs(
        property_filters=[
            props.eq("serial", "A1"),
            props.in_("site", ["pad-a", "pad-b"]),
            props.gt("mass_kg", 10.0),
            props.between("temp_c", 0.0, 100.0),
        ]
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Mapping, Sequence, TypeAlias, TypeVar

from nominal_api import api

PropertyValue: TypeAlias = str | float
"""A stored property value: ``str`` or ``float``.

``bool`` is rejected.
"""

TypedProperties: TypeAlias = Mapping[str, PropertyValue]
"""A mapping of property names to :data:`PropertyValue` values."""

_QueryT = TypeVar("_QueryT")


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


_PROPERTY_COMPARISON_OPERATOR_TO_CONJURE: Mapping[PropertyComparisonOperator, api.PropertyComparisonOperator] = {
    PropertyComparisonOperator.EQ: api.PropertyComparisonOperator.EQ,
    PropertyComparisonOperator.NEQ: api.PropertyComparisonOperator.NEQ,
    PropertyComparisonOperator.GT: api.PropertyComparisonOperator.GT,
    PropertyComparisonOperator.GTE: api.PropertyComparisonOperator.GTE,
    PropertyComparisonOperator.LT: api.PropertyComparisonOperator.LT,
    PropertyComparisonOperator.LTE: api.PropertyComparisonOperator.LTE,
}


class NumericPropertyRangeOperator(Enum):
    """Range operator for typed numeric property filters, wrapping the conjure enum."""

    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT_BETWEEN"

    def _to_conjure(self) -> api.NumericPropertyRangeOperator:
        return _NUMERIC_PROPERTY_RANGE_OPERATOR_TO_CONJURE[self]


_NUMERIC_PROPERTY_RANGE_OPERATOR_TO_CONJURE: Mapping[NumericPropertyRangeOperator, api.NumericPropertyRangeOperator] = {
    NumericPropertyRangeOperator.BETWEEN: api.NumericPropertyRangeOperator.BETWEEN,
    NumericPropertyRangeOperator.NOT_BETWEEN: api.NumericPropertyRangeOperator.NOT_BETWEEN,
}


def _as_numeric_value(value: object, *, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{what} must be int or float, got {type(value).__name__}")
    return float(value)


@dataclass(frozen=True)
class StringInFilter:
    """A string membership filter on a typed property.

    Construct via :func:`eq` or :func:`in_` rather than this class directly.
    ``eq`` is membership in a one-element list.
    """

    name: str
    values: tuple[str, ...]

    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT],
    ) -> _QueryT:
        del query_cls
        return string_in_clause(self.name, self.values)


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


PropertyFilter: TypeAlias = StringInFilter | NumericComparisonFilter | NumericRangeFilter


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


def eq(name: str, value: str | float) -> StringInFilter | NumericComparisonFilter:
    """Equality filter on a string or numeric property.

    Types are not coerced across string vs numeric: ``eq("x", "1")`` matches only
    string properties and ``eq("x", 1.0)`` matches only numeric properties.
    """
    if isinstance(value, str):
        return StringInFilter(name=name, values=(value,))
    return _comparison(name, PropertyComparisonOperator.EQ, value)


def in_(name: str, values: Sequence[str]) -> StringInFilter:
    """Match if the string property equals any of ``values``.

    ``values`` must be non-empty.
    """
    if not values:
        raise ValueError("in_() requires at least one value")
    return StringInFilter(name=name, values=tuple(values))


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


__all__ = [
    "NumericPropertyRangeOperator",
    "PropertyComparisonOperator",
    "PropertyFilter",
    "PropertyValue",
    "TypedProperties",
    "between",
    "eq",
    "gt",
    "gte",
    "in_",
    "lt",
    "lte",
    "neq",
    "not_between",
]
