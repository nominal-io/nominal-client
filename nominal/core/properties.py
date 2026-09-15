"""Property helpers for assets, runs, and datasets.

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
TypedProperties: TypeAlias = Mapping[str, PropertyValue]

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


def _as_numeric_value(value: object, *, what: str) -> float:
    if isinstance(value, float):
        return value
    raise TypeError(f"{what} must be float, got {type(value).__name__}")


@dataclass(frozen=True)
class StringEqualityFilter:
    """A string equality filter on a typed property.

    Construct via :func:`eq` rather than this class directly.
    """

    name: str
    value: str

    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_clause: Callable[[str, str], _QueryT] | None = None,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT] | None = None,
    ) -> _QueryT:
        del query_cls, string_in_clause
        if string_clause is None:
            raise TypeError("string_clause is required")
        return string_clause(self.name, self.value)


@dataclass(frozen=True)
class StringInFilter:
    """A string membership filter on a typed property.

    Construct via :func:`in_` rather than this class directly.
    """

    name: str
    values: tuple[str, ...]

    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_clause: Callable[[str, str], _QueryT] | None = None,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT] | None = None,
    ) -> _QueryT:
        del query_cls, string_clause
        if string_in_clause is None:
            raise TypeError("string_in_clause is required")
        return string_in_clause(self.name, self.values)


@dataclass(frozen=True)
class NumericComparisonFilter:
    """A numeric comparison filter (eq/neq/gt/gte/lt/lte) on a typed property.

    Construct via factory functions such as :func:`gt` rather than this class directly.
    """

    name: str
    operator: PropertyComparisonOperator
    value: float

    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_clause: Callable[[str, str], _QueryT] | None = None,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT] | None = None,
    ) -> _QueryT:
        del string_clause, string_in_clause
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

    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_clause: Callable[[str, str], _QueryT] | None = None,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT] | None = None,
    ) -> _QueryT:
        del string_clause, string_in_clause
        return query_cls(
            numeric_property_range=api.NumericPropertyRangePredicate(
                name=self.name,
                operator=self.operator._to_conjure(),
                min=self.min_value,
                max=self.max_value,
            )
        )


PropertyFilter: TypeAlias = StringEqualityFilter | StringInFilter | NumericComparisonFilter | NumericRangeFilter


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
    """Equality filter on a string or numeric property.

    Types are not coerced: a string value only matches string properties, a float only matches numeric properties.
    """
    if isinstance(value, str):
        return StringEqualityFilter(name=name, value=value)
    return _comparison(name, PropertyComparisonOperator.EQ, value)


def in_(name: str, values: Sequence[str]) -> StringInFilter:
    """Match if the string property equals any of ``values``."""
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
