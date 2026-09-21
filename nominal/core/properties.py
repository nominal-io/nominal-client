"""Property helpers for assets, runs, datasets, and events.

Filters are constructed on :class:`PropertyFilter`:

    from nominal.core import PropertyFilter

    client.search_runs(
        property_filters=[
            PropertyFilter.eq("serial", "A1"),
            PropertyFilter.in_("site", ["pad-a", "pad-b"]),
            PropertyFilter.gt("mass_kg", 10.0),
            PropertyFilter.between("temp_c", 0.0, 100.0),
        ]
    )
"""

from __future__ import annotations

from abc import ABC
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


class _PropertyComparisonOperator(Enum):
    EQ = "EQ"
    NEQ = "NEQ"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"

    def _to_conjure(self) -> api.PropertyComparisonOperator:
        return _PROPERTY_COMPARISON_OPERATOR_TO_CONJURE[self]


_PROPERTY_COMPARISON_OPERATOR_TO_CONJURE: Mapping[_PropertyComparisonOperator, api.PropertyComparisonOperator] = {
    _PropertyComparisonOperator.EQ: api.PropertyComparisonOperator.EQ,
    _PropertyComparisonOperator.NEQ: api.PropertyComparisonOperator.NEQ,
    _PropertyComparisonOperator.GT: api.PropertyComparisonOperator.GT,
    _PropertyComparisonOperator.GTE: api.PropertyComparisonOperator.GTE,
    _PropertyComparisonOperator.LT: api.PropertyComparisonOperator.LT,
    _PropertyComparisonOperator.LTE: api.PropertyComparisonOperator.LTE,
}


class _NumericPropertyRangeOperator(Enum):
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT_BETWEEN"

    def _to_conjure(self) -> api.NumericPropertyRangeOperator:
        return _NUMERIC_PROPERTY_RANGE_OPERATOR_TO_CONJURE[self]


_NUMERIC_PROPERTY_RANGE_OPERATOR_TO_CONJURE: Mapping[
    _NumericPropertyRangeOperator, api.NumericPropertyRangeOperator
] = {
    _NumericPropertyRangeOperator.BETWEEN: api.NumericPropertyRangeOperator.BETWEEN,
    _NumericPropertyRangeOperator.NOT_BETWEEN: api.NumericPropertyRangeOperator.NOT_BETWEEN,
}


def _as_numeric_value(value: object, *, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{what} must be int or float, got {type(value).__name__}")
    return float(value)


class PropertyFilter(ABC):
    """A filter on a typed property. Construct via classmethods such as :meth:`eq`."""

    @classmethod
    def eq(cls, name: str, value: str | float) -> PropertyFilter:
        """Equality filter on a string or numeric property.

        Types are not coerced across string vs numeric: ``eq("x", "1")`` matches only
        string properties and ``eq("x", 1.0)`` matches only numeric properties.
        """
        if isinstance(value, str):
            return _StringInFilter(name=name, values=(value,))
        return _comparison(name, _PropertyComparisonOperator.EQ, value)

    @classmethod
    def in_(cls, name: str, values: Sequence[str]) -> PropertyFilter:
        """Match if the string property equals any of ``values``.

        ``values`` must be non-empty.
        """
        if not values:
            raise ValueError("in_() requires at least one value")
        return _StringInFilter(name=name, values=tuple(values))

    @classmethod
    def neq(cls, name: str, value: float) -> PropertyFilter:
        """Inequality filter on a numeric property."""
        return _comparison(name, _PropertyComparisonOperator.NEQ, value)

    @classmethod
    def gt(cls, name: str, value: float) -> PropertyFilter:
        """Greater-than filter on a numeric property."""
        return _comparison(name, _PropertyComparisonOperator.GT, value)

    @classmethod
    def gte(cls, name: str, value: float) -> PropertyFilter:
        """Greater-than-or-equal filter on a numeric property."""
        return _comparison(name, _PropertyComparisonOperator.GTE, value)

    @classmethod
    def lt(cls, name: str, value: float) -> PropertyFilter:
        """Less-than filter on a numeric property."""
        return _comparison(name, _PropertyComparisonOperator.LT, value)

    @classmethod
    def lte(cls, name: str, value: float) -> PropertyFilter:
        """Less-than-or-equal filter on a numeric property."""
        return _comparison(name, _PropertyComparisonOperator.LTE, value)

    @classmethod
    def between(cls, name: str, min_value: float, max_value: float) -> PropertyFilter:
        """Inclusive range filter: ``min_value <= property <= max_value``."""
        return _range(name, _NumericPropertyRangeOperator.BETWEEN, min_value, max_value)

    @classmethod
    def not_between(cls, name: str, min_value: float, max_value: float) -> PropertyFilter:
        """Filter matching values outside the inclusive range ``[min_value, max_value]``."""
        return _range(name, _NumericPropertyRangeOperator.NOT_BETWEEN, min_value, max_value)


@dataclass(frozen=True)
class _StringInFilter(PropertyFilter):
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
class _NumericComparisonFilter(PropertyFilter):
    name: str
    operator: _PropertyComparisonOperator
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
class _NumericRangeFilter(PropertyFilter):
    name: str
    operator: _NumericPropertyRangeOperator
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


def _comparison(name: str, operator: _PropertyComparisonOperator, value: float) -> _NumericComparisonFilter:
    return _NumericComparisonFilter(
        name=name,
        operator=operator,
        value=_as_numeric_value(value, what="value"),
    )


def _range(
    name: str,
    operator: _NumericPropertyRangeOperator,
    min_value: float,
    max_value: float,
) -> _NumericRangeFilter:
    return _NumericRangeFilter(
        name=name,
        operator=operator,
        min_value=_as_numeric_value(min_value, what="min_value"),
        max_value=_as_numeric_value(max_value, what="max_value"),
    )
