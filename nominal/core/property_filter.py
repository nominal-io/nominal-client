"""Property helpers for assets, runs, datasets, and events.

Filters are constructed on ``PropertyFilter``:

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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Sequence, TypeVar

from nominal_api import api

from nominal.core._utils.api_types import PropertyValue

_QueryT = TypeVar("_QueryT")


def _as_numeric_value(value: object, *, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{what} must be int or float, got {type(value).__name__}")
    return float(value)


class PropertyFilter(ABC):
    """A filter on a typed property. Construct via classmethods such as ``eq()``."""

    @classmethod
    def eq(cls, name: str, value: PropertyValue) -> PropertyFilter:
        """Equality filter on a string or numeric property.

        Types are not coerced across string vs numeric: ``eq("x", "1")`` matches only
        string properties and ``eq("x", 1.0)`` matches only numeric properties.
        """
        if isinstance(value, str):
            return _StringInFilter(name=name, values=(value,))
        return _NumericComparisonFilter(
            name=name,
            operator=api.PropertyComparisonOperator.EQ,
            value=_as_numeric_value(value, what="value"),
        )

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
        return _NumericComparisonFilter(
            name=name,
            operator=api.PropertyComparisonOperator.NEQ,
            value=_as_numeric_value(value, what="value"),
        )

    @classmethod
    def gt(cls, name: str, value: float) -> PropertyFilter:
        """Greater-than filter on a numeric property."""
        return _NumericComparisonFilter(
            name=name,
            operator=api.PropertyComparisonOperator.GT,
            value=_as_numeric_value(value, what="value"),
        )

    @classmethod
    def gte(cls, name: str, value: float) -> PropertyFilter:
        """Greater-than-or-equal filter on a numeric property."""
        return _NumericComparisonFilter(
            name=name,
            operator=api.PropertyComparisonOperator.GTE,
            value=_as_numeric_value(value, what="value"),
        )

    @classmethod
    def lt(cls, name: str, value: float) -> PropertyFilter:
        """Less-than filter on a numeric property."""
        return _NumericComparisonFilter(
            name=name,
            operator=api.PropertyComparisonOperator.LT,
            value=_as_numeric_value(value, what="value"),
        )

    @classmethod
    def lte(cls, name: str, value: float) -> PropertyFilter:
        """Less-than-or-equal filter on a numeric property."""
        return _NumericComparisonFilter(
            name=name,
            operator=api.PropertyComparisonOperator.LTE,
            value=_as_numeric_value(value, what="value"),
        )

    @classmethod
    def between(cls, name: str, min_value: float, max_value: float) -> PropertyFilter:
        """Inclusive range filter: ``min_value <= property <= max_value``."""
        return _NumericRangeFilter(
            name=name,
            operator=api.NumericPropertyRangeOperator.BETWEEN,
            min_value=_as_numeric_value(min_value, what="min_value"),
            max_value=_as_numeric_value(max_value, what="max_value"),
        )

    @classmethod
    def not_between(cls, name: str, min_value: float, max_value: float) -> PropertyFilter:
        """Filter matching values outside the inclusive range ``[min_value, max_value]``."""
        return _NumericRangeFilter(
            name=name,
            operator=api.NumericPropertyRangeOperator.NOT_BETWEEN,
            min_value=_as_numeric_value(min_value, what="min_value"),
            max_value=_as_numeric_value(max_value, what="max_value"),
        )

    @abstractmethod
    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT],
    ) -> _QueryT:
        """Build the conjure search clause for this filter."""


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
    operator: api.PropertyComparisonOperator
    value: float

    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT],
    ) -> _QueryT:
        del string_in_clause
        return query_cls(
            numeric_property=api.NumericPropertyPredicate(
                name=self.name,
                operator=self.operator,
                value=self.value,
            )
        )


@dataclass(frozen=True)
class _NumericRangeFilter(PropertyFilter):
    name: str
    operator: api.NumericPropertyRangeOperator
    min_value: float
    max_value: float

    def to_query_clause(
        self,
        query_cls: Callable[..., _QueryT],
        *,
        string_in_clause: Callable[[str, Sequence[str]], _QueryT],
    ) -> _QueryT:
        del string_in_clause
        return query_cls(
            numeric_property_range=api.NumericPropertyRangePredicate(
                name=self.name,
                operator=self.operator,
                min=self.min_value,
                max=self.max_value,
            )
        )
