"""Property helpers for assets, runs, and datasets.

Filters live here so they stay namespaced:

    from nominal.core import properties as props

    client.search_runs(
        property_filters=[
            props.eq("serial", "A1"),
            props.gt("mass_kg", 10.0),
            props.between("temp_c", 0.0, 100.0),
        ]
    )
"""

from nominal.core._utils.properties import (
    NumericPropertyRangeOperator,
    PropertyComparisonOperator,
    PropertyFilter,
    PropertyValue,
    TypedProperties,
    between,
    eq,
    gt,
    gte,
    lt,
    lte,
    neq,
    not_between,
)

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
    "lt",
    "lte",
    "neq",
    "not_between",
]
