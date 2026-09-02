"""Nominal server-side functions not built into Ibis.

These are declared as "builtin" UDFs: Ibis compiles the call by name and the
server does the work. Both are window aggregates, so call them with .over():

    from nominal.ibis import derivative, integral

    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    pts.select(rate=derivative(_.value).over(w))
"""

from __future__ import annotations

from ibis import udf

__all__ = ["derivative", "integral"]


@udf.agg.builtin
def derivative(value: float) -> float:
    """Rate of change of a series with respect to time, per window frame."""


@udf.agg.builtin
def integral(value: float) -> float:
    """Trapezoidal integral of a series over time, per window frame."""
