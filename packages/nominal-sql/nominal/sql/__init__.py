"""Ibis backend for the Nominal SQL API.

Compiles Ibis expressions to SQL in the Nominal SQL API's dialect and executes
them over the public REST endpoint, streaming results back as Arrow.

Example:
    import ibis
    from ibis import _

    con = ibis.nominal.connect()  # uses the "default" profile in the Nominal config
    pts = con.table("points_double")
    (
        pts.filter(_.dataset_rid == "ri.catalog....", _.channel == "temperature")
        .group_by(minute=_.ts.truncate("m"))
        .agg(n=_.count(), avg=_.value.mean())
        .to_pandas()
    )

    # Server functions come from the SQL catalog; nothing is declared client-side.
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    pts.select(rate=con.fn.derivative(_.value).over(w)).to_pandas()
"""

from nominal.sql._backend import Backend, NominalSqlError, connect
from nominal.sql._functions import Functions

__all__ = [
    "Backend",
    "Functions",
    "NominalSqlError",
    "connect",
]
