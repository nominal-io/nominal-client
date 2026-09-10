"""Ibis backend for the Nominal SQL API.

Compiles Ibis expressions to SQL in the Nominal SQL API's dialect and executes
them through an authenticated NominalClient, streaming results back as Arrow.

Example:
    import ibis
    from ibis import _
    from nominal.core import NominalClient

    client = NominalClient.from_profile("prod")
    con = ibis.nominal.connect(client)
    pts = con.table("points_double")
    df = (
        pts.filter(_.dataset_rid == "ri.catalog....", _.channel == "temperature")
        .group_by(minute=_.ts.truncate("m"))
        .agg(n=_.count(), avg=_.value.mean())
        .to_pandas()
    )

    # Server functions come from the SQL catalog; nothing is declared client-side.
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    rates = pts.select(rate=con.fn.derivative(_.value).over(w)).to_pandas()
"""

from nominal.ibis._backend import Backend, NominalSqlError, connect
from nominal.ibis._functions import Functions

__all__ = [
    "Backend",
    "Functions",
    "NominalSqlError",
    "connect",
]
