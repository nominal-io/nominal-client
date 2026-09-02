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
"""

from nominal.ibis._backend import Backend, NominalSqlError, connect
from nominal.ibis._functions import derivative, integral

__all__ = [
    "Backend",
    "NominalSqlError",
    "connect",
    "derivative",
    "integral",
]
