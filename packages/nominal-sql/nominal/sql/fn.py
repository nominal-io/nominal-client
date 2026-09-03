"""Server functions from the SQL catalog, bound to the default Nominal profile.

Attribute access connects on first use (`ibis.nominal.connect()` with no arguments)
and caches the connection for the life of the process:

    from nominal.sql.fn import derivative, integral

    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    pts.select(rate=derivative(_.value).over(w))

Use `con.fn` instead when working with a named profile or several connections.
"""

from __future__ import annotations

from typing import Any, Callable

from nominal.sql._backend import Backend, connect

_connection: Backend | None = None


def _functions() -> Any:
    global _connection
    if _connection is None:
        _connection = connect()
    return _connection.fn


def __getattr__(name: str) -> Callable[..., Any]:
    if name.startswith("_"):
        raise AttributeError(name)
    return getattr(_functions(), name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_functions())))
