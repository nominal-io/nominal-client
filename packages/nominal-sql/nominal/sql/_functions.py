from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Iterator, Mapping, Sequence

import ibis.expr.datatypes as dt
from ibis import udf

logger = logging.getLogger(__name__)

_SCALAR_KIND = "SQL_CATALOG_FUNCTION_KIND_SCALAR"
_AGGREGATE_KINDS = frozenset({"SQL_CATALOG_FUNCTION_KIND_AGGREGATE", "SQL_CATALOG_FUNCTION_KIND_WINDOW"})

# Catalog type families; an unlisted family accepts any argument and yields an unknown result.
_ARGUMENT_TYPES: Mapping[str, dt.DataType] = {
    "NUMERIC": dt.float64,
    "APPROXIMATE_NUMERIC": dt.float64,
    "EXACT_NUMERIC": dt.int64,
    "INTEGER": dt.int64,
    "DECIMAL": dt.float64,
    "CHARACTER": dt.string,
    "STRING": dt.string,
    "BOOLEAN": dt.boolean,
    "DATETIME": dt.timestamp,
    "TIMESTAMP": dt.timestamp,
    "DATE": dt.date,
    "TIME": dt.time,
    "BINARY": dt.binary,
    "ARRAY": dt.Array(dt.string),
    "MAP": dt.Map(dt.string, dt.string),
}
_RETURN_TYPES: Mapping[str, dt.DataType] = {
    **_ARGUMENT_TYPES,
    "DATETIME": dt.Timestamp(scale=9),
    "TIMESTAMP": dt.Timestamp(scale=9),
}


def build_function(entry: Mapping[str, Any]) -> Callable[..., Any] | None:
    """Build an Ibis builtin UDF from one catalog function entry, or None if it cannot be expressed.

    Variadic functions and entries without a kind (older servers) are skipped; Ibis
    has native methods for the standard SQL functions in that set.
    """
    name = str(entry["name"]).lower()
    kind = entry.get("kind")
    max_args = entry.get("maxArgs")
    if kind == _SCALAR_KIND:
        decorator = udf.scalar.builtin
    elif kind in _AGGREGATE_KINDS:
        decorator = udf.agg.builtin
    else:
        logger.debug("skipping catalog function %s: kind %r is not supported", name, kind)
        return None
    if max_args is None:
        logger.debug("skipping catalog function %s: variadic functions cannot be wrapped", name)
        return None

    min_args = int(entry.get("minArgs", 0))
    families: Sequence[str] = entry.get("argumentTypeFamilies") or []
    parameters = []
    annotations: dict[str, Any] = {}
    for index in range(int(max_args)):
        arg = f"arg{index}"
        default = {} if index < min_args else {"default": None}
        parameters.append(inspect.Parameter(arg, inspect.Parameter.POSITIONAL_OR_KEYWORD, **default))
        if index < len(families) and families[index] in _ARGUMENT_TYPES:
            annotations[arg] = _ARGUMENT_TYPES[families[index]]
    return_family = entry.get("returnTypeFamily")
    annotations["return"] = _RETURN_TYPES.get(return_family, dt.unknown) if return_family else dt.unknown

    def stub(*args: Any) -> Any:
        """Server-side function; compiled by name and executed by the Nominal SQL API."""

    stub.__name__ = name
    stub.__qualname__ = name
    stub.__signature__ = inspect.Signature(parameters)  # type: ignore[attr-defined]
    stub.__annotations__ = annotations
    return decorator(stub, name=name)


class Functions:
    """Attribute namespace of the server functions advertised by the SQL catalog.

    Names are lowercase; call them like any Ibis function and use `.over()` for
    window functions:

        w = ibis.cumulative_window(group_by="channel", order_by="ts")
        pts.select(rate=con.fn.derivative(_.value).over(w))
    """

    def __init__(self, entries: Sequence[Mapping[str, Any]]) -> None:
        functions = ((str(entry["name"]).lower(), build_function(entry)) for entry in entries)
        self._functions: dict[str, Callable[..., Any]] = {
            name: function for name, function in functions if function is not None
        }

    def __getattr__(self, name: str) -> Callable[..., Any]:
        try:
            return self._functions[name]
        except KeyError:
            raise AttributeError(f"the SQL catalog has no function named {name!r}") from None

    def __getitem__(self, name: str) -> Callable[..., Any]:
        return self._functions[name.lower()]

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name.lower() in self._functions

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(self._functions))

    def __len__(self) -> int:
        return len(self._functions)

    def __dir__(self) -> list[str]:
        return sorted(self._functions)

    def __repr__(self) -> str:
        return f"Functions({', '.join(sorted(self._functions))})"
