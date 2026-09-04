from __future__ import annotations

import ibis
from ibis import _

from nominal.sql import Backend
from nominal.sql._functions import build_function

POINTS = ibis.table(
    {
        "ts": "timestamp(9)",
        "value": "float64",
        "channel": "!string",
        "dataset_rid": "!string",
        "tags": "!map<string, string>",
    },
    name="points_double",
)


def compile_sql(expr: ibis.Table) -> str:
    """Compile an expression with the Nominal compiler and render it as SQL."""
    return Backend.compiler.to_sqlglot(expr).sql("postgres")


def test_map_columns_project_uncast() -> None:
    sql = compile_sql(POINTS.select("ts", "tags").limit(5))
    assert '"tags"' in sql
    assert "CAST" not in sql


def test_map_get_renders_as_item_syntax() -> None:
    sql = compile_sql(POINTS.filter(_.tags["site"] == "A").select("ts"))
    assert "\"tags\"['site']" in sql
    assert "json" not in sql.lower()


def test_lag_window_has_no_frame() -> None:
    """LAG/LEAD windows omit the frame clause the API rejects."""
    w = ibis.window(group_by="channel", order_by="ts")
    sql = compile_sql(POINTS.select(prev=_.value.lag(1).over(w)))
    assert "LAG" in sql
    assert "ROWS BETWEEN" not in sql
    assert "RANGE BETWEEN" not in sql


def test_aggregate_window_keeps_frame() -> None:
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    sql = compile_sql(POINTS.select(total=_.value.sum().over(w)))
    assert "ROWS BETWEEN" in sql


def test_argmax_renders_as_max_by() -> None:
    """Argmax compiles to the API's max_by, not sqlglot's ARG_MAX canonicalization."""
    sql = compile_sql(POINTS.group_by("channel").agg(last=_.value.argmax(_.ts)))
    assert "MAX_BY" in sql.upper()
    assert "ARG_MAX" not in sql.upper()


def test_argmin_renders_as_min_by() -> None:
    sql = compile_sql(POINTS.group_by("channel").agg(first=_.value.argmin(_.ts)))
    assert "MIN_BY" in sql.upper()
    assert "ARG_MIN" not in sql.upper()


def test_regex_search_renders_as_regexp_like_function() -> None:
    sql = compile_sql(POINTS.filter(_.channel.re_search("BATTERY")).select("ts"))
    assert "REGEXP_LIKE" in sql.upper()
    assert "~" not in sql


def catalog_function(
    name: str, kind: str, families: list[str], return_family: str | None = None, **extra: object
) -> object:
    entry: dict[str, object] = {
        "name": name,
        "kind": f"SQL_CATALOG_FUNCTION_KIND_{kind}",
        "minArgs": len(families),
        "maxArgs": len(families),
        "argumentTypeFamilies": families,
        **extra,
    }
    if return_family is not None:
        entry["returnTypeFamily"] = return_family
    function = build_function(entry)
    assert function is not None
    return function


def test_catalog_window_function_renders_by_name() -> None:
    derivative = catalog_function("DERIVATIVE", "WINDOW", ["NUMERIC"], "NUMERIC")
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    sql = compile_sql(POINTS.select(rate=derivative(_.value).over(w)))
    assert "derivative(" in sql.lower()
    assert "OVER (PARTITION BY" in sql


def test_catalog_function_bypasses_sqlglot_builtins() -> None:
    """Names sqlglot knows (date_bin, regexp_like) render verbatim rather than through sqlglot's own rules."""
    date_bin = catalog_function("DATE_BIN", "SCALAR", ["ANY", "DATETIME", "DATETIME"], "TIMESTAMP")
    origin = ibis.timestamp("2020-01-01 00:00:00")
    sql = compile_sql(POINTS.select(bucket=date_bin("1m", _.ts, origin)))
    assert "date_bin('1m'" in sql.lower()


def test_catalog_function_drops_omitted_optional_arguments() -> None:
    integral = catalog_function("INTEGRAL", "WINDOW", ["NUMERIC", "ANY"], "NUMERIC", minArgs=1)
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    assert 'integral("t0"."value") over' in compile_sql(POINTS.select(total=integral(_.value).over(w))).lower()
    assert (
        'integral("t0"."value", \'trapezoid\')'
        in compile_sql(POINTS.select(total=integral(_.value, "trapezoid").over(w))).lower()
    )
