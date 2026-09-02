from __future__ import annotations

import ibis
from ibis import _

from nominal.ibis import Backend
from nominal.ibis.functions import derivative

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
    return Backend.compiler.to_sqlglot(expr).sql("postgres")


def test_map_columns_are_not_cast_to_varchar() -> None:
    sql = compile_sql(POINTS.limit(5))
    assert "CAST" not in sql


def test_map_get_renders_as_calcite_item_syntax() -> None:
    sql = compile_sql(POINTS.filter(_.tags["site"] == "A").select("ts"))
    assert "\"tags\"['site']" in sql
    assert "json" not in sql.lower()


def test_lag_window_has_no_frame() -> None:
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


def test_builtin_window_udf_renders_by_name() -> None:
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    sql = compile_sql(POINTS.select(rate=derivative(_.value).over(w)))
    assert "DERIVATIVE" in sql.upper()
