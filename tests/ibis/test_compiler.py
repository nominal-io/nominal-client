from __future__ import annotations

from datetime import datetime, timedelta, timezone

import ibis
import pandas as pd
import pytest
from ibis import _

from nominal.ibis import Backend

POINTS = ibis.table(
    {
        "ts": "timestamp('UTC', 9)",
        "value": "float64",
        "channel": "!string",
        "dataset_rid": "!string",
        "tags": "!map<string, string>",
    },
    name="points_double",
)


def compile_sql(expr: ibis.Table) -> str:
    """Compile an expression with the Nominal compiler and render it as SQL."""
    return Backend.compiler.to_sqlglot(expr).sql(Backend.compiler.dialect)


def test_map_columns_project_uncast() -> None:
    """Map columns are selected without the string cast the Postgres compiler adds for its driver."""
    sql = compile_sql(POINTS.select("ts", "tags").limit(5))
    assert '"tags"' in sql
    assert "CAST" not in sql


def test_map_get_renders_as_item_syntax() -> None:
    """Map lookups compile to bracket syntax instead of Postgres JSON functions."""
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
    """Aggregate windows keep their frame clause, unlike LAG/LEAD."""
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    sql = compile_sql(POINTS.select(total=_.value.sum().over(w)))
    assert "ROWS BETWEEN" in sql


@pytest.mark.parametrize("picked", [_.value.first(), _.value.last(), _.value.nth(1)], ids=["first", "last", "nth"])
def test_value_window_keeps_frame(picked: ibis.Deferred) -> None:
    """First, last, and nth values over a bounded window read only the rows that window covers."""
    w = ibis.window(group_by="channel", order_by="ts", preceding=2, following=0)
    sql = compile_sql(POINTS.select(picked=picked.over(w)))
    assert "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" in sql.upper()


def test_argmax_renders_as_max_by() -> None:
    """Argmax compiles to the API's max_by, not sqlglot's ARG_MAX canonicalization."""
    sql = compile_sql(POINTS.group_by("channel").agg(last=_.value.argmax(_.ts)))
    assert "MAX_BY" in sql.upper()
    assert "ARG_MAX" not in sql.upper()


def test_argmin_renders_as_min_by() -> None:
    """Argmin compiles to the API's min_by, not sqlglot's ARG_MIN canonicalization."""
    sql = compile_sql(POINTS.group_by("channel").agg(first=_.value.argmin(_.ts)))
    assert "MIN_BY" in sql.upper()
    assert "ARG_MIN" not in sql.upper()


def test_regex_search_renders_as_posix_match_operator() -> None:
    """Regex searches compile to the ~ operator instead of REGEXP_LIKE."""
    sql = compile_sql(POINTS.filter(_.channel.re_search("BATTERY")).select("ts"))
    assert " ~ " in sql
    assert "REGEXP_LIKE" not in sql.upper()


def test_timestamp_bucket_renders_date_bin_with_unit_interval() -> None:
    """Timestamp buckets compile to DATE_BIN with a unit interval and a literal epoch origin."""
    sql = compile_sql(POINTS.group_by(t=_.ts.bucket(seconds=10)).agg(n=_.count()))
    assert "DATE_BIN(INTERVAL '10' SECOND" in sql
    assert "CAST('1970-01-01 00:00:00' AS TIMESTAMP)" in sql
    assert "MAKE_TIMESTAMP" not in sql.upper()


def test_interval_arithmetic_uses_unit_interval() -> None:
    """Intervals compile to the INTERVAL 'n' UNIT form the API parses."""
    sql = compile_sql(POINTS.select(later=_.ts + ibis.interval(minutes=5)))
    assert "INTERVAL '5' MINUTE" in sql


@pytest.mark.parametrize(
    "bound",
    [
        datetime(2026, 6, 18, 15, 0, 36, 600000),
        datetime(2026, 6, 18, 11, 0, 36, 600000, tzinfo=timezone(timedelta(hours=-4))),
        pd.Timestamp("2026-06-18 15:00:36.6", tz="UTC"),
    ],
)
def test_timestamp_bounds_render_as_utc_literals(bound: datetime) -> None:
    """Naive and zoned filter bounds compile to UTC timestamps without offsets."""
    sql = compile_sql(POINTS.filter(_.ts > bound).select("ts"))
    assert "'2026-06-18T15:00:36.600000'" in sql
    assert "AS TIMESTAMP" in sql
    assert "TIMESTAMPTZ" not in sql


def test_zoned_timestamp_casts_render_as_timestamp() -> None:
    """Timestamp casts use the SQL API's TIMESTAMP type, including typed nulls."""
    sql = compile_sql(
        POINTS.select(missing=ibis.null("timestamp('UTC', 9)"), parsed=_.channel.cast("timestamp('UTC')"))
    )
    assert "CAST(NULL AS TIMESTAMP(9))" in sql
    assert "AS TIMESTAMP)" in sql
    assert "TIMESTAMPTZ" not in sql
