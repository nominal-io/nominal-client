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


def test_regex_search_renders_as_posix_match_operator() -> None:
    sql = compile_sql(POINTS.filter(_.channel.re_search("BATTERY")).select("ts"))
    assert " ~ " in sql
    assert "REGEXP_LIKE" not in sql.upper()


@pytest.mark.parametrize(
    "bound",
    [
        datetime(2026, 6, 18, 15, 0, 36, 600000),
        datetime(2026, 6, 18, 15, 0, 36, 600000, tzinfo=timezone.utc),
        datetime(2026, 6, 18, 11, 0, 36, 600000, tzinfo=timezone(timedelta(hours=-4))),
        pd.Timestamp("2026-06-18 15:00:36.6", tz="UTC"),
    ],
)
def test_timestamp_bounds_render_as_utc_literals(bound: datetime) -> None:
    """Zoned bounds, such as timestamps from a previous result, compile to the equivalent UTC literal."""
    sql = compile_sql(POINTS.filter(_.ts > bound).select("ts"))
    assert "> CAST('2026-06-18T15:00:36.600000' AS TIMESTAMP)" in sql


def test_zoned_timestamp_casts_render_as_timestamp() -> None:
    sql = compile_sql(
        POINTS.select(missing=ibis.null("timestamp('UTC', 9)"), parsed=_.channel.cast("timestamp('UTC')"))
    )
    assert "CAST(NULL AS TIMESTAMP(9))" in sql
    assert 'CAST("t0"."channel" AS TIMESTAMP)' in sql
