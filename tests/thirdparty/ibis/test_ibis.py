from __future__ import annotations

from unittest.mock import MagicMock, patch

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest

from nominal.core._clientsbunch import SqlService
from nominal.thirdparty.ibis import NominalSqlConnection

# ibis 12's duckdb backend still calls duckdb's deprecated `fetch_arrow_table()`, which the repo-wide
# `filterwarnings = error` would otherwise turn into a failure inside every backend call.
pytestmark = pytest.mark.filterwarnings("ignore:fetch_arrow_table\\(\\) is deprecated:DeprecationWarning")

WORKSPACE_RID = "ri.workspace.gov-staging.workspace.00000000-0000-0000-0000-000000000000"
AUTH_HEADER = "Bearer nominal-token"

CATALOG_RESPONSE = {
    "sqlCatalog": {
        "tables": [
            {
                "name": "points_double",
                "columns": [
                    {"name": "ts", "type": "TIMESTAMP"},
                    {"name": "value", "type": "DOUBLE", "nullable": True},
                    {"name": "channel", "type": "VARCHAR"},
                    {"name": "dataset_rid", "type": "VARCHAR"},
                    {"name": "tags", "type": "MAP"},
                ],
            },
            {
                "name": "runs",
                "columns": [
                    {"name": "run_rid", "type": "VARCHAR"},
                    {"name": "labels", "type": "ARRAY"},
                ],
            },
        ],
        "functions": [{"name": "AVG"}, {"name": "DATE_BIN"}],
    }
}


def arrow_payload(table: pa.Table) -> bytes:
    """Encode a table the way the SQL endpoint does: one Arrow IPC stream"""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


@pytest.fixture
def sql_service():
    service = MagicMock(spec=SqlService)
    service.get_sql_catalog.return_value = CATALOG_RESPONSE
    return service


@pytest.fixture
def conn(sql_service):
    return NominalSqlConnection(
        _sql=sql_service,
        _auth_header=AUTH_HEADER,
        workspace_rid=WORKSPACE_RID,
        backend=ibis.duckdb.connect(),
    )


# -- catalog --


def test_catalog_is_parsed_from_the_wire_response(conn):
    assert conn.list_tables() == ["points_double", "runs"]
    assert conn.list_functions() == ["AVG", "DATE_BIN"]


def test_catalog_is_fetched_once_and_cached(conn, sql_service):
    conn.list_tables()
    conn.list_functions()

    sql_service.get_sql_catalog.assert_called_once_with(AUTH_HEADER)


def test_get_schema_maps_sql_types_to_ibis_types(conn):
    assert conn.get_schema("points_double") == ibis.Schema(
        {
            "ts": dt.timestamp(nullable=False),
            "value": dt.float64,
            "channel": dt.string(nullable=False),
            "dataset_rid": dt.string(nullable=False),
            "tags": dt.Map(dt.string, dt.string, nullable=False),
        }
    )


def test_get_schema_falls_back_to_string_for_unknown_types(conn, sql_service):
    sql_service.get_sql_catalog.return_value = {
        "sqlCatalog": {"tables": [{"name": "t", "columns": [{"name": "c", "type": "GEOMETRY"}]}]}
    }

    assert conn.get_schema("t") == ibis.Schema({"c": dt.string(nullable=False)})


def test_get_schema_rejects_a_table_outside_the_catalog(conn):
    with pytest.raises(ValueError, match="No such table 'my_dataset'"):
        conn.get_schema("my_dataset")


def test_telemetry_tables_are_flagged_as_dataset_scoped(conn):
    catalog = conn.catalog()

    assert catalog.get_table("points_double").requires_dataset_rid_filter
    assert not catalog.get_table("runs").requires_dataset_rid_filter


# -- queries --


def test_sql_returns_an_ibis_table_over_the_query_result(conn, sql_service):
    results = pa.table({"channel": ["engine_rpm", "engine_temp"], "value": [4200.0, 91.5]})
    sql_service.query.return_value = arrow_payload(results)

    table = conn.sql("SELECT channel, value FROM points_double WHERE dataset_rid = 'ri.catalog.1'")

    assert table.to_pyarrow() == results
    assert table.filter(table.value > 100).select("channel").to_pyarrow().column("channel").to_pylist() == [
        "engine_rpm"
    ]


def test_sql_forwards_the_query_workspace_and_row_cap(conn, sql_service):
    sql_service.query.return_value = arrow_payload(pa.table({"n": [1]}))

    conn.sql("SELECT 1 AS n", max_rows=10)

    sql_service.query.assert_called_once_with(AUTH_HEADER, "SELECT 1 AS n", WORKSPACE_RID, 10)


def test_each_query_lands_in_its_own_table_unless_named(conn, sql_service):
    sql_service.query.return_value = arrow_payload(pa.table({"n": [1]}))

    first = conn.sql("SELECT 1 AS n")
    second = conn.sql("SELECT 1 AS n")
    named = conn.sql("SELECT 1 AS n", name="latest")

    sql_service.query.return_value = arrow_payload(pa.table({"n": [2]}))
    replaced = conn.sql("SELECT 2 AS n", name="latest")

    assert first.get_name() != second.get_name()
    assert named.get_name() == replaced.get_name()
    assert sorted(conn.backend.list_tables()) == ["latest", "nominal_sql_1", "nominal_sql_2"]
    assert replaced.to_pyarrow().column("n").to_pylist() == [2]


def test_to_pyarrow_does_not_materialize_into_the_backend(conn, sql_service):
    sql_service.query.return_value = arrow_payload(pa.table({"n": [1]}))

    assert conn.to_pyarrow("SELECT 1 AS n").column("n").to_pylist() == [1]
    assert conn.backend.list_tables() == []


def test_an_empty_response_is_reported_rather_than_parsed(conn, sql_service):
    sql_service.query.return_value = b""

    with pytest.raises(ValueError, match="empty response"):
        conn.sql("SELECT 1 AS n")


# -- export --


def test_export_downloads_the_presigned_csv_and_reads_it(conn, sql_service, tmp_path):
    sql_service.export.return_value = {"presignedUrl": "https://s3.example/export.csv", "queryId": "q-1"}
    download = MagicMock()
    download.iter_content.return_value = [b"channel,value\n", b"engine_rpm,4200.0\n"]
    csv_path = tmp_path / "export.csv"

    with patch("nominal.thirdparty.ibis._ibis.requests.get") as mock_get:
        mock_get.return_value.__enter__.return_value = download
        table = conn.export("SELECT channel, value FROM points_double", csv_path)

    sql_service.export.assert_called_once_with(AUTH_HEADER, "SELECT channel, value FROM points_double", WORKSPACE_RID)
    mock_get.assert_called_once_with("https://s3.example/export.csv", stream=True)
    assert csv_path.read_bytes() == b"channel,value\nengine_rpm,4200.0\n"
    assert table.to_pyarrow() == pa.table({"channel": ["engine_rpm"], "value": [4200.0]})


def test_export_raises_on_a_failed_download(conn, sql_service, tmp_path):
    sql_service.export.return_value = {"presignedUrl": "https://s3.example/export.csv", "queryId": "q-1"}
    download = MagicMock()
    download.raise_for_status.side_effect = RuntimeError("403 Forbidden")

    with patch("nominal.thirdparty.ibis._ibis.requests.get") as mock_get:
        mock_get.return_value.__enter__.return_value = download
        with pytest.raises(RuntimeError, match="403 Forbidden"):
            conn.export("SELECT 1", tmp_path / "export.csv")
