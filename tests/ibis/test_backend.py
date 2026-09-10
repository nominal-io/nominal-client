from __future__ import annotations

import io
from typing import Iterator
from unittest.mock import MagicMock

import grpc
import ibis
import ibis.common.exceptions as com
import pyarrow as pa
import pytest
from ibis import _
from ibis.common.annotations import SignatureValidationError

import nominal.ibis as nibis
from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalInvalidArgumentError
from nominal.protos.sql.v1 import sql_pb2

WORKSPACE_RID = "ri.security.x.workspace.1"


def column(name: str, type_: str, nullable: bool = False) -> sql_pb2.SqlCatalogColumn:
    return sql_pb2.SqlCatalogColumn(name=name, type=type_, nullable=nullable)


def function(
    name: str,
    kind: str,
    families: list[str],
    return_family: str | None = None,
    **fields: object,
) -> sql_pb2.SqlCatalogFunction:
    entry = sql_pb2.SqlCatalogFunction(
        name=name,
        kind=getattr(sql_pb2, f"SQL_CATALOG_FUNCTION_KIND_{kind}"),
        min_args=len(families),
        max_args=len(families),
        argument_type_families=families,
        **fields,
    )
    if return_family is not None:
        entry.return_type_family = return_family
    return entry


CATALOG = sql_pb2.SqlCatalog(
    tables=[
        sql_pb2.SqlCatalogTable(
            name="points_double",
            columns=[
                column("ts", "TIMESTAMP"),
                column("value", "DOUBLE", nullable=True),
                column("channel", "VARCHAR"),
                column("dataset_rid", "VARCHAR"),
                column("tags", "MAP"),
            ],
        ),
        sql_pb2.SqlCatalogTable(
            name="datasets",
            columns=[column("dataset_rid", "VARCHAR"), column("name", "VARCHAR")],
        ),
    ],
    functions=[
        function("AVG", "AGGREGATE", ["NUMERIC"], "NUMERIC", supports_over=True),
        sql_pb2.SqlCatalogFunction(name="COALESCE", kind=sql_pb2.SQL_CATALOG_FUNCTION_KIND_SCALAR, min_args=1),
        function("DATE_BIN", "SCALAR", ["ANY", "DATETIME", "DATETIME"], "TIMESTAMP"),
        function("DERIVATIVE", "WINDOW", ["NUMERIC"], "NUMERIC", supports_over=True),
        sql_pb2.SqlCatalogFunction(name="LEGACY_NAME_ONLY"),
        sql_pb2.SqlCatalogFunction(
            name="MAX", kind=sql_pb2.SQL_CATALOG_FUNCTION_KIND_AGGREGATE, supports_over=True, min_args=1, max_args=1
        ),
        function("REGEXP_LIKE", "SCALAR", ["CHARACTER", "CHARACTER"], "BOOLEAN"),
    ],
)

QUERY_RESULT = pa.table({"dataset_rid": ["ri.catalog.x.dataset.1"], "name": ["flight"], "extra_sort_key": [1]})


def arrow_ipc_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def make_client(query_result: pa.Table = QUERY_RESULT, chunk_size: int = 7) -> NominalClient:
    """Client whose SQL stub serves the fixture catalog and streams `query_result` in small payload chunks."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = WORKSPACE_RID
    clients.sql.GetSqlCatalog.return_value = sql_pb2.GetSqlCatalogResponse(sql_catalog=CATALOG)

    def query(request: sql_pb2.SqlServiceQueryRequest) -> Iterator[sql_pb2.SqlServiceQueryResponse]:
        payload = arrow_ipc_bytes(query_result)
        return iter(
            sql_pb2.SqlServiceQueryResponse(query_id="q", payload=payload[i : i + chunk_size])
            for i in range(0, len(payload), chunk_size)
        )

    clients.sql.Query.side_effect = query
    return NominalClient(_clients=clients)


@pytest.fixture
def client() -> NominalClient:
    return make_client()


@pytest.fixture
def backend(client: NominalClient) -> nibis.Backend:
    return nibis.connect(client)


def test_ibis_entry_point_connects_with_a_client(client: NominalClient) -> None:
    con = ibis.nominal.connect(client)
    assert isinstance(con, nibis.Backend)
    assert con.list_tables() == ["datasets", "points_double"]


def test_workspace_comes_from_the_client(backend: nibis.Backend, client: NominalClient) -> None:
    assert backend.workspace_rid == WORKSPACE_RID
    client._clients.resolve_default_workspace_rid.assert_called_once_with()


def test_list_tables_from_catalog(backend: nibis.Backend) -> None:
    assert backend.list_tables() == ["datasets", "points_double"]


def test_schema_types_from_catalog(backend: nibis.Backend) -> None:
    schema = backend.table("points_double").schema()
    assert schema["ts"].is_timestamp()
    assert schema["value"].is_float64()
    assert schema["value"].nullable
    assert not schema["channel"].nullable
    assert schema["tags"].is_map()


def test_functions_generated_from_catalog(backend: nibis.Backend, client: NominalClient) -> None:
    """Every expressible catalog function is exposed on con.fn; variadic and kind-less entries are skipped."""
    assert list(backend.fn) == ["avg", "date_bin", "derivative", "max", "regexp_like"]
    backend.list_tables()
    client._clients.sql.GetSqlCatalog.assert_called_once()
    with pytest.raises(AttributeError, match="no function named 'nope'"):
        backend.fn.nope


def test_catalog_function_types_follow_type_families(backend: nibis.Backend) -> None:
    pts = backend.table("points_double")
    w = ibis.cumulative_window(group_by="channel", order_by="ts")
    assert pts.select(rate=backend.fn.derivative(_.value).over(w)).schema()["rate"].is_float64()
    origin = ibis.timestamp("2020-01-01 00:00:00")
    assert pts.select(b=backend.fn.date_bin("1m", _.ts, origin)).schema()["b"].is_timestamp()
    assert pts.select(m=backend.fn.regexp_like(_.channel, "^temp")).schema()["m"].is_boolean()
    assert pts.select(m=backend.fn.max(_.value)).schema()["m"].is_unknown()
    with pytest.raises(SignatureValidationError):
        backend.fn.derivative(pts.channel)


def test_unknown_table_raises(backend: nibis.Backend) -> None:
    with pytest.raises(com.TableNotFound):
        backend.table("nope")


def test_query_request_carries_workspace_and_arrow_format(backend: nibis.Backend, client: NominalClient) -> None:
    backend.table("datasets").select("dataset_rid", "name").to_pandas()
    request = client._clients.sql.Query.call_args.args[0]
    assert request.workspace_rid == WORKSPACE_RID
    assert request.result_format == sql_pb2.SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM
    assert not request.HasField("max_rows")


def test_raw_sql_schema_probe_sets_max_rows(backend: nibis.Backend, client: NominalClient) -> None:
    backend.sql("SELECT dataset_rid, name, extra_sort_key FROM datasets")
    assert client._clients.sql.Query.call_args.args[0].max_rows == 1


def test_execute_drops_leaked_sort_key_columns(backend: nibis.Backend) -> None:
    """The server appends ORDER BY keys to the projection; requested columns are selected back by name."""
    df = backend.table("datasets").select("dataset_rid", "name").to_pandas()
    assert list(df.columns) == ["dataset_rid", "name"]
    assert df["name"][0] == "flight"


def test_fewer_columns_than_requested_raises() -> None:
    con = nibis.connect(make_client(query_result=pa.table({"dataset_rid": ["ri.catalog.x.dataset.1"]})))
    with pytest.raises(nibis.NominalSqlError, match="expected"):
        con.table("datasets").select("dataset_rid", "name").to_pandas()


def test_to_pyarrow_batches_reassembles_the_stream_across_payload_chunks(backend: nibis.Backend) -> None:
    """Payload boundaries are arbitrary byte offsets, not record-batch boundaries."""
    expr = backend.table("datasets").select("dataset_rid", "name")
    with expr.to_pyarrow_batches() as reader:
        table = reader.read_all()
    assert table.column_names == ["dataset_rid", "name"]
    assert table.num_rows == 1


def test_write_operations_are_rejected(backend: nibis.Backend) -> None:
    with pytest.raises(com.UnsupportedOperationError):
        backend.create_table("t", schema={"a": "int64"})


def test_grpc_errors_surface_as_nominal_errors(backend: nibis.Backend, client: NominalClient) -> None:
    class InvalidQuery(grpc.RpcError):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.INVALID_ARGUMENT

        def details(self) -> str:
            return "unknown column"

    def failing_query(request: sql_pb2.SqlServiceQueryRequest) -> Iterator[sql_pb2.SqlServiceQueryResponse]:
        raise InvalidQuery()
        yield

    client._clients.sql.Query.side_effect = failing_query
    with pytest.raises(NominalInvalidArgumentError, match="unknown column"):
        backend.table("datasets").select("name").to_pandas()


def test_module_imports_cleanly_in_fresh_interpreter() -> None:
    """Importing nominal.ibis before anything else from nominal must not trip an import cycle."""
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c", "from nominal.ibis import Backend, connect"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
