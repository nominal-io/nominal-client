from __future__ import annotations

import io
from typing import Iterator
from unittest.mock import MagicMock

import grpc
import ibis
import ibis.common.exceptions as com
import pyarrow as pa
import pytest

import nominal.ibis as nibis
from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalInvalidArgumentError
from nominal.protos.sql.v1 import sql_pb2

WORKSPACE_RID = "ri.security.x.workspace.1"


def column(name: str, type_: str, nullable: bool = False) -> sql_pb2.SqlCatalogColumn:
    return sql_pb2.SqlCatalogColumn(name=name, type=type_, nullable=nullable)


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


@pytest.mark.parametrize("output", ["pandas", "arrow", "batches"])
def test_native_max_preserves_numeric_result(output: str) -> None:
    """Native Ibis aggregates retain their numeric type through every result path."""
    con = nibis.connect(make_client(query_result=pa.table({"maximum": [1.5]})))
    points = con.table("points_double")
    expr = points.aggregate(maximum=points.value.max())
    assert expr.schema()["maximum"].is_float64()
    if output == "pandas":
        result = expr.to_pandas()
        assert result["maximum"].dtype.kind == "f"
        assert result["maximum"].tolist() == [1.5]
    else:
        if output == "arrow":
            table = expr.to_pyarrow()
        else:
            with expr.to_pyarrow_batches() as reader:
                table = reader.read_all()
        assert table.schema.field("maximum").type == pa.float64()
        assert table.column("maximum").to_pylist() == [1.5]


@pytest.mark.parametrize("output", ["pandas", "arrow", "batches"])
@pytest.mark.parametrize("names", [["name", "dataset_rid"], ["first", "second"]])
def test_unexpected_columns_are_not_reordered_or_renamed(output: str, names: list[str]) -> None:
    """All output paths reject columns that do not match the requested projection."""
    con = nibis.connect(make_client(pa.table({name: ["value"] for name in names})))
    expr = con.table("datasets")
    with pytest.raises(nibis.NominalSqlError, match="Server returned columns"):
        if output == "pandas":
            expr.to_pandas()
        elif output == "arrow":
            expr.to_pyarrow()
        else:
            with expr.to_pyarrow_batches() as reader:
                reader.read_all()


@pytest.mark.parametrize("output", ["pandas", "arrow", "batches"])
def test_incompatible_result_type_raises(output: str) -> None:
    """Failed conversions report the same error for materialized and streamed results."""
    con = nibis.connect(make_client(pa.table({"value": ["not a number"]})))
    expr = con.table("points_double").select("value")
    with pytest.raises(nibis.NominalSqlError, match="not castable"):
        if output == "pandas":
            expr.to_pandas()
        elif output == "arrow":
            expr.to_pyarrow()
        else:
            with expr.to_pyarrow_batches() as reader:
                reader.read_all()
