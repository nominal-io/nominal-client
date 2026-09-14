from __future__ import annotations

import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock

import grpc
import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest

import nominal.ibis as nibis
from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalInvalidArgumentError
from nominal.protos.sql.v1 import sql_pb2, sql_pb2_grpc

WORKSPACE_RID = "ri.security.x.workspace.1"


def column(name: str, type_: str, nullable: bool = False) -> sql_pb2.SqlCatalogColumn:
    if type_ == "MAP":
        string = sql_pb2.SqlCatalogDataType(scalar=sql_pb2.SQL_CATALOG_SCALAR_TYPE_VARCHAR)
        data_type = sql_pb2.SqlCatalogDataType(map=sql_pb2.SqlCatalogMapType(key=string, value=string))
    else:
        data_type = sql_pb2.SqlCatalogDataType(
            scalar=sql_pb2.SqlCatalogScalarType.Value(f"SQL_CATALOG_SCALAR_TYPE_{type_}")
        )
    return sql_pb2.SqlCatalogColumn(name=name, type=type_, nullable=nullable, data_type=data_type)


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

QUERY_RESULT = pa.table({"dataset_rid": ["ri.catalog.x.dataset.1"], "name": ["flight"]})


def arrow_ipc_bytes(table: pa.Table, max_chunksize: int | None = None) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table, max_chunksize=max_chunksize)
    return sink.getvalue()


def query_call(payload: bytes, chunk_size: int = 7) -> MagicMock:
    """A cancellable gRPC call whose iterator yields arbitrary payload boundaries."""
    call = MagicMock()
    call.__iter__.return_value = iter(
        sql_pb2.SqlServiceQueryResponse(query_id="q", payload=payload[i : i + chunk_size])
        for i in range(0, len(payload), chunk_size)
    )
    return call


def make_client(query_result: pa.Table = QUERY_RESULT, chunk_size: int = 7) -> NominalClient:
    """Client whose SQL stub serves the fixture catalog and streams `query_result` in small payload chunks."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = WORKSPACE_RID
    clients.sql.GetSqlCatalog.return_value = sql_pb2.GetSqlCatalogResponse(sql_catalog=CATALOG)

    def query(request: sql_pb2.SqlServiceQueryRequest) -> MagicMock:
        return query_call(arrow_ipc_bytes(query_result), chunk_size)

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
    backend.sql("SELECT dataset_rid, name FROM datasets")
    assert client._clients.sql.Query.call_args.args[0].max_rows == 1


def test_execute_returns_selected_columns(backend: nibis.Backend) -> None:
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


@pytest.mark.parametrize("exit_mode", ["close", "context", "exception", "exhaust", "arrow_export"])
def test_batch_reader_cancels_query_without_closing_client(client: NominalClient, exit_mode: str) -> None:
    """Closing, exhausting, or exporting a reader releases its RPC while the client stays usable."""
    call = query_call(arrow_ipc_bytes(pa.concat_tables([QUERY_RESULT, QUERY_RESULT]), max_chunksize=1))
    client._clients.sql.Query.side_effect = None
    client._clients.sql.Query.return_value = call
    con = nibis.connect(client)
    reader = con.table("datasets").to_pyarrow_batches()
    client._clients.sql.Query.assert_not_called()
    assert reader.read_next_batch().num_rows == 1
    call.cancel.assert_not_called()

    if exit_mode == "close":
        reader.close()
    elif exit_mode == "context":
        with reader:
            pass
    elif exit_mode == "exception":
        with pytest.raises(ValueError, match="consumer failed"), reader:
            raise ValueError("consumer failed")
    elif exit_mode == "exhaust":
        assert [batch.num_rows for batch in reader] == [1]
    else:
        exported = pa.RecordBatchReader._import_from_c_capsule(reader.__arrow_c_stream__())
        exported.close()

    call.cancel.assert_called_once_with()
    reader.close()
    call.cancel.assert_called_once_with()
    client._clients.grpc_channel.close.assert_not_called()

    next_call = query_call(arrow_ipc_bytes(QUERY_RESULT))
    client._clients.sql.Query.return_value = next_call
    assert con.table("datasets").to_pandas()["name"].tolist() == ["flight"]
    next_call.cancel.assert_called_once_with()


def test_closing_unstarted_batch_reader_does_not_start_query(client: NominalClient) -> None:
    """Closing before the first read never opens a query RPC."""
    reader = nibis.connect(client).table("datasets").to_pyarrow_batches()
    reader.close()
    assert list(reader) == []
    client._clients.sql.Query.assert_not_called()


def test_batch_reader_close_cancels_live_grpc_call(client: NominalClient) -> None:
    """An early close reaches the server while the reader remains referenced by the caller."""
    cancelled = threading.Event()
    finish = threading.Event()
    result = pa.table({"dataset_rid": ["dataset"] * 10_000, "name": ["flight"] * 10_000})
    payload = arrow_ipc_bytes(result, max_chunksize=1000)

    class Service(sql_pb2_grpc.SqlServiceServicer):
        def Query(self, request: sql_pb2.SqlServiceQueryRequest, context: grpc.ServicerContext):
            context.add_callback(cancelled.set)
            yield sql_pb2.SqlServiceQueryResponse(payload=payload)
            finish.wait(10)

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        sql_pb2_grpc.add_SqlServiceServicer_to_server(Service(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                client._clients.sql.Query = sql_pb2_grpc.SqlServiceStub(channel).Query
                reader = nibis.connect(client).table("datasets").to_pyarrow_batches()
                with reader:
                    assert reader.read_next_batch().num_rows == 1000
                    assert not cancelled.is_set()
                assert cancelled.wait(2), "Closing the reader did not cancel the RPC"
        finally:
            finish.set()
            server.stop(0).wait()


@pytest.mark.parametrize("operation", ["raw_sql", "schema_probe", "invalid_stream", "invalid_columns", "invalid_cast"])
def test_query_rpc_is_cancelled_on_completion_or_error(client: NominalClient, operation: str) -> None:
    """Schema probes, materialized reads, and stream/conversion failures all release the RPC."""
    result = pa.table({"value": ["not numeric"]}) if operation == "invalid_cast" else QUERY_RESULT
    payload = b"invalid Arrow" if operation == "invalid_stream" else arrow_ipc_bytes(result)
    call = query_call(payload)
    client._clients.sql.Query.side_effect = None
    client._clients.sql.Query.return_value = call
    con = nibis.connect(client)
    if operation == "raw_sql":
        assert con.raw_sql("SELECT * FROM datasets").equals(QUERY_RESULT)
    elif operation == "schema_probe":
        con.sql("SELECT * FROM datasets")
    elif operation == "invalid_stream":
        with pytest.raises(pa.ArrowInvalid):
            con.raw_sql("SELECT * FROM datasets")
    else:
        expr = con.table("points_double").select("value")
        reader = expr.to_pyarrow_batches()
        with pytest.raises(nibis.NominalSqlError):
            reader.read_all()
    call.cancel.assert_called_once_with()


def test_exported_arrow_stream_keeps_temporary_reader_alive(client: NominalClient) -> None:
    """An Arrow consumer can own the stream after the Python wrapper goes out of scope."""
    call = query_call(arrow_ipc_bytes(QUERY_RESULT))
    client._clients.sql.Query.side_effect = None
    client._clients.sql.Query.return_value = call
    expr = nibis.connect(client).table("datasets")
    with pa.RecordBatchReader._import_from_c_capsule(expr.to_pyarrow_batches().__arrow_c_stream__()) as reader:
        assert reader.read_all().to_pydict() == QUERY_RESULT.to_pydict()
    call.cancel.assert_called_once_with()


def test_managed_reader_supports_ibis_dataset_export(backend: nibis.Backend, tmp_path: Path) -> None:
    """Ibis can write a Parquet dataset through the managed reader, including on PyArrow 14."""
    import pyarrow.dataset as ds

    backend.table("datasets").to_parquet_dir(tmp_path)
    assert ds.dataset(tmp_path, format="parquet").to_table().to_pydict() == QUERY_RESULT.to_pydict()


@pytest.mark.skipif(not hasattr(pa.RecordBatchReader, "cast"), reason="PyArrow version does not support reader.cast")
def test_cast_reader_closes_query(client: NominalClient) -> None:
    """A lazily cast reader retains ownership and cancels the query on early close."""
    call = query_call(arrow_ipc_bytes(QUERY_RESULT))
    client._clients.sql.Query.side_effect = None
    client._clients.sql.Query.return_value = call
    reader = nibis.connect(client).table("datasets").to_pyarrow_batches().cast(QUERY_RESULT.schema)
    with reader:
        assert reader.read_next_batch().num_rows == 1
    call.cancel.assert_called_once_with()


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

    call = MagicMock()
    call.__iter__.return_value = failing_query(sql_pb2.SqlServiceQueryRequest())
    client._clients.sql.Query.side_effect = None
    client._clients.sql.Query.return_value = call
    with pytest.raises(NominalInvalidArgumentError, match="unknown column"):
        backend.table("datasets").select("name").to_pandas()
    call.cancel.assert_called_once_with()


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
@pytest.mark.parametrize(
    "names",
    [["name", "dataset_rid"], ["first", "second"], ["dataset_rid"], ["dataset_rid", "name", "extra_sort_key"]],
)
@pytest.mark.parametrize("empty", [False, True])
def test_unexpected_columns_are_not_reordered_or_renamed(output: str, names: list[str], empty: bool) -> None:
    """All output paths reject columns that do not match the requested projection."""
    con = nibis.connect(
        make_client(pa.table({name: pa.array([] if empty else ["value"], type=pa.string()) for name in names}))
    )
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


@pytest.mark.parametrize(
    ("scalar", "expected"),
    [
        ("VARCHAR", dt.string),
        ("BOOLEAN", dt.boolean),
        ("INTEGER", dt.int32),
        ("BIGINT", dt.int64),
        ("DOUBLE", dt.float64),
        ("TIMESTAMP", dt.Timestamp(scale=9)),
    ],
)
def test_catalog_scalar_types(client: NominalClient, scalar: str, expected: dt.DataType) -> None:
    client._clients.sql.GetSqlCatalog.return_value.sql_catalog.CopyFrom(
        sql_pb2.SqlCatalog(tables=[sql_pb2.SqlCatalogTable(name="typed", columns=[column("value", scalar, True)])])
    )
    assert nibis.connect(client).table("typed").schema()["value"] == expected


def test_nested_catalog_types_round_trip() -> None:
    string = sql_pb2.SqlCatalogDataType(scalar=sql_pb2.SQL_CATALOG_SCALAR_TYPE_VARCHAR)
    links = sql_pb2.SqlCatalogDataType(
        array_element=sql_pb2.SqlCatalogDataType(map=sql_pb2.SqlCatalogMapType(key=string, value=string))
    )
    catalog = sql_pb2.SqlCatalog(
        tables=[
            sql_pb2.SqlCatalogTable(
                name="assets",
                columns=[sql_pb2.SqlCatalogColumn(name="links", type="ARRAY", data_type=links, nullable=True)],
            )
        ]
    )
    result = pa.table(
        {"links": [[[("url", "https://nominal.io"), ("label", "Nominal")]]]},
        schema=pa.schema([pa.field("links", pa.list_(pa.map_(pa.string(), pa.string())))]),
    )
    client = make_client(result)
    client._clients.sql.GetSqlCatalog.return_value.sql_catalog.CopyFrom(catalog)
    expr = nibis.connect(client).table("assets")
    assert expr.schema()["links"] == dt.Array(dt.Map(dt.string, dt.string))
    assert expr.to_pyarrow().equals(result)


@pytest.mark.parametrize(
    ("data_type", "message"),
    [
        (sql_pb2.SqlCatalogDataType(), "Missing or unsupported catalog data_type"),
        (
            sql_pb2.SqlCatalogDataType(scalar=sql_pb2.SQL_CATALOG_SCALAR_TYPE_UNSPECIFIED),
            "Unsupported catalog scalar type",
        ),
        (sql_pb2.SqlCatalogDataType(scalar=999), "Unsupported catalog scalar type"),
        (
            sql_pb2.SqlCatalogDataType(
                map=sql_pb2.SqlCatalogMapType(
                    key=sql_pb2.SqlCatalogDataType(scalar=sql_pb2.SQL_CATALOG_SCALAR_TYPE_ANY),
                    value=sql_pb2.SqlCatalogDataType(scalar=sql_pb2.SQL_CATALOG_SCALAR_TYPE_VARCHAR),
                )
            ),
            "Catalog type ANY",
        ),
    ],
)
def test_unsupported_catalog_type_only_blocks_its_table(
    client: NominalClient, data_type: sql_pb2.SqlCatalogDataType, message: str
) -> None:
    catalog = client._clients.sql.GetSqlCatalog.return_value.sql_catalog
    catalog.tables.add(
        name="points_struct", columns=[sql_pb2.SqlCatalogColumn(name="value", type="MAP", data_type=data_type)]
    )
    con = nibis.connect(client)
    assert con.list_tables() == ["datasets", "points_double", "points_struct"]
    assert con.table("datasets").columns == ("dataset_rid", "name")
    with pytest.raises(nibis.NominalSqlError, match=message + ".*points_struct.value"):
        con.table("points_struct")


@pytest.mark.parametrize("output", ["pandas", "arrow", "batches"])
def test_points_struct_value_is_json_text(output: str) -> None:
    value = {"location": {"lat": 42.5}, "tags": ["flight", "test"], "count": 10, "valid": True}
    text = json.dumps(value)
    client = make_client(pa.table({"value": pa.array([text, None], type=pa.string())}))
    any_type = sql_pb2.SqlCatalogDataType(scalar=sql_pb2.SQL_CATALOG_SCALAR_TYPE_ANY)
    client._clients.sql.GetSqlCatalog.return_value.sql_catalog.tables.add(
        name="points_struct",
        columns=[
            sql_pb2.SqlCatalogColumn(
                name="value",
                type="MAP",
                nullable=True,
                data_type=sql_pb2.SqlCatalogDataType(map=sql_pb2.SqlCatalogMapType(key=any_type, value=any_type)),
            )
        ],
    )
    con = nibis.connect(client)
    expr = con.table("points_struct").select("value")
    assert expr.schema()["value"] == dt.string
    assert con.table("points_double").schema()["tags"].is_map()
    assert "CAST" not in con.compile(expr)
    if output == "pandas":
        values = expr.to_pandas()["value"].tolist()
    elif output == "arrow":
        values = expr.to_pyarrow()["value"].to_pylist()
    else:
        with expr.to_pyarrow_batches() as reader:
            values = reader.read_all()["value"].to_pylist()
    assert values == [text, None]
    assert json.loads(values[0]) == value
