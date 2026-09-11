from __future__ import annotations

import functools
import io
from typing import Any, Iterator, Mapping

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import pyarrow as pa
import sqlglot.expressions as sge
from ibis.backends import NoUrl
from ibis.backends.sql import SQLBackend
from ibis.backends.sql.compilers.postgres import PostgresCompiler
from ibis.formats.pandas import PandasData
from ibis.formats.pyarrow import PyArrowSchema

from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.client import NominalClient
from nominal.protos.sql.v1 import sql_pb2, sql_pb2_grpc

__all__ = ["Backend", "NominalSqlError", "connect"]

_CATALOG_SCALAR_TYPES: dict[int, dt.DataType] = {
    sql_pb2.SQL_CATALOG_SCALAR_TYPE_TIMESTAMP: dt.Timestamp(scale=9),
    sql_pb2.SQL_CATALOG_SCALAR_TYPE_DOUBLE: dt.Float64(),
    sql_pb2.SQL_CATALOG_SCALAR_TYPE_BIGINT: dt.Int64(),
    sql_pb2.SQL_CATALOG_SCALAR_TYPE_INTEGER: dt.Int32(),
    sql_pb2.SQL_CATALOG_SCALAR_TYPE_BOOLEAN: dt.Boolean(),
    sql_pb2.SQL_CATALOG_SCALAR_TYPE_VARCHAR: dt.String(),
}


class NominalSqlError(com.IbisError):
    """A catalog type or query result that cannot be represented by this backend."""


def _catalog_type(data_type: sql_pb2.SqlCatalogDataType, column: str) -> dt.DataType:
    kind = data_type.WhichOneof("kind")
    if kind == "scalar":
        if data_type.scalar == sql_pb2.SQL_CATALOG_SCALAR_TYPE_ANY:
            raise NominalSqlError(
                f"Catalog type ANY for {column} has no concrete Ibis type; "
                "use con.sql() with an explicitly typed projection"
            )
        dtype = _CATALOG_SCALAR_TYPES.get(data_type.scalar)
        if dtype is not None:
            return dtype
        raise NominalSqlError(
            f"Unsupported catalog scalar type {data_type.scalar} for {column}; "
            "use con.sql() with an explicitly typed projection"
        )
    if kind == "array_element":
        return dt.Array(_catalog_type(data_type.array_element, column))
    if kind == "map":
        return dt.Map(_catalog_type(data_type.map.key, column), _catalog_type(data_type.map.value, column))
    raise NominalSqlError(
        f"Missing or unsupported catalog data_type for {column}; the server must provide recursive column types"
    )


class NominalCompiler(PostgresCompiler):
    """Postgres-flavored SQL adjusted for the Nominal SQL API's dialect."""

    # Excluding RegexSearch keeps our visit_RegexSearch from being overwritten
    # by the generated simple-op impl, whose "regexp_like" sqlglot renders as
    # the ~ operator, which the API rejects.
    SIMPLE_OPS = {op: name for op, name in PostgresCompiler.SIMPLE_OPS.items() if op is not ops.RegexSearch}

    def to_sqlglot(
        self,
        expr: ir.Expr,
        *,
        limit: str | None = None,
        params: Mapping[ir.Expr, Any] | None = None,
    ) -> Any:
        # Postgres casts map/JSON outputs to strings for its driver. Our Arrow
        # transport preserves these types, so bypass that preprocessing.
        return super(PostgresCompiler, self).to_sqlglot(expr, limit=limit, params=params)

    def visit_MapGet(self, op: ops.MapGet, *, arg: Any, key: Any, default: Any) -> Any:
        item = sge.Bracket(this=arg, expressions=[key])
        if default is None:
            return item
        return self.f.coalesce(item, default)

    def _anon_agg(self, name: str, *args: Any, where: Any = None) -> Any:
        func = self.f.anon[name](*args)
        if where is not None:
            return sge.Filter(this=func, expression=sge.Where(this=where))
        return func

    def visit_ArgMax(self, op: ops.ArgMax, *, arg: Any, key: Any, where: Any) -> Any:
        # Anonymous rendering: sqlglot canonicalizes max_by to ARG_MAX, which the API rejects.
        return self._anon_agg("max_by", arg, key, where=where)

    def visit_ArgMin(self, op: ops.ArgMin, *, arg: Any, key: Any, where: Any) -> Any:
        return self._anon_agg("min_by", arg, key, where=where)

    def visit_RegexSearch(self, op: ops.RegexSearch, *, arg: Any, pattern: Any) -> Any:
        return self.f.anon.regexp_like(arg, pattern)

    @staticmethod
    def _minimize_spec(op: ops.WindowFunction, spec: Any) -> Any:
        # The API rejects ROW/RANGE frames on RANK/ROW_NUMBER/LAG/LEAD.
        if isinstance(op.func, ops.Analytic) and not isinstance(op.func, (ops.First, ops.Last, ops.NthValue)):
            return None
        return spec


class _PayloadReader(io.RawIOBase):
    """File view over the Arrow IPC stream the server splits across streamed response payloads."""

    def __init__(self, responses: Iterator[sql_pb2.SqlServiceQueryResponse]) -> None:
        self._responses = responses
        self._pending = memoryview(b"")

    def readable(self) -> bool:
        return True

    def readinto(self, target: Any) -> int:
        while not self._pending:
            with translate_grpc_errors():
                response = next(self._responses, None)
            if response is None:
                return 0
            self._pending = memoryview(response.payload)
        count = min(len(target), len(self._pending))
        target[:count] = self._pending[:count]
        self._pending = self._pending[count:]
        return count


class Backend(SQLBackend, NoUrl):
    """Ibis backend executing queries against the Nominal SQL API."""

    name = "nominal"
    compiler = NominalCompiler()
    supports_temporary_tables = False
    supports_python_udfs = False

    workspace_rid: str

    def do_connect(self, client: NominalClient) -> None:
        """Query the Nominal SQL API through an existing client.

        Args:
            client: Authenticated Nominal client. Queries run in its default workspace, either the one
                pinned in the profile or the tenant default.
        """
        self._sql: sql_pb2_grpc.SqlServiceStub = client._clients.sql
        self.workspace_rid = client._clients.resolve_default_workspace_rid()
        self.__dict__.pop("_catalog", None)

    @functools.cached_property
    def _catalog(self) -> sql_pb2.SqlCatalog:
        with translate_grpc_errors():
            return self._sql.GetSqlCatalog(sql_pb2.GetSqlCatalogRequest()).sql_catalog

    def list_tables(self, *, like: str | None = None, database: tuple[str, str] | str | None = None) -> list[str]:
        return self._filter_with_like(sorted(table.name for table in self._catalog.tables), like)

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        for table in self._catalog.tables:
            if table.name == table_name:
                return sch.Schema(
                    {
                        column.name: _catalog_type(column.data_type, f"{table_name}.{column.name}").copy(
                            nullable=column.nullable
                        )
                        for column in table.columns
                    }
                )
        raise com.TableNotFound(table_name)

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        with self._open_stream(query, max_rows=1) as reader:
            return PyArrowSchema.to_ibis(reader.schema)

    @property
    def version(self) -> str:
        return "1"

    def _open_stream(self, sql: str, max_rows: int | None = None) -> pa.ipc.RecordBatchStreamReader:
        request = sql_pb2.SqlServiceQueryRequest(
            query=sql,
            workspace_rid=self.workspace_rid,
            result_format=sql_pb2.SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM,
        )
        if max_rows is not None:
            request.max_rows = max_rows
        with translate_grpc_errors():
            responses = self._sql.Query(request)
        return pa.ipc.open_stream(io.BufferedReader(_PayloadReader(iter(responses))))

    def raw_sql(self, query: str) -> pa.Table:
        with self._open_stream(query) as reader:
            return reader.read_all()

    def _cast_result(self, result: pa.Table, target: pa.Schema) -> pa.Table:
        expected = target.names
        if result.column_names != expected:
            raise NominalSqlError(f"Server returned columns {result.column_names}, expected {expected}")
        try:
            return result.cast(target)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
            raise NominalSqlError(
                f"query result schema {result.schema} is not castable to the expression schema {target}"
            ) from e

    def _to_pyarrow_table(
        self,
        table_expr: ir.Table,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
    ) -> pa.Table:
        sql = self.compile(table_expr, params=params, limit=limit)
        result = self.raw_sql(sql)
        return self._cast_result(result, table_expr.schema().to_pyarrow())

    def to_pyarrow(
        self,
        expr: ir.Expr,
        /,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        **kwargs: Any,
    ) -> pa.Table | pa.Array | pa.Scalar:
        self._run_pre_execute_hooks(expr)
        table = self._to_pyarrow_table(expr.as_table(), params=params, limit=limit)
        return expr.__pyarrow_result__(table)

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        /,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        chunk_size: int = 1_000_000,
        **kwargs: Any,
    ) -> pa.ipc.RecordBatchReader:
        """Execute the expression, streaming record batches without materializing the result."""
        self._run_pre_execute_hooks(expr)
        table_expr = expr.as_table()
        reader = self._open_stream(self.compile(table_expr, params=params, limit=limit))
        target = table_expr.schema().to_pyarrow()

        def converted_batches() -> Iterator[pa.RecordBatch]:
            with reader:
                for batch in reader:
                    table = self._cast_result(pa.Table.from_batches([batch]), target)
                    yield from table.to_batches(max_chunksize=chunk_size)

        return pa.RecordBatchReader.from_batches(target, converted_batches())

    def execute(
        self,
        expr: ir.Expr,
        /,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        **kwargs: Any,
    ) -> Any:
        self._run_pre_execute_hooks(expr)
        table_expr = expr.as_table()
        table = self._to_pyarrow_table(table_expr, params=params, limit=limit)
        df = PandasData.convert_table(table.to_pandas(timestamp_as_object=False), table_expr.schema())
        return expr.__pandas_result__(df)

    def create_table(self, *args: Any, **kwargs: Any) -> ir.Table:
        raise com.UnsupportedOperationError("The Nominal SQL API is read-only")

    def drop_table(self, *args: Any, **kwargs: Any) -> None:
        raise com.UnsupportedOperationError("The Nominal SQL API is read-only")

    def create_view(self, *args: Any, **kwargs: Any) -> ir.Table:
        raise com.UnsupportedOperationError("The Nominal SQL API is read-only")

    def drop_view(self, *args: Any, **kwargs: Any) -> None:
        raise com.UnsupportedOperationError("The Nominal SQL API is read-only")

    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        raise com.UnsupportedOperationError("In-memory tables cannot be uploaded to the Nominal SQL API")

    def disconnect(self) -> None:
        """No-op: the gRPC channel belongs to the NominalClient."""


def connect(client: NominalClient) -> Backend:
    """Connect Ibis to the Nominal SQL API through an existing client; see `Backend.do_connect`."""
    backend = Backend(client)
    backend.reconnect()
    return backend
