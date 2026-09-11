from __future__ import annotations

import functools
import io
import logging
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

logger = logging.getLogger(__name__)

# Element types of MAP and ARRAY columns are not reported by the catalog; the
# API's telemetry and metadata tables use string elements throughout.
_CATALOG_TYPES: dict[str, dt.DataType] = {
    "TIMESTAMP": dt.Timestamp(scale=9),
    "DOUBLE": dt.Float64(),
    "BIGINT": dt.Int64(),
    "VARCHAR": dt.String(),
    "MAP": dt.Map(dt.string, dt.string),
    "ARRAY": dt.Array(dt.string),
}


class NominalSqlError(com.IbisError):
    """A query result that cannot be mapped onto the Ibis expression that produced it."""


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
        for cached in ("_catalog", "_schemas"):
            self.__dict__.pop(cached, None)

    @functools.cached_property
    def _catalog(self) -> sql_pb2.SqlCatalog:
        with translate_grpc_errors():
            return self._sql.GetSqlCatalog(sql_pb2.GetSqlCatalogRequest()).sql_catalog

    @functools.cached_property
    def _schemas(self) -> dict[str, sch.Schema]:
        schemas: dict[str, sch.Schema] = {}
        for table in self._catalog.tables:
            fields: dict[str, dt.DataType] = {}
            for column in table.columns:
                dtype = _CATALOG_TYPES.get(column.type)
                if dtype is None:
                    logger.warning(
                        "unknown catalog type %r for column %s.%s; treating it as a string",
                        column.type,
                        table.name,
                        column.name,
                    )
                    dtype = dt.string
                fields[column.name] = dtype.copy(nullable=column.nullable)
            schemas[table.name] = sch.Schema(fields)
        return schemas

    def list_tables(self, *, like: str | None = None, database: tuple[str, str] | str | None = None) -> list[str]:
        return self._filter_with_like(sorted(self._schemas), like)

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        if table_name not in self._schemas:
            raise com.TableNotFound(table_name)
        return self._schemas[table_name]

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

    def _align_columns(self, result: pa.Table, expected: list[str]) -> pa.Table:
        """Project the server result onto the expression's output columns.

        The server may append ORDER BY sort keys to the projection; requested
        columns keep their aliases, so they are selected back by name.
        """
        names = result.column_names
        if len(names) < len(expected):
            raise NominalSqlError(f"Server returned columns {names}, expected {expected}")
        if names == expected:
            return result
        if names[: len(expected)] == expected:
            return result.select(list(range(len(expected))))
        if all(names.count(name) == 1 for name in expected):
            return result.select(expected)
        if len(names) == len(expected):
            return result.rename_columns(expected)
        raise NominalSqlError(f"Cannot map server columns {names} onto expected columns {expected}")

    def _cast_result(self, result: pa.Table, target: pa.Schema) -> pa.Table:
        try:
            return result.cast(target)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            logger.warning(
                "query result schema %s is not castable to the expression schema %s; returning server types",
                result.schema,
                target,
            )
            return result

    def _to_pyarrow_table(
        self,
        table_expr: ir.Table,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
    ) -> pa.Table:
        sql = self.compile(table_expr, params=params, limit=limit)
        result = self._align_columns(self.raw_sql(sql), list(table_expr.columns))
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
        expected_names = list(table_expr.columns)
        target = table_expr.schema().to_pyarrow()

        def aligned_batches() -> Iterator[pa.RecordBatch]:
            with reader:
                for batch in reader:
                    table = self._align_columns(pa.Table.from_batches([batch]), expected_names)
                    try:
                        table = table.cast(target)
                    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
                        raise NominalSqlError(
                            f"query result schema {table.schema} is not castable to the expression schema {target}"
                        ) from e
                    yield from table.to_batches(max_chunksize=chunk_size)

        return pa.RecordBatchReader.from_batches(target, aligned_batches())

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
