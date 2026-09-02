"""Ibis backend for the Nominal SQL API.

Compiles Ibis expressions to Postgres-flavored SQL (the closest sqlglot dialect
to the Calcite parser behind the Nominal SQL API) and executes them over the
public REST endpoint, streaming results back as Arrow.

Example:
    import ibis
    from ibis import _

    con = ibis.nominal.connect()  # uses the "default" profile in the Nominal config
    pts = con.table("points_double")
    (
        pts.filter(_.dataset_rid == "ri.catalog....", _.channel == "temperature")
        .group_by(minute=_.ts.truncate("m"))
        .agg(n=_.count(), avg=_.value.mean())
        .to_pandas()
    )
"""

from __future__ import annotations

import io
import os
from typing import Any, Mapping
from urllib.parse import ParseResult

import pyarrow as pa
import requests
import sqlglot.expressions as sge

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import nominal.core  # noqa: F401  # nominal.config circularly imports nominal.core; initialize core first
from ibis.backends.sql import SQLBackend
from ibis.backends.sql.compilers.postgres import PostgresCompiler
from ibis.formats.pandas import PandasData
from ibis.formats.pyarrow import PyArrowSchema
from nominal.config import NominalConfig
from nominal.core._constants import DEFAULT_API_BASE_URL

__all__ = ["Backend", "NominalSqlError", "connect"]

_ARROW_FORMAT = "SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM"

_CATALOG_TYPES: dict[str, dt.DataType] = {
    "TIMESTAMP": dt.Timestamp(scale=9),
    "DOUBLE": dt.Float64(),
    "BIGINT": dt.Int64(),
    "VARCHAR": dt.String(),
    "MAP": dt.Map(dt.string, dt.string),
    "ARRAY": dt.Array(dt.string),
}


class NominalSqlError(com.IbisError):
    """Error returned by the Nominal SQL API."""


class NominalCompiler(PostgresCompiler):
    """Postgres-flavored SQL tweaked for the Calcite parser behind the Nominal SQL API."""

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
        # Skip PostgresCompiler's cast of map/json output columns to VARCHAR
        # (a psycopg workaround); the API returns maps natively as Arrow maps.
        return super(PostgresCompiler, self).to_sqlglot(expr, limit=limit, params=params)

    def visit_MapGet(self, op: ops.MapGet, *, arg: Any, key: Any, default: Any) -> Any:
        # Calcite's native ITEM syntax, m['k'], instead of Postgres jsonb operators.
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
        # Calcite rejects ROW/RANGE frames on RANK/ROW_NUMBER/LAG/LEAD.
        if isinstance(op.func, ops.Analytic) and not isinstance(op.func, (ops.First, ops.Last, ops.NthValue)):
            return None
        return spec


class Backend(SQLBackend):
    """Ibis backend executing queries against the Nominal SQL API."""

    name = "nominal"
    compiler = NominalCompiler()
    supports_temporary_tables = False
    supports_python_udfs = False

    base_url: str
    workspace_rid: str

    def do_connect(
        self,
        profile: str | None = None,
        *,
        base_url: str | None = None,
        token: str | None = None,
        workspace_rid: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> None:
        """Connect to the Nominal SQL API.

        Args:
            profile: Named profile in the Nominal config (see `nom config profile add`).
                Used when no token is given; defaults to the `NOMINAL_PROFILE`
                environment variable, then to "default".
            base_url: API base URL; overrides the profile's URL when given.
            token: API key or auth token. When given, the Nominal config is not read.
            workspace_rid: Workspace to query in; overrides the profile's workspace.
                When neither is set, the caller's first workspace is used.
            timeout_seconds: HTTP timeout for catalog and query requests.
        """
        if token is None:
            prof = NominalConfig.from_yaml().get_profile(profile or os.environ.get("NOMINAL_PROFILE", "default"))
            base_url = base_url or prof.base_url
            token = prof.token
            workspace_rid = workspace_rid or prof.workspace_rid
        self.base_url = (base_url or DEFAULT_API_BASE_URL).rstrip("/")
        self._timeout = timeout_seconds
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {token}"
        self._catalog_cache: dict[str, sch.Schema] | None = None
        self.workspace_rid = workspace_rid or self._default_workspace_rid()

    def _from_url(self, url: ParseResult, **kwargs: Any) -> "Backend":
        self.do_connect(base_url=f"https://{url.netloc}{url.path}".rstrip("/"), **kwargs)
        return self

    def _raise_for_error(self, response: requests.Response) -> None:
        if response.ok:
            return
        try:
            detail: object = response.json()
        except ValueError:
            detail = response.text[:2000]
        raise NominalSqlError(f"HTTP {response.status_code}: {detail}")

    def _default_workspace_rid(self) -> str:
        response = self._session.get(f"{self.base_url}/workspaces/v1/workspaces", timeout=self._timeout)
        self._raise_for_error(response)
        workspaces = response.json()
        if not workspaces:
            raise NominalSqlError("Caller belongs to no workspaces; pass workspace_rid")
        return str(workspaces[0]["rid"])

    # -- catalog / schema ---------------------------------------------------

    def _fetch_catalog(self) -> dict[str, sch.Schema]:
        if self._catalog_cache is None:
            response = self._session.get(f"{self.base_url}/sql/v1/catalog", timeout=self._timeout)
            self._raise_for_error(response)
            tables = (response.json().get("sqlCatalog") or {}).get("tables") or []
            catalog: dict[str, sch.Schema] = {}
            for table in tables:
                fields: dict[str, dt.DataType] = {}
                for column in table.get("columns") or []:
                    dtype = _CATALOG_TYPES.get(column["type"], dt.string)
                    fields[column["name"]] = dtype.copy(nullable=bool(column.get("nullable", False)))
                catalog[table["name"]] = sch.Schema(fields)
            self._catalog_cache = catalog
        return self._catalog_cache

    def list_tables(self, *, like: str | None = None, database: tuple[str, str] | str | None = None) -> list[str]:
        return self._filter_with_like(sorted(self._fetch_catalog()), like)

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        schemas = self._fetch_catalog()
        if table_name not in schemas:
            raise com.TableNotFound(table_name)
        return schemas[table_name]

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        table = self._execute_sql(query, max_rows=1)
        return PyArrowSchema.to_ibis(table.schema)

    @property
    def version(self) -> str:
        return "1"

    # -- execution ----------------------------------------------------------

    def _execute_sql(self, sql: str, max_rows: int | None = None) -> pa.Table:
        body: dict[str, Any] = {
            "query": sql,
            "workspaceRid": self.workspace_rid,
            "resultFormat": _ARROW_FORMAT,
        }
        if max_rows is not None:
            body["maxRows"] = max_rows
        response = self._session.post(f"{self.base_url}/sql/v1/query", json=body, timeout=self._timeout)
        self._raise_for_error(response)
        with pa.ipc.open_stream(io.BytesIO(response.content)) as reader:
            return reader.read_all()

    def raw_sql(self, query: str) -> pa.Table:
        return self._execute_sql(query)

    def _to_pyarrow_table(
        self,
        table_expr: ir.Table,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
    ) -> pa.Table:
        sql = self.compile(table_expr, params=params, limit=limit)
        result = self._execute_sql(sql)
        expected = list(table_expr.columns)
        if result.num_columns != len(expected):
            # The server appends ORDER BY sort keys to the projection; the
            # requested columns always come first, in order.
            result = result.select(list(range(len(expected))))
        result = result.rename_columns(expected)
        try:
            return result.cast(table_expr.schema().to_pyarrow())
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            return result

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
        self._run_pre_execute_hooks(expr)
        table = self._to_pyarrow_table(expr.as_table(), params=params, limit=limit)
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=chunk_size))

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

    # -- read-only stubs ----------------------------------------------------

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
        self._session.close()


def connect(
    profile: str | None = None,
    *,
    base_url: str | None = None,
    token: str | None = None,
    workspace_rid: str | None = None,
    timeout_seconds: float = 120.0,
) -> Backend:
    """Connect to the Nominal SQL API; see `Backend.do_connect` for arguments."""
    backend = Backend()
    backend.do_connect(
        profile,
        base_url=base_url,
        token=token,
        workspace_rid=workspace_rid,
        timeout_seconds=timeout_seconds,
    )
    return backend
