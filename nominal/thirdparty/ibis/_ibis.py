from __future__ import annotations

import logging
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import pyarrow as pa
import requests
from ibis.backends import BaseBackend
from typing_extensions import Self

from nominal._utils.dataclass_tools import LazyField
from nominal.core._clientsbunch import SqlService
from nominal.core.client import NominalClient

logger = logging.getLogger(__name__)

_DOWNLOAD_CHUNK_SIZE = 1024 * 1024

# Logical SQL type names emitted by the catalog endpoint, mapped to their ibis equivalents. The catalog does not
# carry element types for MAP and ARRAY columns, so those default to strings: cast in SQL when you need something
# more specific.
_CATALOG_TYPES: Mapping[str, dt.DataType] = {
    "ARRAY": dt.Array(dt.string),
    "BIGINT": dt.int64,
    "BOOLEAN": dt.boolean,
    "DECIMAL": dt.Decimal(),
    "DOUBLE": dt.float64,
    "INTEGER": dt.int32,
    "MAP": dt.Map(dt.string, dt.string),
    "TIMESTAMP": dt.timestamp,
    "VARCHAR": dt.string,
}

# Tables the SQL validator rejects without an explicit `dataset_rid` equality or IN filter. Mirrors scout's
# transpiler catalog until the catalog endpoint carries the flag itself.
DATASET_SCOPED_TABLES = frozenset({"channels", "logs", "points_double", "points_int", "points_string", "points_struct"})


@dataclass(frozen=True)
class SqlColumn:
    name: str
    type: str
    """Logical SQL type name, e.g. TIMESTAMP, DOUBLE, BIGINT, VARCHAR, MAP, or ARRAY."""
    nullable: bool

    @property
    def dtype(self) -> dt.DataType:
        """The ibis type this column maps to, defaulting to string for type names we don't recognize."""
        dtype = _CATALOG_TYPES.get(self.type.upper())
        if dtype is None:
            logger.warning("Unrecognized SQL type %r for column %r: treating it as a string", self.type, self.name)
            dtype = dt.string
        return dtype.copy(nullable=self.nullable)


@dataclass(frozen=True)
class SqlTable:
    name: str
    columns: Sequence[SqlColumn]

    @property
    def requires_dataset_rid_filter(self) -> bool:
        """Whether queries against this table must filter on `dataset_rid`"""
        return self.name in DATASET_SCOPED_TABLES

    def schema(self) -> ibis.Schema:
        """This table's columns as an ibis schema"""
        return ibis.Schema({column.name: column.dtype for column in self.columns})


@dataclass(frozen=True)
class SqlCatalog:
    """The queryable SQL surface: a fixed set of tables and the functions allowed within a query"""

    tables: Sequence[SqlTable]
    functions: Sequence[str]

    def get_table(self, name: str) -> SqlTable:
        for table in self.tables:
            if table.name == name:
                return table
        known = ", ".join(table.name for table in self.tables)
        raise ValueError(f"No such table {name!r} in the Nominal SQL catalog: expected one of {known}")


@dataclass(frozen=True)
class NominalSqlConnection:
    """Query Nominal's SQL interface, returning results as ibis tables.

    Queries execute server-side against Nominal's warehouse; each result is materialized into a local ibis
    backend (DuckDB by default) so it can be composed, joined against other results, and executed lazily like
    any other ibis table.

    The queryable surface is a fixed catalog rather than your own tables -- `points_double`, `points_int`,
    `points_string`, `points_struct`, `logs` and `channels` hold telemetry, and `assets`, `runs`, `run_assets`,
    `datasets` and `events` hold metadata. Queries against the telemetry tables must filter on `dataset_rid`.
    Use `list_tables()` and `get_schema()` to see the catalog as the server reports it.

    Example:
    -------
    ```
    conn = NominalSqlConnection.from_client(client)
    rpm = conn.sql(f'''
        SELECT channel, date_bin(INTERVAL '1' SECOND, ts) AS bucket, avg(value) AS value
        FROM points_double
        WHERE dataset_rid = '{dataset.rid}' AND channel LIKE 'engine%'
        GROUP BY channel, bucket
    ''')
    rpm.filter(rpm.value > 4000).order_by("bucket").to_pandas()
    ```

    """

    _sql: SqlService = field(repr=False)
    _auth_header: str = field(repr=False)
    workspace_rid: str
    backend: BaseBackend
    """The local ibis backend that query results are materialized into"""

    _catalog: LazyField[SqlCatalog] = field(default_factory=LazyField, init=False, repr=False, compare=False)
    _table_counter: Iterator[int] = field(default_factory=lambda: count(1), init=False, repr=False, compare=False)

    @classmethod
    def from_client(
        cls,
        client: NominalClient,
        *,
        workspace_rid: str | None = None,
        backend: BaseBackend | None = None,
    ) -> Self:
        """Create a SQL connection from an existing Nominal client.

        Args:
            client: Client to take credentials and the API base URL from.
            workspace_rid: Workspace to query. Defaults to the workspace the client is pinned to, or the
                tenant's default workspace.
            backend: ibis backend to materialize results into. Defaults to a new in-memory DuckDB backend;
                pass `ibis.duckdb.connect("results.ddb")` to keep results on disk instead.

        Returns:
            A connection to the Nominal SQL interface.
        """
        clients = client._clients
        return cls(
            _sql=clients.sql,
            _auth_header=clients.auth_header,
            workspace_rid=workspace_rid or clients.resolve_default_workspace_rid(),
            backend=backend if backend is not None else ibis.duckdb.connect(),
        )

    def catalog(self) -> SqlCatalog:
        """Get the queryable SQL surface: tables, their columns, and the allowed functions.

        The catalog is fixed for a given server, so it is fetched once and cached on this connection.
        """
        return self._catalog.get_or_init(self._fetch_catalog)

    def list_tables(self) -> list[str]:
        """List the tables that can be queried"""
        return [table.name for table in self.catalog().tables]

    def list_functions(self) -> list[str]:
        """List the functions allowed within a query"""
        return list(self.catalog().functions)

    def get_schema(self, table: str) -> ibis.Schema:
        """Get the schema of a table in the SQL catalog.

        Args:
            table: Name of the table, as returned by `list_tables()`.

        Returns:
            The table's columns as an ibis schema.
        """
        return self.catalog().get_table(table).schema()

    def to_pyarrow(self, query: str, *, max_rows: int | None = None) -> pa.Table:
        """Execute a query and return the result as an Arrow table, without materializing it into the backend.

        Args:
            query: SQL to execute against the Nominal catalog.
            max_rows: Cap on the number of rows returned, up to 5000. When unset, the result is bounded only by
                the server's byte and time limits; use `export()` for results larger than those allow.

        Returns:
            The query result as a pyarrow Table.
        """
        payload = self._sql.query(self._auth_header, query, self.workspace_rid, max_rows)
        if not payload:
            raise ValueError("SQL query returned an empty response instead of an Arrow stream")
        with pa.ipc.open_stream(pa.BufferReader(payload)) as reader:
            return reader.read_all()

    def sql(self, query: str, *, max_rows: int | None = None, name: str | None = None) -> ir.Table:
        """Execute a query and return the result as an ibis table.

        Args:
            query: SQL to execute against the Nominal catalog.
            max_rows: Cap on the number of rows returned, up to 5000. When unset, the result is bounded only by
                the server's byte and time limits; use `export()` for results larger than those allow.
            name: Name to give the result in the local backend, replacing any table already using it. Defaults
                to a generated name, so repeated calls do not clobber each other.

        Returns:
            An ibis table over the query result, held in this connection's backend.
        """
        return self._register(self.to_pyarrow(query, max_rows=max_rows), name)

    def export(self, query: str, path: str | Path, *, name: str | None = None) -> ir.Table:
        """Execute a query whose result is too large for `sql()`, streaming it to a local CSV file.

        The server writes the full result to object storage as CSV, which is downloaded to `path` and read
        lazily by the backend -- so unlike `sql()`, the rows are never all held in memory at once, and the
        file must stay on disk for as long as the returned table is used.

        Args:
            query: SQL to execute against the Nominal catalog.
            path: Local path to write the exported CSV to.
            name: Name to give the result in the local backend. Defaults to a generated name.

        Returns:
            An ibis table over the downloaded CSV.
        """
        response = self._sql.export(self._auth_header, query, self.workspace_rid)
        presigned_url = response["presignedUrl"]
        destination = Path(path)
        with requests.get(presigned_url, stream=True) as download:
            download.raise_for_status()
            with destination.open("wb") as csv_file:
                for chunk in download.iter_content(chunk_size=_DOWNLOAD_CHUNK_SIZE):
                    csv_file.write(chunk)
        return self.backend.read_csv(destination, table_name=name or self._next_table_name())

    def _fetch_catalog(self) -> SqlCatalog:
        # Proto3 JSON omits fields holding their default value, hence the `.get(..., default)` on every field.
        catalog: Mapping[str, Any] = self._sql.get_sql_catalog(self._auth_header).get("sqlCatalog", {})
        tables = [
            SqlTable(
                name=table.get("name", ""),
                columns=[
                    SqlColumn(
                        name=column.get("name", ""),
                        type=column.get("type", ""),
                        nullable=column.get("nullable", False),
                    )
                    for column in table.get("columns", [])
                ],
            )
            for table in catalog.get("tables", [])
        ]
        return SqlCatalog(
            tables=tables,
            functions=[function.get("name", "") for function in catalog.get("functions", [])],
        )

    def _register(self, results: pa.Table, name: str | None) -> ir.Table:
        return self.backend.create_table(name or self._next_table_name(), results, overwrite=True)

    def _next_table_name(self) -> str:
        return f"nominal_sql_{next(self._table_counter)}"
