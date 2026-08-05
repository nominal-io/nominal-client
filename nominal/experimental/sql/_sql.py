"""SQL query API for Nominal telemetry tables.

This module provides a hand-maintained shim for the `SqlService` endpoint, standing in until
official generated bindings ship in `nominal-api-protos`. Once that lands, this module should be
deprecated and replaced with generated types.

Known limitations:

- `query_id` is not retrievable via `query_sql()` — the backend's HTTP mapping for the `Query` RPC
  returns only the raw Arrow payload, dropping the `query_id` field that is present in the proto.
  Use `export_sql()` if you need a query ID.

- JSON field casing (`workspace_rid`, `max_rows`, `sql_catalog`, etc.) is inferred from live testing
  against a deployed backend, not from official documentation or codegen. It should be re-verified
  once the backend team publishes the real generated bindings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from nominal.core import NominalClient

if TYPE_CHECKING:
    import pyarrow as pa  # type: ignore[import-untyped]


@dataclass(frozen=True)
class SqlCatalogColumn:
    """A column in a queryable SQL table."""

    name: str
    type: str
    nullable: bool


@dataclass(frozen=True)
class SqlCatalogTable:
    """A queryable SQL table."""

    name: str
    columns: list[SqlCatalogColumn]


@dataclass(frozen=True)
class SqlCatalogFunction:
    """A queryable SQL function."""

    name: str


@dataclass(frozen=True)
class SqlCatalog:
    """The catalog of tables, columns, and functions available to SQL queries."""

    tables: list[SqlCatalogTable]
    functions: list[SqlCatalogFunction]

    @classmethod
    def _from_json(cls, raw: dict[str, object]) -> SqlCatalog:
        catalog = raw["sqlCatalog"]
        if not isinstance(catalog, dict):
            raise ValueError(f"Expected sql_catalog to be a dict, got {type(catalog)}")

        tables = [
            SqlCatalogTable(
                name=t["name"],
                columns=[SqlCatalogColumn(**c) for c in t.get("columns", [])],
            )
            for t in catalog.get("tables", [])
        ]
        functions = [SqlCatalogFunction(name=f["name"]) for f in catalog.get("functions", [])]
        return cls(tables=tables, functions=functions)


def query_sql(
    client: NominalClient,
    query: str,
    *,
    max_rows: int | None = None,
    workspace_rid: str | None = None,
) -> pa.Table:
    """Run a read-only SQL query against Nominal telemetry tables and return the result as an Arrow table.

    Args:
        client: The NominalClient to query with.
        query: The SQL query text (max 100000 characters).
        max_rows: Optional cap on rows returned (1..5000). If omitted, the backend's default applies.
        workspace_rid: Workspace to scope the query to. Defaults to the client's resolved default workspace.

    Returns:
        Query results as a `pyarrow.Table`.

    Raises:
        conjure_python_client.ConjureHTTPError: On invalid query, missing datasets, execution failure,
            resource exhaustion, or timeout.
    """
    import pyarrow as pa

    resolved_workspace_rid = (
        workspace_rid if workspace_rid is not None else client._clients.resolve_default_workspace_rid()
    )
    payload = client._clients.sql.query(
        client._clients.auth_header,
        resolved_workspace_rid,
        query,
        max_rows,
    )
    return pa.ipc.open_stream(pa.py_buffer(payload)).read_all()


def export_sql(
    client: NominalClient,
    query: str,
    *,
    workspace_rid: str | None = None,
) -> str:
    """Run a read-only SQL query and return a presigned URL to download the full result set as CSV.

    Args:
        client: The NominalClient to query with.
        query: The SQL query text (max 100000 characters).
        workspace_rid: Workspace to scope the query to. Defaults to the client's resolved default workspace.

    Returns:
        A presigned URL for downloading the query result as CSV.

    Raises:
        conjure_python_client.ConjureHTTPError: On invalid query, missing datasets, execution failure,
            or if SQL export is not configured for this deployment.
    """
    resolved_workspace_rid = (
        workspace_rid if workspace_rid is not None else client._clients.resolve_default_workspace_rid()
    )
    response = client._clients.sql.export(
        client._clients.auth_header,
        resolved_workspace_rid,
        query,
    )
    return cast(str, response["presignedUrl"])


def get_sql_catalog(client: NominalClient) -> SqlCatalog:
    """Fetch the catalog of tables, columns, and functions available to SQL queries.

    Args:
        client: The NominalClient to use.

    Returns:
        The queryable SQL surface.

    Raises:
        conjure_python_client.ConjureHTTPError: On backend errors.
    """
    raw = client._clients.sql.get_sql_catalog(client._clients.auth_header)
    return SqlCatalog._from_json(raw)
