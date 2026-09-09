"""Polars convenience wrapper for SQL queries."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from nominal.core import NominalClient

if TYPE_CHECKING:
    import polars as pl


def query_sql_to_dataframe(
    client: NominalClient,
    query: str,
    *,
    max_rows: int | None = None,
    workspace_rid: str | None = None,
) -> pl.DataFrame:
    """Run a read-only SQL query against Nominal telemetry tables and return the result as a polars DataFrame.

    Args:
        client: The NominalClient to query with.
        query: The SQL query text.
        max_rows: Optional cap on rows returned (1..5000).
        workspace_rid: Workspace to scope the query to. Defaults to the client's resolved default workspace.

    Returns:
        Query results as a polars `DataFrame`.

    Raises:
        conjure_python_client.ConjureHTTPError: On query failure.
    """
    import polars as pl
    from nominal.experimental.sql import query_sql

    # query_sql always returns pa.Table, so pl.from_arrow always returns DataFrame
    return cast(pl.DataFrame, pl.from_arrow(query_sql(client, query, max_rows=max_rows, workspace_rid=workspace_rid)))
