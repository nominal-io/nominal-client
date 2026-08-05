from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureHTTPError
from requests.exceptions import HTTPError

from nominal.core.client import NominalClient
from nominal.experimental.sql import (
    SqlCatalogColumn,
    SqlCatalogFunction,
    export_sql,
    get_sql_catalog,
    query_sql,
)

pa = pytest.importorskip("pyarrow")


def _arrow_ipc_bytes(table: pa.Table) -> bytes:
    """Serialize an Arrow table to IPC format."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def test_query_sql_round_trips_arrow_payload_and_resolves_default_workspace() -> None:
    """Test that query_sql parses Arrow IPC payload and uses default workspace when none is passed."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.main.workspace.default"
    client = NominalClient(_clients=clients)

    table = pa.table({"dataset_rid": ["ri.a", "ri.b"], "name": ["a", "b"]})
    clients.sql.query.return_value = _arrow_ipc_bytes(table)

    result = query_sql(client, "SELECT dataset_rid, name FROM datasets LIMIT 20")

    assert result.equals(table)
    clients.sql.query.assert_called_once_with(
        "Bearer token", "ri.workspace.main.workspace.default", "SELECT dataset_rid, name FROM datasets LIMIT 20", None
    )
    clients.resolve_default_workspace_rid.assert_called_once()


def test_query_sql_passes_explicit_workspace_and_max_rows() -> None:
    """Test that query_sql does not call resolve_default_workspace_rid when workspace_rid is explicit."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    client = NominalClient(_clients=clients)

    table = pa.table({"x": [1]})
    clients.sql.query.return_value = _arrow_ipc_bytes(table)

    query_sql(client, "SELECT 1", max_rows=100, workspace_rid="ri.workspace.explicit")

    clients.sql.query.assert_called_once_with("Bearer token", "ri.workspace.explicit", "SELECT 1", 100)
    clients.resolve_default_workspace_rid.assert_not_called()


def test_query_sql_calls_with_default_workspace_when_workspace_rid_none() -> None:
    """Test that query_sql explicitly passes None for workspace_rid in service call when max_rows is not set."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    client = NominalClient(_clients=clients)

    table = pa.table({"x": [1]})
    clients.sql.query.return_value = _arrow_ipc_bytes(table)

    query_sql(client, "SELECT 1")

    clients.sql.query.assert_called_once_with("Bearer token", "ri.workspace.default", "SELECT 1", None)


def test_export_sql_returns_presigned_url() -> None:
    """Test that export_sql extracts and returns the presigned_url from the response."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    client = NominalClient(_clients=clients)

    clients.sql.export.return_value = {
        "presignedUrl": "https://example.com/result.parquet",
        "query_id": "q-1",
    }

    url = export_sql(client, "SELECT * FROM datasets")

    assert url == "https://example.com/result.parquet"
    clients.sql.export.assert_called_once_with("Bearer token", "ri.workspace.default", "SELECT * FROM datasets")


def test_export_sql_with_explicit_workspace() -> None:
    """Test that export_sql uses explicit workspace_rid when provided."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    client = NominalClient(_clients=clients)

    clients.sql.export.return_value = {"presignedUrl": "https://example.com/result.csv", "query_id": "q-2"}

    export_sql(client, "SELECT COUNT(*) FROM datasets", workspace_rid="ri.workspace.explicit")

    clients.sql.export.assert_called_once_with("Bearer token", "ri.workspace.explicit", "SELECT COUNT(*) FROM datasets")
    clients.resolve_default_workspace_rid.assert_not_called()


def test_get_sql_catalog_parses_tables_and_functions() -> None:
    """Test that get_sql_catalog parses the JSON response into SqlCatalog objects."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    client = NominalClient(_clients=clients)

    clients.sql.get_sql_catalog.return_value = {
        "sqlCatalog": {
            "tables": [
                {
                    "name": "datasets",
                    "columns": [
                        {"name": "dataset_rid", "type": "string", "nullable": False},
                        {"name": "name", "type": "varchar", "nullable": True},
                    ],
                },
                {"name": "channels", "columns": [{"name": "channel_rid", "type": "string", "nullable": False}]},
            ],
            "functions": [{"name": "now"}, {"name": "count"}],
        }
    }

    catalog = get_sql_catalog(client)

    assert len(catalog.tables) == 2
    assert catalog.tables[0].name == "datasets"
    assert len(catalog.tables[0].columns) == 2
    assert catalog.tables[0].columns[0] == SqlCatalogColumn(name="dataset_rid", type="string", nullable=False)
    assert catalog.tables[0].columns[1] == SqlCatalogColumn(name="name", type="varchar", nullable=True)
    assert catalog.tables[1].name == "channels"
    assert len(catalog.functions) == 2
    assert catalog.functions[0] == SqlCatalogFunction(name="now")


def test_get_sql_catalog_handles_empty_catalog() -> None:
    """Test that get_sql_catalog handles an empty catalog with no tables or functions."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    client = NominalClient(_clients=clients)

    clients.sql.get_sql_catalog.return_value = {"sqlCatalog": {"tables": [], "functions": []}}

    catalog = get_sql_catalog(client)

    assert catalog.tables == []
    assert catalog.functions == []


def test_query_sql_propagates_conjure_http_error() -> None:
    """Test that ConjureHTTPError from the service propagates unchanged through query_sql."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    client = NominalClient(_clients=clients)

    # Construct a real ConjureHTTPError with a minimal fake requests.Response
    fake_response = MagicMock()
    fake_response.status_code = 400
    fake_response.json.return_value = {
        "errorCode": "INVALID_ARGUMENT",
        "errorName": "Invalid query syntax",
        "errorInstanceId": "e-123",
        "parameters": {},
    }
    fake_response.headers.get.return_value = "trace-id-123"
    fake_response.request = MagicMock()

    http_error = HTTPError(response=fake_response)
    conjure_error = ConjureHTTPError(http_error)
    clients.sql.query.side_effect = conjure_error

    with pytest.raises(ConjureHTTPError) as exc_info:
        query_sql(client, "SELECT * FROM nonexistent_table")

    assert exc_info.value is conjure_error


def test_export_sql_propagates_conjure_http_error() -> None:
    """Test that ConjureHTTPError from export_sql propagates unchanged."""
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    client = NominalClient(_clients=clients)

    fake_response = MagicMock()
    fake_response.status_code = 412
    fake_response.json.return_value = {
        "errorCode": "FAILED_PRECONDITION",
        "errorName": "SQL export is not configured for this deployment",
        "errorInstanceId": "e-124",
        "parameters": {},
    }
    fake_response.headers.get.return_value = "trace-id-124"
    fake_response.request = MagicMock()

    http_error = HTTPError(response=fake_response)
    conjure_error = ConjureHTTPError(http_error)
    clients.sql.export.side_effect = conjure_error

    with pytest.raises(ConjureHTTPError) as exc_info:
        export_sql(client, "SELECT * FROM datasets")

    assert exc_info.value is conjure_error
