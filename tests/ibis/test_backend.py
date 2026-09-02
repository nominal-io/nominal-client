from __future__ import annotations

import io
from typing import Any
from unittest.mock import MagicMock

import ibis.common.exceptions as com
import pyarrow as pa
import pytest

import nominal.ibis as nibis

CATALOG_JSON = {
    "sqlCatalog": {
        "tables": [
            {
                "name": "points_double",
                "columns": [
                    {"name": "ts", "type": "TIMESTAMP"},
                    {"name": "value", "type": "DOUBLE", "nullable": True},
                    {"name": "channel", "type": "VARCHAR"},
                    {"name": "dataset_rid", "type": "VARCHAR"},
                    {"name": "tags", "type": "MAP"},
                ],
            },
            {
                "name": "datasets",
                "columns": [
                    {"name": "dataset_rid", "type": "VARCHAR"},
                    {"name": "name", "type": "VARCHAR"},
                ],
            },
        ],
        "functions": [{"name": "AVG"}],
    }
}

QUERY_RESULT = pa.table({"dataset_rid": ["ri.catalog.x.dataset.1"], "name": ["flight"], "extra_sort_key": [1]})


def arrow_ipc_bytes(table: pa.Table) -> bytes:
    """Serialize a table as a single Arrow IPC stream, as the query endpoint returns."""
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def fake_response(*, json_data: Any = None, content: bytes = b"", status: int = 200) -> MagicMock:
    """Stub of the requests.Response surface the backend uses."""
    response = MagicMock()
    response.status_code = status
    response.ok = status < 400
    response.content = content
    response.raw = io.BytesIO(content)
    response.text = ""
    if json_data is None:
        response.json.side_effect = ValueError("no json")
    else:
        response.json.return_value = json_data
    return response


def make_session(query_result: pa.Table = QUERY_RESULT) -> MagicMock:
    """Mock session answering the workspace, catalog, and query endpoints."""
    session = MagicMock()
    session.headers = {}

    def get(url: str, **kwargs: Any) -> MagicMock:
        if url.endswith("/workspaces/v1/default-workspace"):
            return fake_response(json_data={"rid": "ri.security.x.workspace.1"}, content=b"{}")
        if url.endswith("/sql/v1/catalog"):
            return fake_response(json_data=CATALOG_JSON)
        raise AssertionError(f"unexpected GET {url}")

    def post(url: str, **kwargs: Any) -> MagicMock:
        assert url.endswith("/sql/v1/query")
        return fake_response(content=arrow_ipc_bytes(query_result))

    session.get.side_effect = get
    session.post.side_effect = post
    return session


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = make_session()
    monkeypatch.setattr("nominal.ibis._backend.requests.Session", MagicMock(return_value=mock))
    return mock


@pytest.fixture
def backend(session: MagicMock) -> nibis.Backend:
    return nibis.connect(token="test-token", base_url="https://api.test/api")


def test_connect_sets_auth_and_user_agent_headers(backend: nibis.Backend, session: MagicMock) -> None:
    """The session authenticates with the bearer token and identifies the client."""
    assert session.headers["Authorization"] == "Bearer test-token"
    assert session.headers["User-Agent"].startswith("nominal-python/")


def test_default_workspace_resolved_from_api(backend: nibis.Backend) -> None:
    """Without an explicit workspace, the tenant default-workspace endpoint decides."""
    assert backend.workspace_rid == "ri.security.x.workspace.1"


def test_no_default_workspace_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty default-workspace response asks the caller to pass workspace_rid."""
    mock = make_session()
    mock.get.side_effect = lambda url, **kwargs: fake_response(status=204)
    monkeypatch.setattr("nominal.ibis._backend.requests.Session", MagicMock(return_value=mock))
    with pytest.raises(nibis.NominalSqlError, match="workspace_rid"):
        nibis.connect(token="test-token", base_url="https://api.test/api")


def test_explicit_workspace_skips_lookup(session: MagicMock) -> None:
    """A caller-supplied workspace_rid is used without calling the workspace endpoint."""
    con = nibis.connect(token="test-token", base_url="https://api.test/api", workspace_rid="ri.security.x.workspace.9")
    assert con.workspace_rid == "ri.security.x.workspace.9"
    session.get.assert_not_called()


def test_list_tables_from_catalog(backend: nibis.Backend) -> None:
    """Table names come from the SQL catalog endpoint."""
    assert backend.list_tables() == ["datasets", "points_double"]


def test_schema_types_from_catalog(backend: nibis.Backend) -> None:
    """Catalog logical types map onto Ibis types, keeping per-column nullability."""
    schema = backend.table("points_double").schema()
    assert schema["ts"].is_timestamp()
    assert schema["value"].is_float64()
    assert schema["value"].nullable
    assert not schema["channel"].nullable
    assert schema["tags"].is_map()


def test_unknown_table_raises(backend: nibis.Backend) -> None:
    """Tables absent from the catalog raise TableNotFound."""
    with pytest.raises(com.TableNotFound):
        backend.table("nope")


def test_query_request_carries_workspace_and_format(backend: nibis.Backend, session: MagicMock) -> None:
    """Query requests send the workspace RID and ask for the Arrow stream format."""
    backend.table("datasets").select("dataset_rid", "name").to_pandas()
    body = session.post.call_args.kwargs["json"]
    assert body["workspaceRid"] == "ri.security.x.workspace.1"
    assert body["resultFormat"] == "SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM"


def test_raw_sql_schema_probe_sets_max_rows(backend: nibis.Backend, session: MagicMock) -> None:
    """con.sql() infers the result schema from a single-row probe."""
    backend.sql("SELECT dataset_rid, name, extra_sort_key FROM datasets")
    body = session.post.call_args.kwargs["json"]
    assert body["maxRows"] == 1


def test_execute_drops_leaked_sort_key_columns(backend: nibis.Backend) -> None:
    """Extra server-appended sort-key columns are dropped; requested columns are selected by name."""
    df = backend.table("datasets").select("dataset_rid", "name").to_pandas()
    assert list(df.columns) == ["dataset_rid", "name"]
    assert df["name"][0] == "flight"


def test_fewer_columns_than_requested_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A response missing requested columns raises a diagnosable error, not an index error."""
    mock = make_session(query_result=pa.table({"dataset_rid": ["ri.catalog.x.dataset.1"]}))
    monkeypatch.setattr("nominal.ibis._backend.requests.Session", MagicMock(return_value=mock))
    con = nibis.connect(token="test-token", base_url="https://api.test/api")
    with pytest.raises(nibis.NominalSqlError, match="expected"):
        con.table("datasets").select("dataset_rid", "name").to_pandas()


def test_to_pyarrow_batches_streams_aligned_batches(backend: nibis.Backend, session: MagicMock) -> None:
    """Batch execution streams the HTTP response and yields aligned, typed batches."""
    expr = backend.table("datasets").select("dataset_rid", "name")
    with expr.to_pyarrow_batches() as reader:
        table = reader.read_all()
    assert table.column_names == ["dataset_rid", "name"]
    assert session.post.call_args.kwargs["stream"] is True


def test_write_operations_are_rejected(backend: nibis.Backend) -> None:
    """The SQL API is read-only, so DDL raises UnsupportedOperationError."""
    with pytest.raises(com.UnsupportedOperationError):
        backend.create_table("t", schema={"a": "int64"})


def test_http_errors_surface_details(monkeypatch: pytest.MonkeyPatch) -> None:
    """API error bodies are included in the raised NominalSqlError."""
    mock = make_session()
    mock.get.side_effect = lambda url, **kwargs: fake_response(
        json_data={"errorName": "SqlErrorInvalidQuery"}, status=400
    )
    monkeypatch.setattr("nominal.ibis._backend.requests.Session", MagicMock(return_value=mock))
    with pytest.raises(nibis.NominalSqlError, match="SqlErrorInvalidQuery"):
        nibis.connect(token="test-token", base_url="https://api.test/api")


def test_module_imports_cleanly_in_fresh_interpreter() -> None:
    """Connecting with nominal.ibis as the first nominal import must not trip the config/core import cycle."""
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from nominal.ibis import Backend; b = Backend(); "
            "b.do_connect(token='x', base_url='https://api.test', workspace_rid='ri.x.y.workspace.1')",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
