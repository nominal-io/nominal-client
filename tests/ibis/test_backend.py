from __future__ import annotations

import io
from typing import Any

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


def arrow_ipc_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


class FakeResponse:
    def __init__(self, *, json_data: Any = None, content: bytes = b"", status: int = 200) -> None:
        """Stub of the requests.Response surface the backend uses."""
        self._json = json_data
        self.content = content
        self.status_code = status
        self.ok = status < 400
        self.text = ""

    def json(self) -> Any:
        if self._json is None:
            raise ValueError("no json")
        return self._json


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> nibis.Backend:
    query_result = pa.table({"dataset_rid": ["ri.catalog.x.dataset.1"], "name": ["flight"], "extra_sort_key": [1]})

    def fake_get(self: Any, url: str, **kwargs: Any) -> FakeResponse:
        if url.endswith("/workspaces/v1/workspaces"):
            return FakeResponse(json_data=[{"rid": "ri.security.x.workspace.1"}])
        if url.endswith("/sql/v1/catalog"):
            return FakeResponse(json_data=CATALOG_JSON)
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(self: Any, url: str, **kwargs: Any) -> FakeResponse:
        assert url.endswith("/sql/v1/query")
        assert kwargs["json"]["workspaceRid"] == "ri.security.x.workspace.1"
        return FakeResponse(content=arrow_ipc_bytes(query_result))

    monkeypatch.setattr("requests.Session.get", fake_get)
    monkeypatch.setattr("requests.Session.post", fake_post)
    return nibis.connect(token="test-token", base_url="https://api.test/api")


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


def test_execute_drops_leaked_sort_key_columns(backend: nibis.Backend) -> None:
    # The server appends ORDER BY sort keys to the projection; the client
    # keeps only the requested columns.
    expr = backend.table("datasets").select("dataset_rid", "name")
    df = expr.to_pandas()
    assert list(df.columns) == ["dataset_rid", "name"]


def test_write_operations_are_rejected(backend: nibis.Backend) -> None:
    with pytest.raises(com.UnsupportedOperationError):
        backend.create_table("t", schema={"a": "int64"})


def test_default_workspace_resolved_from_api(backend: nibis.Backend) -> None:
    assert backend.workspace_rid == "ri.security.x.workspace.1"


def test_explicit_workspace_skips_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_get(self: Any, url: str, **kwargs: Any) -> FakeResponse:
        raise AssertionError("no GET expected")

    monkeypatch.setattr("requests.Session.get", fail_get)
    con = nibis.connect(token="test-token", base_url="https://api.test/api", workspace_rid="ri.security.x.workspace.9")
    assert con.workspace_rid == "ri.security.x.workspace.9"


def test_http_errors_surface_details(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(self: Any, url: str, **kwargs: Any) -> FakeResponse:
        return FakeResponse(json_data={"errorName": "SqlErrorInvalidQuery"}, status=400)

    monkeypatch.setattr("requests.Session.get", fake_get)
    with pytest.raises(nibis.NominalSqlError, match="SqlErrorInvalidQuery"):
        nibis.connect(token="test-token", base_url="https://api.test/api")
