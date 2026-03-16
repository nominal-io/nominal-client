"""Unit tests for the experimental multiplayer workbook client.

These tests focus on behavior at the API boundary:

- transport bootstrap and sync handshake behavior
- lifecycle transitions, disconnect classification, and reconnect semantics
- callback-thread safety for blocking public methods
- high-level plain-Python workbook mutation and notebook replacement flows
"""

from __future__ import annotations

import asyncio
import base64
import copy
import io
import json
import threading
import time
import urllib.error
from email.message import Message
from types import SimpleNamespace
from typing import Any, Callable, cast

import pytest

pytest.importorskip("pycrdt")
pytest.importorskip("websockets")

from nominal_api import scout_notebook_api
from pycrdt import Doc, Map

from nominal.experimental.multiplayer import (
    DEFAULT_SCHEMA_VERSION,
    ConnectionState,
    InteractiveWorkbookSession,
    MultiplayerAuthError,
    MultiplayerError,
    MultiplayerFatalError,
    MultiplayerSyncTimeoutError,
    NotebookDeserializeError,
    NotebookSerializeError,
    WorkbookSession,
    prompt_for_user_jwt,
)
from nominal.experimental.multiplayer._session import (
    DEFAULT_SYNC_TIMEOUT,
    _app_base_url_to_multiplayer_ws_url,
    _build_room_url,
    _decode_message_header,
    _decode_varuint,
    _encode_message,
    _get_ws_auth_subprotocol,
    _MsgType,
    _read_clipboard_text,
    _SyncType,
)
from nominal.experimental.multiplayer._workbook import _to_yjs

WORKBOOK_RID = "ri.scout.main.notebook.1234"


def _sample_snapshot() -> dict[str, Any]:
    """Return a representative frontend-shaped multiplayer room snapshot."""
    return {
        "snapshotRid": "ri.scout.main.snapshot.0001",
        "workbook": {
            "rid": WORKBOOK_RID,
            "metadata": {"title": "Original Title"},
            "layout": {"tabs": []},
            "content": {
                "charts": {},
                "channelVariables": {},
                "eventRefs": [],
                "checkAlertRefs": [],
                "settings": {},
                "inputs": {},
                "dataScopeInputs": {},
            },
        },
    }


def _make_user_jwt(*, user_uuid: str = "user-123", organization_uuid: str = "org-456") -> str:
    """Return a JWT-shaped token with the Nominal user claims Scout expects."""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode()).decode().rstrip("=")
    payload = base64.urlsafe_b64encode(
        json.dumps({"nominal": {"user_uuid": user_uuid, "organization_uuid": organization_uuid}}).encode()
    ).decode().rstrip("=")
    return f"{header}.{payload}.signature"


def _sample_notebook() -> scout_notebook_api.Notebook:
    """Return a representative conjure notebook object for serialize tests."""
    return scout_notebook_api.Notebook(
        rid=WORKBOOK_RID,
        snapshot_rid="ri.scout.main.snapshot.0001",
        snapshot_author_rid="ri.authn.main.user.1",
        snapshot_created_at=cast(Any, None),
        metadata=cast(Any, None),
        state_as_json="{}",
        charts=None,
        layout=cast(Any, None),
        content=None,
        content_v2=cast(Any, None),
        event_refs=[],
        check_alert_refs=[],
    )


def _seed_doc(doc: Doc[Any], snapshot: dict[str, Any]) -> None:
    """Populate a pycrdt document with a detached snapshot."""
    with doc.transaction():
        root = doc.get(WORKBOOK_RID, type=Map)
        root["snapshotRid"] = snapshot["snapshotRid"]
        root["workbook"] = _to_yjs(copy.deepcopy(snapshot["workbook"]))


def _to_native(value: Any) -> Any:
    """Convert pycrdt values into ordinary Python structures."""
    if hasattr(value, "to_py"):
        return value.to_py()
    if isinstance(value, dict):
        return {key: _to_native(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_native(item) for item in value]
    return value


class _FakeLowLevelSession:
    """Small high-level-session test double for plain-Python state flows."""

    def __init__(self, snapshot: dict[str, Any]) -> None:
        self._state = copy.deepcopy(snapshot)
        self._update_callbacks: list[Callable[[dict[str, Any]], None]] = []
        self.closed = False
        self.state = ConnectionState.SYNCED
        self.is_connected = True
        self.is_synced = True
        self.last_close_code: int | None = None
        self.last_close_reason: str | None = None

    def get_state(self) -> dict[str, Any]:
        """Return a detached copy of the stored room state."""
        return copy.deepcopy(self._state)

    def mutate(self, fn: Callable[[dict[str, Any]], None]) -> None:
        """Apply a plain-Python mutation to the fake room state."""
        fn(self._state)
        self._state = _to_native(self._state)

    def on_update(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Register an update callback."""
        self._update_callbacks.append(callback)

    def on_state_change(self, callback: Callable[[ConnectionState, ConnectionState], None]) -> None:
        """Store the lifecycle callback for parity with the real API."""
        self._state_callback = callback

    def on_disconnect(self, callback: Callable[[int | None, str | None], None]) -> None:
        """Store the disconnect callback for parity with the real API."""
        self._disconnect_callback = callback

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """Store the reconnect callback for parity with the real API."""
        self._reconnect_callback = callback

    def on_fatal_error(self, callback: Callable[[BaseException], None]) -> None:
        """Store the fatal-error callback for parity with the real API."""
        self._fatal_callback = callback

    def emit_update(self) -> None:
        """Emit a stored update to all registered callbacks."""
        state = self.get_state()
        for callback in self._update_callbacks:
            callback(state)

    def reconnect(self) -> None:
        """Mirror the real API by keeping the fake session synced."""
        self.state = ConnectionState.SYNCED

    def close(self) -> None:
        """Record that the fake session was closed."""
        self.closed = True


class _FakeWebSocket:
    """In-memory websocket double that records frames and can be closed on demand."""

    def __init__(self, incoming: list[bytes] | None = None) -> None:
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        for item in incoming or []:
            self._queue.put_nowait(item)
        self.sent: list[bytes] = []
        self.closed = False
        self.close_code: int | None = None
        self.close_reason: str | None = None

    async def send(self, data: bytes) -> None:
        """Record an outbound websocket frame."""
        self.sent.append(data)

    async def recv(self) -> bytes:
        """Yield the next inbound frame or fail once the socket is closed."""
        while True:
            if not self._queue.empty():
                return self._queue.get_nowait()
            if self.closed:
                raise EOFError("websocket is closed")
            await asyncio.sleep(0.01)

    async def close(self) -> None:
        """Close the websocket with a default normal-close code."""
        if self.closed:
            return
        self.trigger_close(code=self.close_code or 1000, reason=self.close_reason or "closed")

    def queue_message(self, data: bytes) -> None:
        """Add an inbound frame to the receive queue."""
        self._queue.put_nowait(data)

    def trigger_close(self, *, code: int, reason: str) -> None:
        """Mark the websocket closed with the given close metadata."""
        self.closed = True
        self.close_code = code
        self.close_reason = reason


class _ConnectFactory:
    """Fake `websockets.connect` callable with queued outcomes."""

    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, url: str, *, subprotocols: list[Any]) -> _FakeWebSocket:
        """Return the next queued websocket outcome."""
        self.calls.append({"url": url, "subprotocols": subprotocols})
        if not self.outcomes:
            raise AssertionError("No fake websocket outcome available for connect()")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return cast(_FakeWebSocket, outcome)


def _wait_until(predicate: Callable[[], bool], timeout: float = 1.0) -> None:
    """Poll until a predicate becomes true or raise on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


@pytest.fixture
def workbook_stub() -> SimpleNamespace:
    """Return a minimal workbook object with the client context needed by create()."""
    return SimpleNamespace(
        rid=WORKBOOK_RID,
        _get_latest_api=lambda: _sample_notebook(),
        _clients=SimpleNamespace(
            auth_header="Bearer token-123",
            app_base_url="https://app.gov.nominal.io",
            notebook=SimpleNamespace(_uri="https://api.gov.nominal.io/api"),
        ),
    )


@pytest.fixture
def sync_done_message() -> bytes:
    """Return a multiplayer SyncDone frame for the test workbook RID."""
    return _encode_message(WORKBOOK_RID, _MsgType.SYNC_DONE)


@pytest.fixture
def fast_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Patch asyncio.sleep in the session module to advance immediately."""
    original_sleep = asyncio.sleep
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        await original_sleep(0)

    monkeypatch.setattr("nominal.experimental.multiplayer._session.asyncio.sleep", fake_sleep)
    return sleep_calls


def test_build_room_url_appends_the_schema_version_query() -> None:
    """The derived websocket room URL should include the schema version query string."""
    assert _app_base_url_to_multiplayer_ws_url("https://app.gov.nominal.io") == "wss://api.gov.nominal.io/multiplayer"
    assert (
        _build_room_url("wss://api.gov.nominal.io/multiplayer", WORKBOOK_RID, DEFAULT_SCHEMA_VERSION)
        == f"wss://api.gov.nominal.io/multiplayer/{WORKBOOK_RID}?schemaVersion={DEFAULT_SCHEMA_VERSION}"
    )


def test_create_derives_transport_context_from_the_workbook(workbook_stub: SimpleNamespace, monkeypatch) -> None:
    """`WorkbookSession.create()` should derive URL and RID while preserving the explicit access token."""
    captured: dict[str, Any] = {}

    def fake_init(
        self,
        ws_base_url,
        workbook_rid,
        access_token,
        *,
        timeout=DEFAULT_SYNC_TIMEOUT,
        connection_timeout=10.0,
        sync_timeout=30.0,
        reconnect=True,
        schema_version=DEFAULT_SCHEMA_VERSION,
    ) -> None:
        captured.update(
            ws_base_url=ws_base_url,
            workbook_rid=workbook_rid,
            access_token=access_token,
            timeout=timeout,
            connection_timeout=connection_timeout,
            sync_timeout=sync_timeout,
            reconnect=reconnect,
            schema_version=schema_version,
        )

    monkeypatch.setattr(WorkbookSession, "__init__", fake_init)

    WorkbookSession.create(
        cast(Any, workbook_stub),
        access_token="access-token-123",
        connection_timeout=12.5,
        sync_timeout=45.0,
        schema_version="5.0.0",
    )

    assert captured["ws_base_url"] == "wss://api.gov.nominal.io/multiplayer"
    assert captured["workbook_rid"] == WORKBOOK_RID
    assert captured["access_token"] == "access-token-123"
    assert captured["timeout"] == DEFAULT_SYNC_TIMEOUT
    assert captured["connection_timeout"] == 12.5
    assert captured["sync_timeout"] == 45.0
    assert captured["schema_version"] == "5.0.0"


def test_interactive_create_passes_the_default_initial_timeout(workbook_stub: SimpleNamespace, monkeypatch) -> None:
    """`InteractiveWorkbookSession.create()` should use the same concrete create timeout as the low-level layer."""
    captured: dict[str, Any] = {}

    def fake_create(
        workbook,
        *,
        access_token,
        timeout,
        ws_base_url,
        connection_timeout,
        sync_timeout,
        reconnect,
        schema_version,
    ) -> _FakeLowLevelSession:
        captured.update(
            workbook=workbook,
            access_token=access_token,
            timeout=timeout,
            ws_base_url=ws_base_url,
            connection_timeout=connection_timeout,
            sync_timeout=sync_timeout,
            reconnect=reconnect,
            schema_version=schema_version,
        )
        return _FakeLowLevelSession(_sample_snapshot())

    monkeypatch.setattr(WorkbookSession, "create", staticmethod(fake_create))

    session = InteractiveWorkbookSession.create(cast(Any, workbook_stub), access_token="access-token-123")

    assert isinstance(session, InteractiveWorkbookSession)
    assert captured["workbook"] is workbook_stub
    assert captured["access_token"] == "access-token-123"
    assert captured["timeout"] == DEFAULT_SYNC_TIMEOUT


def test_access_token_validation_rejects_api_keys() -> None:
    """The constructor should reject API keys and direct callers toward personal access tokens."""
    with pytest.raises(ValueError, match="24 hour personal access token"):
        WorkbookSession(
            ws_base_url="wss://api.gov.nominal.io/multiplayer",
            workbook_rid=WORKBOOK_RID,
            access_token="nominal_api_key_123",
        )


def test_prompt_for_user_jwt_opens_the_tokens_page_and_returns_the_first_valid_clipboard_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper should keep polling until the clipboard contains a valid user JWT."""
    clipboard_values = iter(["", "not-a-jwt", f"Bearer {_make_user_jwt()}"])
    opened_urls: list[str] = []
    sleep_calls: list[float] = []

    monkeypatch.setattr(
        "nominal.experimental.multiplayer._session._read_clipboard_text",
        lambda: next(clipboard_values),
    )
    def fake_open(url: str, new: int, autoraise: bool) -> bool:
        del new, autoraise
        opened_urls.append(url)
        return True

    monkeypatch.setattr("nominal.experimental.multiplayer._session.webbrowser.open", fake_open)
    monkeypatch.setattr(
        "nominal.experimental.multiplayer._session.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    token = prompt_for_user_jwt("https://app.gov.nominal.io", timeout=1.0, poll_interval=0.1)

    assert token == _make_user_jwt()
    assert opened_urls == ["https://app.gov.nominal.io/settings/user/tokens"]
    assert sleep_calls == [0.1, 0.1]


def test_prompt_for_user_jwt_times_out_when_no_valid_clipboard_token_appears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper should raise a clear auth error when the clipboard never contains a valid JWT."""
    monotonic_values = iter([100.0, 100.0, 100.2, 100.2])

    monkeypatch.setattr("nominal.experimental.multiplayer._session._read_clipboard_text", lambda: "api-key")
    monkeypatch.setattr(
        "nominal.experimental.multiplayer._session.webbrowser.open",
        lambda url, new, autoraise: True,
    )
    monkeypatch.setattr("nominal.experimental.multiplayer._session.time.monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr("nominal.experimental.multiplayer._session.time.sleep", lambda seconds: None)

    with pytest.raises(MultiplayerAuthError, match="Timed out waiting for a valid user JWT"):
        prompt_for_user_jwt("https://app.gov.nominal.io", timeout=0.1, poll_interval=0.05)


def test_read_clipboard_text_uses_pyperclip() -> None:
    """Clipboard reads should delegate to pyperclip so platform handling stays centralized."""

    class _FakePyperclip:
        PyperclipException = RuntimeError

        @staticmethod
        def paste() -> str:
            return "token-from-clipboard"

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("nominal.experimental.multiplayer._session._load_pyperclip", lambda: _FakePyperclip())
    try:
        assert _read_clipboard_text() == "token-from-clipboard"
    finally:
        monkeypatch.undo()


def test_read_clipboard_text_raises_a_multiplayer_auth_error_when_pyperclip_fails() -> None:
    """Clipboard read failures should surface as multiplayer auth errors with clear guidance."""

    class _FakePyperclip:
        PyperclipException = RuntimeError

        @staticmethod
        def paste() -> str:
            raise RuntimeError("clipboard unavailable")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("nominal.experimental.multiplayer._session._load_pyperclip", lambda: _FakePyperclip())
    try:
        with pytest.raises(MultiplayerAuthError, match="Failed to read a JWT from the clipboard"):
            _read_clipboard_text()
    finally:
        monkeypatch.undo()


def test_initial_create_returns_a_synced_session_and_acknowledges_sync_done(
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful first handshake should produce a synced session and a SyncDone ack."""
    websocket = _FakeWebSocket([sync_done_message])
    factory = _ConnectFactory([websocket])
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", factory)

    session = WorkbookSession(
        ws_base_url="wss://api.gov.nominal.io/multiplayer",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        reconnect=False,
        timeout=1.0,
    )
    try:
        assert session.state is ConnectionState.SYNCED
        assert session.is_connected is True
        assert session.is_synced is True

        first_doc_name, first_msg_type, first_offset = _decode_message_header(websocket.sent[0])
        sync_type, _ = _decode_varuint(websocket.sent[0][first_offset:], 0)
        assert first_doc_name == WORKBOOK_RID
        assert first_msg_type == _MsgType.SYNC
        assert sync_type == _SyncType.STEP1

        second_doc_name, second_msg_type, _ = _decode_message_header(websocket.sent[1])
        assert second_doc_name == WORKBOOK_RID
        assert second_msg_type == _MsgType.SYNC_DONE
    finally:
        session.close()


def test_retryable_initial_sync_timeout_uses_the_concrete_create_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The constructor should fail the initial create wait after the configured concrete timeout."""

    async def hanging_connect(*_args, **_kwargs) -> _FakeWebSocket:
        await asyncio.sleep(10.0)
        return _FakeWebSocket()

    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", hanging_connect)

    with pytest.raises(MultiplayerSyncTimeoutError, match="initial multiplayer sync"):
        WorkbookSession(
            ws_base_url="wss://api.gov.nominal.io/multiplayer",
            workbook_rid=WORKBOOK_RID,
            access_token="token-123",
            connection_timeout=10.0,
            timeout=0.05,
        )


def test_sync_timeout_resends_step1_until_the_handshake_budget_is_exhausted(
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
    fast_sleep: list[float],
) -> None:
    """A stalled sync handshake should keep resending Step1 until the sync timeout expires."""
    del sync_done_message  # not used in this stalled-handshake test
    websocket = _FakeWebSocket()
    factory = _ConnectFactory([websocket])
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", factory)

    with pytest.raises(MultiplayerSyncTimeoutError):
        WorkbookSession(
            ws_base_url="wss://api.gov.nominal.io/multiplayer",
            workbook_rid=WORKBOOK_RID,
            access_token="token-123",
            reconnect=False,
            connection_timeout=0.1,
            sync_timeout=0.05,
            timeout=1.0,
        )

    assert websocket.sent
    assert fast_sleep


def test_retryable_disconnect_reconnects_with_the_same_access_token_and_fires_callbacks(
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
    fast_sleep: list[float],
) -> None:
    """A retryable close should reconnect, reuse the shared Doc, and preserve the explicit access token."""
    del fast_sleep
    first_socket = _FakeWebSocket([sync_done_message])
    second_socket = _FakeWebSocket([sync_done_message])
    factory = _ConnectFactory([first_socket, second_socket])
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", factory)

    session = WorkbookSession(
        ws_base_url="wss://api.gov.nominal.io/multiplayer",
        workbook_rid=WORKBOOK_RID,
        access_token="token-a",
        timeout=1.0,
    )
    try:
        disconnects: list[tuple[int | None, str | None]] = []
        reconnects: list[str] = []
        state_changes: list[tuple[ConnectionState, ConnectionState]] = []
        session.on_disconnect(lambda code, reason: disconnects.append((code, reason)))
        session.on_reconnect(lambda: reconnects.append("reconnected"))
        session.on_state_change(lambda state, previous: state_changes.append((state, previous)))

        first_socket.trigger_close(code=1011, reason="server restart")

        _wait_until(lambda: session.state is ConnectionState.SYNCED and len(factory.calls) == 2)

        assert disconnects == [(1011, "server restart")]
        assert reconnects == ["reconnected"]
        assert str(factory.calls[0]["subprotocols"][0]) == _get_ws_auth_subprotocol("token-a")
        assert str(factory.calls[1]["subprotocols"][0]) == _get_ws_auth_subprotocol("token-a")
        assert session.last_close_code == 1011
        assert session.last_close_reason == "server restart"
        assert (ConnectionState.RECONNECTING, ConnectionState.SYNCED) in state_changes
        assert (ConnectionState.SYNCING, ConnectionState.RECONNECTING) in state_changes
        assert (ConnectionState.SYNCED, ConnectionState.SYNCING) in state_changes
    finally:
        session.close()


def test_fatal_disconnect_transitions_to_fatal_without_retry(
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fatal close should stop the supervisor and surface a terminal error."""
    websocket = _FakeWebSocket([sync_done_message])
    factory = _ConnectFactory([websocket])
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", factory)

    session = WorkbookSession(
        ws_base_url="wss://api.gov.nominal.io/multiplayer",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        timeout=1.0,
    )
    fatal_errors: list[BaseException] = []
    session.on_fatal_error(fatal_errors.append)
    try:
        websocket.trigger_close(code=1008, reason="policy violation")

        _wait_until(lambda: session.state is ConnectionState.FATAL)

        assert len(factory.calls) == 1
        assert fatal_errors
        assert isinstance(fatal_errors[0], MultiplayerFatalError)
    finally:
        session.close()


def test_manual_reconnect_from_a_healthy_session_forces_a_new_connection(
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
    fast_sleep: list[float],
) -> None:
    """Calling `reconnect()` on a healthy session should close and replace the websocket connection."""
    del fast_sleep
    first_socket = _FakeWebSocket([sync_done_message])
    second_socket = _FakeWebSocket([sync_done_message])
    factory = _ConnectFactory([first_socket, second_socket])
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", factory)

    session = WorkbookSession(
        ws_base_url="wss://api.gov.nominal.io/multiplayer",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        timeout=1.0,
    )
    try:
        disconnects: list[tuple[int | None, str | None]] = []
        reconnects: list[str] = []
        session.on_disconnect(lambda code, reason: disconnects.append((code, reason)))
        session.on_reconnect(lambda: reconnects.append("yes"))

        session.reconnect()

        assert session.state is ConnectionState.SYNCED
        assert len(factory.calls) == 2
        assert disconnects == [(1000, "closed")]
        assert reconnects == ["yes"]
    finally:
        session.close()


def test_manual_reconnect_requested_while_connecting_still_forces_a_fresh_connection(
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
    fast_sleep: list[float],
) -> None:
    """A reconnect requested before the first socket exists should still yield a second fresh connection."""
    del fast_sleep
    first_connect_gate = threading.Event()
    session_holder: dict[str, WorkbookSession] = {}
    holder_ready = threading.Event()
    create_result: dict[str, Any] = {}
    first_socket = _FakeWebSocket([sync_done_message])
    second_socket = _FakeWebSocket([sync_done_message])
    connect_calls: list[str] = []

    original_supervisor_loop = WorkbookSession._supervisor_loop

    async def wrapped_supervisor_loop(self: WorkbookSession) -> None:
        session_holder["session"] = self
        holder_ready.set()
        await original_supervisor_loop(self)

    async def delayed_connect(url: str, *, subprotocols: list[Any]) -> _FakeWebSocket:
        del subprotocols
        connect_calls.append(url)
        if len(connect_calls) == 1:
            while not first_connect_gate.is_set():
                await asyncio.sleep(0.01)
            return first_socket
        return second_socket

    monkeypatch.setattr(WorkbookSession, "_supervisor_loop", wrapped_supervisor_loop)
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", delayed_connect)

    def create_session() -> None:
        try:
            create_result["session"] = WorkbookSession(
                ws_base_url="wss://api.gov.nominal.io/multiplayer",
                workbook_rid=WORKBOOK_RID,
                access_token="token-123",
                timeout=1.0,
            )
        except BaseException as exc:  # pragma: no cover - test failure path
            create_result["error"] = exc

    creator_thread = threading.Thread(target=create_session, daemon=True)
    creator_thread.start()
    assert holder_ready.wait(timeout=1.0)

    session = session_holder["session"]
    disconnects: list[tuple[int | None, str | None]] = []
    reconnects: list[str] = []
    session.on_disconnect(lambda code, reason: disconnects.append((code, reason)))
    session.on_reconnect(lambda: reconnects.append("reconnected"))

    reconnect_thread = threading.Thread(target=session.reconnect, daemon=True)
    reconnect_thread.start()
    _wait_until(lambda: session.state is ConnectionState.CONNECTING)
    first_connect_gate.set()

    creator_thread.join(timeout=2.0)
    reconnect_thread.join(timeout=2.0)

    assert "error" not in create_result
    assert reconnect_thread.is_alive() is False
    assert len(connect_calls) == 2
    assert disconnects == [(1000, "closed")]
    assert reconnects == ["reconnected"]

    session.close()


@pytest.mark.parametrize("method_name", ["mutate", "reconnect", "close"])
def test_blocking_public_methods_fail_fast_on_the_session_thread(
    method_name: str,
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blocking APIs should raise instead of deadlocking when invoked on the session thread."""
    websocket = _FakeWebSocket([sync_done_message])
    factory = _ConnectFactory([websocket])
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", factory)

    session = WorkbookSession(
        ws_base_url="wss://api.gov.nominal.io/multiplayer",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        timeout=1.0,
    )
    try:
        async def invoke() -> None:
            with pytest.raises(MultiplayerError, match="callback thread"):
                if method_name == "mutate":
                    session.mutate(lambda _root: None)
                elif method_name == "reconnect":
                    session.reconnect()
                else:
                    session.close()

        asyncio.run_coroutine_threadsafe(invoke(), session._loop).result(timeout=1.0)
    finally:
        session.close()


def test_close_transitions_to_closed_and_stops_future_retries(
    sync_done_message: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing the session should terminate the supervisor and leave the lifecycle in CLOSED."""
    websocket = _FakeWebSocket([sync_done_message])
    factory = _ConnectFactory([websocket])
    monkeypatch.setattr("nominal.experimental.multiplayer._session.websockets.connect", factory)

    session = WorkbookSession(
        ws_base_url="wss://api.gov.nominal.io/multiplayer",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        timeout=1.0,
    )

    session.close()
    assert session.state is ConnectionState.CLOSED


def test_interactive_session_uses_native_types_and_deduplicates_subscriptions() -> None:
    """High-level subscriptions should emit plain Python values and suppress unchanged repeats."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    observed: list[Any] = []
    session.subscribe(
        observed.append,
        selector=lambda snapshot: snapshot["workbook"]["content"]["eventRefs"],
        emit_initial=True,
    )

    def mutate(workbook: dict[str, Any]) -> None:
        workbook["content"]["eventRefs"].append("ri.scout.main.event.1")

    session.mutate_workbook(mutate)
    low_level.emit_update()
    low_level.emit_update()

    assert observed == [[], ["ri.scout.main.event.1"]]
    assert session.state is ConnectionState.SYNCED


def test_interactive_session_proxies_low_level_lifecycle_properties() -> None:
    """The high-level wrapper should expose the underlying lifecycle state verbatim."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    assert session.state is ConnectionState.SYNCED
    assert session.is_connected is True
    assert session.is_synced is True

    session.reconnect()
    assert low_level.state is ConnectionState.SYNCED


def test_replace_workbook_json_rejects_rid_mismatches() -> None:
    """Whole-workbook replacement should reject payloads for a different workbook RID."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    bad_workbook = session.get_workbook_json()
    bad_workbook["rid"] = "ri.scout.main.notebook.other"

    with pytest.raises(ValueError, match="Workbook RID mismatch"):
        session.replace_workbook_json(bad_workbook)


def test_deserialize_notebook_uses_the_app_deserialize_endpoint_and_returns_workbook_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Notebook deserialization should POST to the Scout app endpoint using the explicit access token."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    captured: dict[str, Any] = {}
    response_workbook = {
        "rid": WORKBOOK_RID,
        "metadata": {"title": "From Notebook"},
        "layout": {"tabs": []},
        "content": {
            "charts": {},
            "channelVariables": {},
            "eventRefs": ["ri.scout.main.event.1"],
            "checkAlertRefs": [],
            "settings": {},
            "inputs": {},
            "dataScopeInputs": {},
        },
    }

    class _FakeResponse:
        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(response_workbook).encode("utf-8")

    def fake_urlopen(request) -> _FakeResponse:
        captured["url"] = request.full_url
        captured["authorization"] = request.get_header("Authorization")
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse()

    monkeypatch.setattr("nominal.experimental.multiplayer._session.urllib.request.urlopen", fake_urlopen)

    notebook = {"rid": WORKBOOK_RID, "metadata": {"title": "Notebook"}}
    updated = session.deserialize_notebook(notebook)

    assert captured["url"] == "https://app.gov.nominal.io/api/workbooks/deserialize"
    assert captured["authorization"] == "Bearer token-123"
    assert captured["body"] == notebook
    assert updated["metadata"]["title"] == "From Notebook"


def test_replace_notebook_deserializes_and_then_updates_the_live_workbook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Notebook replacement should deserialize through Scout and then write the live workbook."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    response_workbook = {
        "rid": WORKBOOK_RID,
        "metadata": {"title": "From App Fallback"},
        "layout": {"tabs": []},
        "content": {
            "charts": {},
            "channelVariables": {},
            "eventRefs": [],
            "checkAlertRefs": [],
            "settings": {},
            "inputs": {},
            "dataScopeInputs": {},
        },
    }
    class _FakeResponse:
        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(response_workbook).encode("utf-8")

    def fake_urlopen(request):
        return _FakeResponse()

    monkeypatch.setattr("nominal.experimental.multiplayer._session.urllib.request.urlopen", fake_urlopen)

    updated = session.replace_notebook({"rid": WORKBOOK_RID})

    assert updated["metadata"]["title"] == "From App Fallback"
    assert session.get_workbook_json()["metadata"]["title"] == "From App Fallback"


@pytest.mark.parametrize(
    ("status_code", "expected_message"),
    [
        (401, "token is not valid"),
        (403, "access is denied"),
        (422, "request payload was invalid"),
        (500, "Notebook deserialization failed with HTTP 500"),
    ],
)
def test_replace_notebook_reports_http_failures_clearly(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    expected_message: str,
) -> None:
    """Deserialize HTTP errors should become clear, typed notebook-deserialize failures."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    def fake_urlopen(_request):
        raise urllib.error.HTTPError(
            url="https://app.gov.nominal.io/api/workbooks/deserialize",
            code=status_code,
            msg="boom",
            hdrs=Message(),
            fp=io.BytesIO(b""),
        )

    monkeypatch.setattr("nominal.experimental.multiplayer._session.urllib.request.urlopen", fake_urlopen)

    with pytest.raises(NotebookDeserializeError, match=expected_message):
        session.replace_notebook({"rid": WORKBOOK_RID})


def test_replace_notebook_reports_url_errors_as_notebook_deserialize_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transport-layer deserialize failures should become typed notebook-deserialize errors."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    def fake_urlopen(_request):
        raise urllib.error.URLError("network unavailable")

    monkeypatch.setattr("nominal.experimental.multiplayer._session.urllib.request.urlopen", fake_urlopen)

    with pytest.raises(NotebookDeserializeError, match="network unavailable"):
        session.replace_notebook({"rid": WORKBOOK_RID})


def test_serialize_workbook_json_uses_the_app_serialize_endpoint_and_returns_a_notebook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Workbook serialization should POST to the Scout app endpoint and rebuild a Notebook object."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    base_notebook = _sample_notebook()
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        notebook_supplier=lambda: base_notebook,
    )
    captured: dict[str, Any] = {}

    def fake_urlopen(request):
        captured["url"] = request.full_url
        captured["authorization"] = request.get_header("Authorization")
        captured["body"] = json.loads(request.data.decode("utf-8"))

        class _FakeResponse:
            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, exc_type, exc_val, exc_tb) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps({}).encode("utf-8")

        return _FakeResponse()

    monkeypatch.setattr("nominal.experimental.multiplayer._session.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "nominal.experimental.multiplayer._session.ConjureDecoder.do_decode",
        lambda _self, _payload, _obj_type: scout_notebook_api.UpdateNotebookRequest(
            state_as_json='{"hello": "world"}',
            charts=None,
            layout=cast(Any, None),
            content=None,
            content_v2=None,
            event_refs=[],
            check_alert_refs=[],
        ),
    )

    notebook = session.serialize_workbook_json()

    assert captured["url"] == "https://app.gov.nominal.io/api/workbooks/serialize"
    assert captured["authorization"] == "Bearer token-123"
    assert captured["body"]["latestSnapshotRid"] == "ri.scout.main.snapshot.0001"
    assert captured["body"]["workbook"]["rid"] == WORKBOOK_RID
    assert notebook.rid == WORKBOOK_RID
    assert notebook.state_as_json == '{"hello": "world"}'


def test_get_notebook_serializes_the_current_live_workbook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_notebook()` should serialize the current live workbook using the stored notebook supplier."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    base_notebook = _sample_notebook()
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        notebook_supplier=lambda: base_notebook,
    )

    monkeypatch.setattr(
        InteractiveWorkbookSession,
        "_serialize_workbook_json",
        lambda self, workbook, notebook: notebook,
    )

    assert session.get_notebook() is base_notebook


@pytest.mark.parametrize(
    ("status_code", "expected_message"),
    [
        (401, "token is not valid"),
        (403, "access is denied"),
        (422, "request payload was invalid"),
        (500, "Notebook serialization failed with HTTP 500"),
    ],
)
def test_serialize_workbook_json_reports_http_failures_clearly(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    expected_message: str,
) -> None:
    """Serialize HTTP errors should become clear, typed notebook-serialize failures."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
        notebook_supplier=_sample_notebook,
    )

    def fake_urlopen(_request):
        raise urllib.error.HTTPError(
            url="https://app.gov.nominal.io/api/workbooks/serialize",
            code=status_code,
            msg="boom",
            hdrs=Message(),
            fp=io.BytesIO(b""),
        )

    monkeypatch.setattr("nominal.experimental.multiplayer._session.urllib.request.urlopen", fake_urlopen)

    with pytest.raises(NotebookSerializeError, match=expected_message):
        session.serialize_workbook_json()


def test_serialize_workbook_json_requires_a_base_notebook_when_no_supplier_is_available() -> None:
    """Manual high-level sessions should require a base notebook or notebook supplier for serialize calls."""
    low_level = _FakeLowLevelSession(_sample_snapshot())
    session = InteractiveWorkbookSession(
        session=cast(WorkbookSession, low_level),
        app_base_url="https://app.gov.nominal.io",
        workbook_rid=WORKBOOK_RID,
        access_token="token-123",
    )

    with pytest.raises(NotebookSerializeError, match="no notebook supplier"):
        session.serialize_workbook_json()


def test_close_classification_distinguishes_auth_fatal_and_retryable_cases() -> None:
    """Close classification should separate auth/protocol failures from retryable transport errors."""
    session = WorkbookSession.__new__(WorkbookSession)

    auth_drop = session._connection_dropped_from_error(
        code=4403,
        reason="forbidden",
        error=None,
        should_notify_disconnect=True,
    )
    fatal_drop = session._connection_dropped_from_error(
        code=1008,
        reason="policy",
        error=None,
        should_notify_disconnect=True,
    )
    retryable_drop = session._connection_dropped_from_error(
        code=1011,
        reason="server restart",
        error=None,
        should_notify_disconnect=True,
    )

    assert auth_drop.disposition.value == "fatal"
    assert isinstance(auth_drop.error, MultiplayerAuthError)
    assert fatal_drop.disposition.value == "fatal"
    assert isinstance(session._coerce_fatal_error(auth_drop.error), MultiplayerAuthError)
    assert retryable_drop.disposition.value == "retryable"
