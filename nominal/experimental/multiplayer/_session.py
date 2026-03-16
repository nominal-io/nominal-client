from __future__ import annotations

import asyncio
import base64
import binascii
import copy
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from enum import Enum, IntEnum
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, NoReturn, Protocol, cast

from conjure_python_client._serde.decoder import ConjureDecoder
from conjure_python_client._serde.encoder import ConjureEncoder
from nominal_api import scout_notebook_api

try:
    import websockets
    from pycrdt import Array, Doc, Map
    from websockets.asyncio.client import ClientConnection
    from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus, WebSocketException
    from websockets.typing import Subprotocol
except ModuleNotFoundError:
    raise ImportError("nominal[websockets] is required to use websocket-based multiplayer API")

try:
    import pyperclip  # type: ignore[import-untyped]
except ModuleNotFoundError:
    pyperclip = None

from nominal.experimental.multiplayer._workbook import _to_yjs, new_id

if TYPE_CHECKING:
    from nominal.core.workbook import Workbook


DEFAULT_SCHEMA_VERSION = "4.0.0"
DEFAULT_CONNECTION_TIMEOUT = 10.0
DEFAULT_SYNC_TIMEOUT = 30.0
DEFAULT_SYNC_RETRY_INTERVAL = 2.0
DEFAULT_RECONNECT_BACKOFFS = (1.0, 2.0, 5.0, 10.0)
DEFAULT_USER_JWT_TIMEOUT = 300.0
DEFAULT_USER_JWT_POLL_INTERVAL = 0.5
_SUBSCRIPTION_SENTINEL = object()


class MultiplayerError(RuntimeError):
    """Base class for multiplayer client errors."""


class MultiplayerSyncTimeoutError(MultiplayerError):
    """Raised when the websocket session fails to finish syncing in time."""


class MultiplayerAuthError(MultiplayerError):
    """Raised when authentication or authorization fails."""


class MultiplayerFatalError(MultiplayerError):
    """Raised when the session reaches a terminal, non-retryable failure state."""


class NotebookDeserializeError(MultiplayerError):
    """Raised when notebook-to-workbook deserialization fails."""


class NotebookSerializeError(MultiplayerError):
    """Raised when workbook-to-notebook serialization fails."""


class ConnectionState(Enum):
    """Connection lifecycle states for workbook multiplayer sessions."""

    CONNECTING = "connecting"
    SYNCING = "syncing"
    SYNCED = "synced"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"
    FATAL = "fatal"


def _encode_varuint(n: int) -> bytes:
    """Encode a non-negative integer using LEB128 variable-length encoding."""
    result = bytearray()
    while True:
        bits = n & 0x7F
        n >>= 7
        if n == 0:
            result.append(bits)
            break
        result.append(bits | 0x80)
    return bytes(result)


def _decode_varuint(data: bytes, offset: int) -> tuple[int, int]:
    """Decode a LEB128 variable-length unsigned integer."""
    result, shift = 0, 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, offset
        shift += 7


def _encode_varuint8array(data: bytes) -> bytes:
    """Encode a byte array as ``[length: varuint][bytes]``."""
    return _encode_varuint(len(data)) + data


def _decode_varuint8array(data: bytes, offset: int) -> tuple[bytes, int]:
    """Decode a length-prefixed byte array."""
    length, offset = _decode_varuint(data, offset)
    return data[offset : offset + length], offset + length


def _encode_varstring(s: str) -> bytes:
    """Encode a UTF-8 string as a length-prefixed byte array."""
    return _encode_varuint8array(s.encode("utf-8"))


def _decode_varstring(data: bytes, offset: int) -> tuple[str, int]:
    """Decode a length-prefixed UTF-8 string."""
    raw, offset = _decode_varuint8array(data, offset)
    return raw.decode("utf-8"), offset


def _encode_message(doc_name: str, msg_type: int, payload: bytes = b"") -> bytes:
    """Frame a multiplayer message as ``[docName: varstring][msgType: varuint][payload]``."""
    return _encode_varstring(doc_name) + _encode_varuint(msg_type) + payload


def _decode_message_header(data: bytes) -> tuple[str, int, int]:
    """Decode the leading header from a raw multiplayer message."""
    doc_name, offset = _decode_varstring(data, 0)
    msg_type, offset = _decode_varuint(data, offset)
    return doc_name, msg_type, offset


def _app_base_url_to_multiplayer_ws_url(app_base_url: str) -> str:
    """Derive the multiplayer WebSocket base URL from the Nominal app base URL."""
    match = re.match(r"^https?://app([^/]*)", app_base_url.rstrip("/"))
    if not match:
        raise ValueError(f"Cannot derive multiplayer WS URL from app_base_url: {app_base_url!r}")
    return f"wss://api{match.group(1)}/multiplayer"


def _app_base_url_to_deserialize_url(app_base_url: str) -> str:
    """Build the Scout deserialize endpoint from the app base URL."""
    return f"{app_base_url.rstrip('/')}/api/workbooks/deserialize"


def _app_base_url_to_serialize_url(app_base_url: str) -> str:
    """Build the Scout serialize endpoint from the app base URL."""
    return f"{app_base_url.rstrip('/')}/api/workbooks/serialize"


def _app_base_url_to_user_tokens_url(app_base_url: str) -> str:
    """Build the Scout user-token settings URL from the app base URL."""
    return f"{app_base_url.rstrip('/')}/settings/user/tokens"


def _build_room_url(ws_base_url: str, workbook_rid: str, schema_version: str | None) -> str:
    """Build the final multiplayer room URL."""
    base = f"{ws_base_url.rstrip('/')}/{workbook_rid}"
    if schema_version is None:
        return base
    query = urllib.parse.urlencode({"schemaVersion": schema_version})
    return f"{base}?{query}"


def _get_ws_auth_subprotocol(token: str) -> str:
    """Encode an API token as a base64url WebSocket sub-protocol header value."""
    encoded = base64.b64encode(token.encode()).decode()
    encoded = encoded.replace("+", "-").replace("/", "_").rstrip("=")
    return f"base64url.auth.{encoded}"


def _base64url_decode(value: str) -> bytes:
    """Decode a base64url string with optional missing padding."""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _decode_user_jwt_claims(token: str) -> dict[str, Any] | None:
    """Decode a JWT payload if it contains the Nominal user claims Scout expects."""
    parts = token.split(".")
    if len(parts) != 3:
        return None

    try:
        payload = json.loads(_base64url_decode(parts[1]).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    nominal_claims = payload.get("nominal")
    if not isinstance(nominal_claims, dict):
        return None
    if not nominal_claims.get("user_uuid") or not nominal_claims.get("organization_uuid"):
        return None

    return cast(dict[str, Any], payload)


def _extract_valid_user_jwt(candidate: str | None) -> str | None:
    """Return a valid user JWT from raw clipboard text or ``Bearer`` text."""
    if candidate is None:
        return None

    token = candidate.strip()
    if not token:
        return None
    if token.startswith("Bearer "):
        token = token.removeprefix("Bearer ").strip()
    if not token:
        return None

    if _decode_user_jwt_claims(token) is None:
        return None
    return token


def _validate_access_token(access_token: str) -> str:
    """Validate a raw access token intended for Scout-backed multiplayer APIs.

    Args:
        access_token: Raw access token without the ``Bearer `` prefix.

    Returns:
        The validated token, unchanged.

    Raises:
        ValueError: If the caller provides an API key or a token that still
            contains the ``Bearer `` prefix.
    """
    if access_token.startswith("Bearer "):
        raise ValueError("Pass a raw `access_token`, not an Authorization header. Omit the `Bearer ` prefix.")
    if access_token.startswith("nominal_api_key"):
        raise ValueError(
            "Multiplayer frontend APIs do not accept API keys. Provide a 24 hour personal access token instead."
        )
    return access_token


class _PyperclipModule(Protocol):
    """Small protocol describing the pyperclip surface this module uses."""

    PyperclipException: type[Exception]

    def paste(self) -> str: ...


def _load_pyperclip() -> _PyperclipModule:
    """Return the configured pyperclip module or raise a guided auth error."""
    if pyperclip is None:
        raise MultiplayerAuthError(
            "Clipboard-based JWT capture requires `pyperclip`; install `nominal[websockets]` "
            "or add `pyperclip` to your environment"
        )

    return cast(_PyperclipModule, pyperclip)


def _read_clipboard_text() -> str:
    """Read clipboard text from the local workstation via pyperclip.

    Raises:
        MultiplayerAuthError: If pyperclip is unavailable or clipboard access
            is not configured on the current machine.
    """
    pyperclip = _load_pyperclip()
    try:
        return pyperclip.paste()
    except pyperclip.PyperclipException as exc:
        raise MultiplayerAuthError(
            "Failed to read a JWT from the clipboard; ensure clipboard access is available on this machine"
        ) from exc


def prompt_for_user_jwt(
    app_base_url: str,
    *,
    timeout: float = DEFAULT_USER_JWT_TIMEOUT,
    poll_interval: float = DEFAULT_USER_JWT_POLL_INTERVAL,
) -> str:
    """Open the user token page and wait for a valid Scout-compatible JWT.

    This helper is intended for interactive developer workflows where a human
    can copy a personal access token from the Scout UI into the clipboard.

    Args:
        app_base_url: Scout application base URL.
        timeout: Maximum number of seconds to wait for the clipboard to contain
            a valid JWT.
        poll_interval: Delay between clipboard polls.

    Returns:
        A JWT without the ``Bearer `` prefix.

    Raises:
        MultiplayerAuthError: If the clipboard cannot be read or no valid JWT
            appears before the timeout expires.
    """
    tokens_url = _app_base_url_to_user_tokens_url(app_base_url)
    try:
        webbrowser.open(tokens_url, new=2, autoraise=True)
    except Exception as exc:
        raise MultiplayerAuthError(f"Failed to open the user token page: {tokens_url}") from exc

    deadline = time.monotonic() + timeout
    while True:
        token = _extract_valid_user_jwt(_read_clipboard_text())
        if token is not None:
            return token

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise MultiplayerAuthError(
                "Timed out waiting for a valid user JWT in the clipboard after opening "
                f"{tokens_url}. Copy a personal access token from the page and try again."
            )
        time.sleep(min(poll_interval, remaining))


class _MsgType(IntEnum):
    SYNC = 0
    AWARENESS = 1
    SYNC_DONE = 3
    PING = 4
    PONG = 5


class _SyncType(IntEnum):
    STEP1 = 0
    STEP2 = 1
    UPDATE = 2


class _CloseDisposition(Enum):
    RETRYABLE = "retryable"
    FATAL = "fatal"


class _ConnectionDropped(Exception):
    """Internal signal describing a closed or failed websocket attempt."""

    def __init__(
        self,
        *,
        disposition: _CloseDisposition,
        code: int | None = None,
        reason: str | None = None,
        error: BaseException | None = None,
        should_notify_disconnect: bool = False,
    ) -> None:
        super().__init__(reason or str(error) or disposition.value)
        self.disposition = disposition
        self.code = code
        self.reason = reason
        self.error = error
        self.should_notify_disconnect = should_notify_disconnect


class WorkbookSession:
    """Low-level connection to a live workbook multiplayer room.

    This class manages the websocket transport, sync handshake, reconnect
    behavior, and low-level document mutation for a workbook multiplayer room.
    Use it when you want direct access to the shared document or lifecycle
    callbacks.

    Most application code should prefer :class:`InteractiveWorkbookSession`,
    which wraps this transport layer with plain Python ``dict`` and ``list``
    access to the live workbook.

    Lifecycle callbacks registered on this class run on the background session
    thread. Callback code should stay lightweight and should hand off
    thread-sensitive work to the caller's preferred execution context.

    Calling :meth:`reconnect` forces the current socket to close, fires
    ``on_disconnect`` using the normal disconnect path, and blocks until the
    session is back in ``SYNCED`` or fails terminally.

    Example:
        access_token = "<personal-access-token>"
        client = NominalClient.from_profile("staging")
        workbook = client.get_workbook("ri.scout.gov-staging.notebook....")

        with WorkbookSession.create(workbook, access_token=access_token) as session:
            print(session.state)
            print(session.get_state()["workbook"]["metadata"]["title"])
    """

    @classmethod
    def create(
        cls,
        workbook: Workbook,
        *,
        access_token: str,
        timeout: float = DEFAULT_SYNC_TIMEOUT,
        ws_base_url: str | None = None,
        connection_timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        sync_timeout: float = DEFAULT_SYNC_TIMEOUT,
        reconnect: bool = True,
        schema_version: str | None = DEFAULT_SCHEMA_VERSION,
    ) -> WorkbookSession:
        """Open a low-level session from a workbook instance.

        Args:
            workbook: Workbook whose multiplayer room should be opened.
            access_token: Raw personal access token used for websocket auth.
            timeout: Overall timeout for the initial synchronous ``create()``
                wait.
            ws_base_url: Explicit websocket base URL. When omitted, the value is
                derived from the workbook client's app base URL.
            connection_timeout: Timeout for each websocket connect attempt.
            sync_timeout: Timeout budget for each sync handshake attempt.
            reconnect: Whether retryable disconnects should reconnect
                automatically.
            schema_version: Multiplayer schema version query parameter.

        Returns:
            A connected and initially synced session.
        """
        derived_ws_base_url = ws_base_url or _app_base_url_to_multiplayer_ws_url(workbook._clients.app_base_url)

        return cls(
            ws_base_url=derived_ws_base_url,
            workbook_rid=workbook.rid,
            access_token=access_token,
            timeout=timeout,
            connection_timeout=connection_timeout,
            sync_timeout=sync_timeout,
            reconnect=reconnect,
            schema_version=schema_version,
        )

    def __init__(
        self,
        ws_base_url: str,
        workbook_rid: str,
        access_token: str,
        *,
        timeout: float = DEFAULT_SYNC_TIMEOUT,
        connection_timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        sync_timeout: float = DEFAULT_SYNC_TIMEOUT,
        reconnect: bool = True,
        schema_version: str | None = DEFAULT_SCHEMA_VERSION,
    ) -> None:
        """Connect to the multiplayer service and wait for the initial sync.

        Args:
            ws_base_url: Multiplayer websocket base URL.
            workbook_rid: Workbook room identifier.
            access_token: Raw personal access token used for websocket auth.
            timeout: Overall timeout for the initial synchronous wait.
            connection_timeout: Timeout for each websocket connect attempt.
            sync_timeout: Timeout budget for each sync handshake attempt.
            reconnect: Whether retryable disconnects should reconnect
                automatically.
            schema_version: Multiplayer schema version query parameter.
        """
        self._ws_base_url = ws_base_url
        self._workbook_rid = workbook_rid
        self._access_token = _validate_access_token(access_token)
        self._connection_timeout = connection_timeout
        self._sync_timeout = sync_timeout
        self._reconnect_enabled = reconnect
        self._schema_version = schema_version

        self._doc: Doc[Any] = Doc()
        self._ws: ClientConnection | None = None
        self._has_ever_synced = False
        self._pending_reconnect_callback = False
        self._manual_reconnect_generation = 0
        self._pending_manual_reconnects: list[tuple[int, asyncio.Future[None]]] = []
        self._close_requested = False

        self._state = ConnectionState.CONNECTING
        self._last_close_code: int | None = None
        self._last_close_reason: str | None = None
        self._lifecycle_lock = threading.Lock()

        self._update_callbacks: list[Callable[[dict[str, Any]], None]] = []
        self._state_callbacks: list[Callable[[ConnectionState, ConnectionState], None]] = []
        self._disconnect_callbacks: list[Callable[[int | None, str | None], None]] = []
        self._reconnect_callbacks: list[Callable[[], None]] = []
        self._fatal_error_callbacks: list[Callable[[BaseException], None]] = []
        self._callbacks_lock = threading.Lock()

        self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            daemon=True,
            name="nominal-multiplayer",
        )
        self._thread.start()

        self._first_sync_future: Future[None] = Future()
        self._supervisor_future = asyncio.run_coroutine_threadsafe(self._supervisor_loop(), self._loop)

        try:
            self._first_sync_future.result(timeout=timeout)
        except FutureTimeoutError as exc:
            self.close()
            raise MultiplayerSyncTimeoutError("Timed out waiting for the initial multiplayer sync") from exc
        except Exception:
            self.close()
            raise

    @property
    def state(self) -> ConnectionState:
        """ConnectionState: Current connection lifecycle state."""
        with self._lifecycle_lock:
            return self._state

    @property
    def is_connected(self) -> bool:
        """bool: Whether a websocket connection is currently active."""
        return self.state in {ConnectionState.SYNCING, ConnectionState.SYNCED}

    @property
    def is_synced(self) -> bool:
        """bool: Whether the session has completed the current sync handshake."""
        return self.state is ConnectionState.SYNCED

    @property
    def last_close_code(self) -> int | None:
        """Int | None: Most recent websocket close code observed by the client."""
        with self._lifecycle_lock:
            return self._last_close_code

    @property
    def last_close_reason(self) -> str | None:
        """Str | None: Most recent websocket close reason observed by the client."""
        with self._lifecycle_lock:
            return self._last_close_reason

    def on_update(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Register a callback for synced document updates.

        Args:
            callback: Function invoked with the full room state after each
                incoming document update.
        """
        with self._callbacks_lock:
            self._update_callbacks.append(callback)

    def on_state_change(self, callback: Callable[[ConnectionState, ConnectionState], None]) -> None:
        """Register a callback for lifecycle state transitions.

        Args:
            callback: Function invoked as ``callback(new_state, previous_state)``
                on every state transition.
        """
        with self._callbacks_lock:
            self._state_callbacks.append(callback)

    def on_disconnect(self, callback: Callable[[int | None, str | None], None]) -> None:
        """Register a callback for disconnect events.

        Args:
            callback: Function invoked as ``callback(close_code, close_reason)``
                when a previously connected socket drops before reconnect logic
                begins.

        Notes:
            This callback also fires for manual :meth:`reconnect` calls because
            they intentionally reuse the normal disconnect and reconnect path.
        """
        with self._callbacks_lock:
            self._disconnect_callbacks.append(callback)

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """Register a callback for successful reconnects.

        Args:
            callback: Function invoked after a disconnected session returns to
                ``SYNCED``.

        Notes:
            This callback does not fire for the initial connection. It fires
            only after a session was already connected, then disconnected, and
            later finished syncing again.
        """
        with self._callbacks_lock:
            self._reconnect_callbacks.append(callback)

    def on_fatal_error(self, callback: Callable[[BaseException], None]) -> None:
        """Register a callback for terminal session failures.

        Args:
            callback: Function invoked once when the session transitions to
                ``FATAL``.

        Notes:
            Fatal errors stop automatic reconnect attempts.
        """
        with self._callbacks_lock:
            self._fatal_error_callbacks.append(callback)

    def get_state(self) -> dict[str, Any]:
        """Return the latest synced room state as plain Python data.

        Returns:
            A detached snapshot of the room state.
        """
        return cast(dict[str, Any], self._doc.get(self._workbook_rid, type=Map).to_py())

    def mutate(self, fn: Callable[[Map[Any]], None]) -> None:
        """Apply a low-level mutation to the live shared document.

        Args:
            fn: Callback that mutates the room root map in place.

        Raises:
            MultiplayerError: If called from the multiplayer callback thread.
        """
        self._raise_if_called_from_callback_thread("mutate")
        future = asyncio.run_coroutine_threadsafe(self._async_mutate(fn), self._loop)
        future.result()

    def reconnect(self) -> None:
        """Force a reconnect cycle and wait until the session is synced again.

        This method disconnects the current socket if one is active, triggers
        the normal reconnect path, and blocks until the session returns to
        ``SYNCED``. A manual reconnect fires ``on_disconnect`` before the
        reconnect begins.

        Raises:
            MultiplayerError: If the session is already closed.
            MultiplayerFatalError: If the session is already in a terminal
                fatal state or enters one during the reconnect attempt.

        Notes:
            This synchronous method must not be called from a lifecycle or
            update callback because those callbacks run on the session thread.
        """
        self._raise_if_called_from_callback_thread("reconnect")
        future = asyncio.run_coroutine_threadsafe(self._async_force_reconnect(), self._loop)
        future.result()

    def set_workbook(self, variables: dict[str, Any], tabs: list[Any]) -> None:
        """Replace workbook variables and layout using builder helpers.

        Args:
            variables: Frontend-shaped variable map.
            tabs: Helper-built tab objects with ``to_charts()`` and
                ``to_layout_tab()`` methods.
        """
        all_charts: dict[str, Any] = {}
        layout_tabs: list[Any] = []
        for tab in tabs:
            all_charts.update(tab.to_charts())
            layout_tabs.append(tab.to_layout_tab())
        self.set_workbook_raw(variables, charts=all_charts, layout_tabs=layout_tabs)

    def set_workbook_raw(
        self,
        variables: dict[str, Any],
        *,
        charts: dict[str, Any],
        layout_tabs: list[Any],
    ) -> None:
        """Replace workbook variables, charts, and layout from raw helper output.

        Args:
            variables: Frontend-shaped variable map.
            charts: Frontend-shaped chart definition map.
            layout_tabs: Frontend-shaped layout tabs.
        """
        new_layout_id = new_id()
        yjs_layout_tabs = Array([_to_yjs(tab) for tab in layout_tabs])
        yjs_charts = {chart_id: _to_yjs(panel) for chart_id, panel in charts.items()}
        yjs_variables = _to_yjs(variables)

        def _apply(root: Any) -> None:
            workbook = root["workbook"]
            content = workbook["content"]
            content["channelVariables"] = yjs_variables
            wb_charts = content["charts"]
            for key in list(wb_charts.keys()):
                del wb_charts[key]
            for chart_id, panel_def in yjs_charts.items():
                wb_charts[chart_id] = panel_def
            workbook["layout"] = {"id": new_layout_id, "tabs": yjs_layout_tabs}

        self.mutate(_apply)

    def close(self) -> None:
        """Close the websocket session and stop background resources.

        Notes:
            This synchronous method must not be called from a lifecycle or
            update callback because those callbacks run on the session thread.
        """
        self._raise_if_called_from_callback_thread("close")
        try:
            future = asyncio.run_coroutine_threadsafe(self._async_request_close(), self._loop)
            future.result(timeout=5.0)
        except Exception:
            pass

        try:
            self._supervisor_future.result(timeout=5.0)
        except Exception:
            pass

        self._shutdown_background_resources()

    def __enter__(self) -> WorkbookSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    async def _supervisor_loop(self) -> None:
        reconnect_attempt = 0

        try:
            while not self._close_requested:
                next_state = (
                    ConnectionState.RECONNECTING
                    if self._has_ever_synced or reconnect_attempt > 0
                    else ConnectionState.CONNECTING
                )
                self._transition_state(next_state)

                try:
                    disconnect = await self._run_connection_attempt()
                except MultiplayerFatalError as exc:
                    self._transition_to_fatal(exc)
                    return

                if self._close_requested:
                    break

                self._record_close(disconnect.code, disconnect.reason)
                if disconnect.should_notify_disconnect:
                    self._pending_reconnect_callback = True
                    self._fire_disconnect_callbacks(disconnect.code, disconnect.reason)

                if disconnect.disposition == _CloseDisposition.FATAL:
                    if disconnect.error is not None:
                        self._transition_to_fatal(self._coerce_fatal_error(disconnect.error, disconnect.reason))
                    else:
                        self._transition_to_fatal(
                            MultiplayerFatalError(disconnect.reason or "Encountered a fatal websocket close")
                        )
                    return

                if not (self._reconnect_enabled or self._has_pending_manual_reconnects()):
                    error = disconnect.error or MultiplayerError(
                        "Multiplayer session disconnected and reconnect is disabled"
                    )
                    if not self._first_sync_future.done():
                        self._first_sync_future.set_exception(error)
                    self._fail_pending_manual_reconnects(error)
                    break

                self._transition_state(ConnectionState.RECONNECTING)
                await asyncio.sleep(self._next_reconnect_backoff(reconnect_attempt))
                reconnect_attempt += 1
        except Exception as exc:
            self._transition_to_fatal(self._coerce_fatal_error(exc))
            return

        if self.state is not ConnectionState.FATAL:
            self._transition_state(ConnectionState.CLOSED)
            if not self._first_sync_future.done():
                self._first_sync_future.set_exception(
                    MultiplayerError("Multiplayer session closed before the initial sync completed")
                )
            self._fail_pending_manual_reconnects(MultiplayerError("Multiplayer session was closed"))

    async def _run_connection_attempt(self) -> _ConnectionDropped:
        ws: ClientConnection | None = None
        attempt_generation = self._manual_reconnect_generation
        try:
            ws = await self._connect_socket()
            self._ws = ws
            self._transition_state(ConnectionState.SYNCING)

            await self._perform_sync(ws)
            self._transition_state(ConnectionState.SYNCED)
            self._has_ever_synced = True

            if self._should_force_manual_reconnect(attempt_generation):
                return self._retryable_drop(
                    code=1000,
                    reason="closed",
                    error=None,
                    should_notify_disconnect=True,
                )

            if not self._first_sync_future.done():
                self._first_sync_future.set_result(None)
            if self._pending_reconnect_callback:
                self._pending_reconnect_callback = False
                self._fire_reconnect_callbacks()
            self._resolve_pending_manual_reconnects(attempt_generation)

            await self._send_sync_done_ack(ws)
            await self._receive_messages(ws)

            return self._build_connection_dropped(ws, should_notify_disconnect=True)
        except _ConnectionDropped as exc:
            return exc
        except MultiplayerFatalError:
            raise
        except Exception as exc:
            return self._build_connection_dropped(ws, error=exc, should_notify_disconnect=self._has_ever_synced)
        finally:
            self._ws = None
            if ws is not None:
                await self._safe_close_socket(ws)

    async def _connect_socket(self) -> ClientConnection:
        url = _build_room_url(self._ws_base_url, self._workbook_rid, self._schema_version)
        try:
            return await asyncio.wait_for(
                websockets.connect(url, subprotocols=[Subprotocol(_get_ws_auth_subprotocol(self._access_token))]),
                timeout=self._connection_timeout,
            )
        except Exception as exc:
            dropped = self._build_connection_dropped(None, error=exc, should_notify_disconnect=False)
            if dropped.disposition is _CloseDisposition.FATAL and dropped.error is not None:
                raise self._coerce_fatal_error(dropped.error, dropped.reason) from dropped.error
            raise dropped

    async def _perform_sync(self, ws: ClientConnection) -> None:
        sync_done = asyncio.Event()
        await self._send_sync_step1(ws)
        resend_task = asyncio.create_task(self._resend_sync_step1_until_done(ws, sync_done))
        deadline = self._loop.time() + self._sync_timeout

        try:
            while not sync_done.is_set():
                remaining = deadline - self._loop.time()
                if remaining <= 0:
                    raise MultiplayerSyncTimeoutError(
                        "Timed out waiting for the workbook multiplayer session to finish syncing"
                    )
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise MultiplayerSyncTimeoutError(
                        "Timed out waiting for the workbook multiplayer session to finish syncing"
                    ) from exc
                if not isinstance(raw, bytes):
                    raise MultiplayerFatalError(
                        "Received a non-binary websocket frame from the multiplayer service"
                    )
                await self._handle(raw, sync_done)
        except MultiplayerSyncTimeoutError:
            raise
        except MultiplayerFatalError:
            raise
        except Exception as exc:
            raise self._build_connection_dropped(ws, error=exc, should_notify_disconnect=self._has_ever_synced)
        finally:
            resend_task.cancel()
            try:
                await resend_task
            except asyncio.CancelledError:
                pass

    async def _resend_sync_step1_until_done(self, ws: ClientConnection, sync_done: asyncio.Event) -> None:
        # The server expects the client to keep nudging the sync handshake until
        # SyncDone arrives, especially across flaky or slow network conditions.
        while not sync_done.is_set():
            await self._send_sync_step1(ws)
            await asyncio.sleep(DEFAULT_SYNC_RETRY_INTERVAL)

    async def _receive_messages(self, ws: ClientConnection) -> None:
        while True:
            try:
                raw = await ws.recv()
            except Exception as exc:
                raise self._build_connection_dropped(ws, error=exc, should_notify_disconnect=True) from exc
            if not isinstance(raw, bytes):
                raise MultiplayerFatalError("Received a non-binary websocket frame from the multiplayer service")
            await self._handle(raw, None)

    async def _handle(self, data: bytes, sync_done: asyncio.Event | None) -> None:
        if not isinstance(data, bytes):
            raise MultiplayerFatalError("Received a non-binary websocket frame from the multiplayer service")

        doc_name, msg_type, offset = _decode_message_header(data)
        if doc_name != self._workbook_rid:
            raise MultiplayerFatalError(f"Received multiplayer update for unexpected document: {doc_name!r}")

        payload = data[offset:]

        match msg_type:
            case _MsgType.SYNC:
                sync_type, payload_offset = _decode_varuint(payload, 0)
                body = payload[payload_offset:]
                match sync_type:
                    case _SyncType.STEP1:
                        server_state_vector, _ = _decode_varuint8array(body, 0)
                        await self._send_sync_step2(cast(ClientConnection, self._ws), server_state_vector)
                    case _SyncType.STEP2 | _SyncType.UPDATE:
                        update, _ = _decode_varuint8array(body, 0)
                        self._doc.apply_update(update)
                        self._fire_update_callbacks()
            case _MsgType.AWARENESS:
                return
            case _MsgType.SYNC_DONE:
                if sync_done is not None:
                    sync_done.set()
            case _MsgType.PING:
                timestamp, _ = _decode_varuint(payload, 0)
                pong = _encode_message(self._workbook_rid, _MsgType.PONG, _encode_varuint(timestamp))
                await cast(ClientConnection, self._ws).send(pong)
            case _MsgType.PONG:
                return

    async def _send_sync_step1(self, ws: ClientConnection) -> None:
        state_vector = self._doc.get_state()
        payload = _encode_varuint(_SyncType.STEP1) + _encode_varuint8array(state_vector)
        await ws.send(_encode_message(self._workbook_rid, _MsgType.SYNC, payload))

    async def _send_sync_step2(self, ws: ClientConnection, server_state_vector: bytes) -> None:
        update = self._doc.get_update(server_state_vector)
        payload = _encode_varuint(_SyncType.STEP2) + _encode_varuint8array(update)
        await ws.send(_encode_message(self._workbook_rid, _MsgType.SYNC, payload))

    async def _send_sync_done_ack(self, ws: ClientConnection) -> None:
        await ws.send(_encode_message(self._workbook_rid, _MsgType.SYNC_DONE))

    async def _async_mutate(self, fn: Callable[[Map[Any]], None]) -> None:
        ws = self._ws
        if ws is None or self.state is not ConnectionState.SYNCED:
            raise MultiplayerError("Cannot mutate workbook state while the multiplayer session is not synced")

        state_before = self._doc.get_state()
        with self._doc.transaction():
            fn(self._doc.get(self._workbook_rid, type=Map))
        update = self._doc.get_update(state_before)
        payload = _encode_varuint(_SyncType.UPDATE) + _encode_varuint8array(update)
        await ws.send(_encode_message(self._workbook_rid, _MsgType.SYNC, payload))

    async def _async_force_reconnect(self) -> None:
        if self.state is ConnectionState.CLOSED:
            raise MultiplayerError("Cannot reconnect a closed multiplayer session")
        if self.state is ConnectionState.FATAL:
            raise MultiplayerFatalError("Cannot reconnect a multiplayer session in the fatal state")

        self._manual_reconnect_generation += 1
        target_generation = self._manual_reconnect_generation
        waiter = self._loop.create_future()
        self._register_pending_manual_reconnect(target_generation, waiter)

        if self._ws is not None:
            await self._safe_close_socket(self._ws)

        await waiter

    async def _async_request_close(self) -> None:
        if self._close_requested:
            return
        self._close_requested = True
        if self._ws is not None:
            await self._safe_close_socket(self._ws)

    async def _safe_close_socket(self, ws: ClientConnection) -> None:
        try:
            await ws.close()
        except Exception:
            return

    def _next_reconnect_backoff(self, reconnect_attempt: int) -> float:
        if reconnect_attempt >= len(DEFAULT_RECONNECT_BACKOFFS):
            return DEFAULT_RECONNECT_BACKOFFS[-1]
        return DEFAULT_RECONNECT_BACKOFFS[reconnect_attempt]

    def _build_connection_dropped(
        self,
        ws: ClientConnection | None,
        *,
        error: BaseException | None = None,
        should_notify_disconnect: bool,
    ) -> _ConnectionDropped:
        code = getattr(ws, "close_code", None)
        reason = getattr(ws, "close_reason", None)

        if isinstance(error, _ConnectionDropped):
            return error

        if isinstance(error, ConnectionClosed):
            code = error.code
            reason = error.reason

        return self._connection_dropped_from_error(
            code=code,
            reason=reason,
            error=error,
            should_notify_disconnect=should_notify_disconnect,
        )

    def _connection_dropped_from_error(
        self,
        *,
        code: int | None,
        reason: str | None,
        error: BaseException | None,
        should_notify_disconnect: bool,
    ) -> _ConnectionDropped:
        dropped: _ConnectionDropped | None = None

        if isinstance(error, MultiplayerSyncTimeoutError):
            dropped = self._retryable_drop(
                code=code,
                reason=str(error),
                error=error,
                should_notify_disconnect=should_notify_disconnect,
            )
        elif isinstance(error, (MultiplayerFatalError, MultiplayerAuthError)):
            dropped = self._fatal_drop(
                code=code,
                reason=reason or str(error),
                error=error,
                should_notify_disconnect=should_notify_disconnect,
            )
        elif isinstance(error, InvalidStatus):
            status_code = self._extract_status_code(error)
            if status_code in {401, 403}:
                auth_error = MultiplayerAuthError(f"Multiplayer authentication failed with HTTP {status_code}")
                dropped = self._fatal_drop(
                    code=status_code,
                    reason=str(auth_error),
                    error=auth_error,
                    should_notify_disconnect=should_notify_disconnect,
                )
            else:
                dropped = self._fatal_drop(
                    code=status_code,
                    reason=f"Multiplayer websocket handshake failed with HTTP {status_code}",
                    error=error,
                    should_notify_disconnect=should_notify_disconnect,
                )
        elif isinstance(error, (InvalidHandshake, WebSocketException)):
            dropped = self._fatal_drop(
                code=code,
                reason=reason or str(error),
                error=error,
                should_notify_disconnect=should_notify_disconnect,
            )
        elif code in {4001, 4003, 4401, 4403}:
            auth_error = MultiplayerAuthError(reason or "Multiplayer authentication failed")
            dropped = self._fatal_drop(
                code=code,
                reason=reason or str(auth_error),
                error=auth_error,
                should_notify_disconnect=should_notify_disconnect,
            )
        elif code in {1002, 1003, 1007, 1008, 1009, 1010}:
            dropped = self._fatal_drop(
                code=code,
                reason=reason or "Encountered a fatal websocket close",
                error=error,
                should_notify_disconnect=should_notify_disconnect,
            )
        else:
            dropped = self._retryable_drop(
                code=code,
                reason=reason or (str(error) if error is not None else None),
                error=error,
                should_notify_disconnect=should_notify_disconnect,
            )

        return dropped

    def _fatal_drop(
        self,
        *,
        code: int | None,
        reason: str | None,
        error: BaseException | None,
        should_notify_disconnect: bool,
    ) -> _ConnectionDropped:
        return _ConnectionDropped(
            disposition=_CloseDisposition.FATAL,
            code=code,
            reason=reason,
            error=error,
            should_notify_disconnect=should_notify_disconnect,
        )

    def _retryable_drop(
        self,
        *,
        code: int | None,
        reason: str | None,
        error: BaseException | None,
        should_notify_disconnect: bool,
    ) -> _ConnectionDropped:
        return _ConnectionDropped(
            disposition=_CloseDisposition.RETRYABLE,
            code=code,
            reason=reason,
            error=error,
            should_notify_disconnect=should_notify_disconnect,
        )

    def _extract_status_code(self, exc: InvalidStatus) -> int | None:
        status_code = getattr(exc, "status_code", None)
        if status_code is not None:
            return cast(int, status_code)

        response = getattr(exc, "response", None)
        if response is None:
            return None
        return cast(int | None, getattr(response, "status_code", None))

    def _coerce_fatal_error(self, error: BaseException, reason: str | None = None) -> BaseException:
        if isinstance(error, MultiplayerAuthError):
            return error
        if isinstance(error, MultiplayerFatalError):
            return error
        return MultiplayerFatalError(reason or str(error) or "Encountered a fatal multiplayer error")

    def _transition_state(self, new_state: ConnectionState) -> None:
        with self._lifecycle_lock:
            previous_state = self._state
            if previous_state is new_state:
                return
            self._state = new_state

        self._fire_state_callbacks(new_state, previous_state)

    def _transition_to_fatal(self, exc: BaseException) -> None:
        self._transition_state(ConnectionState.FATAL)
        if not self._first_sync_future.done():
            self._first_sync_future.set_exception(exc)
        self._fail_pending_manual_reconnects(exc)
        self._fire_fatal_error_callbacks(exc)

    def _record_close(self, code: int | None, reason: str | None) -> None:
        with self._lifecycle_lock:
            self._last_close_code = code
            self._last_close_reason = reason

    def _register_pending_manual_reconnect(self, generation: int, waiter: asyncio.Future[None]) -> None:
        self._pending_manual_reconnects.append((generation, waiter))

    def _resolve_pending_manual_reconnects(self, completed_generation: int) -> None:
        remaining: list[tuple[int, asyncio.Future[None]]] = []
        for generation, waiter in self._pending_manual_reconnects:
            if generation <= completed_generation:
                if not waiter.done():
                    waiter.set_result(None)
            else:
                remaining.append((generation, waiter))
        self._pending_manual_reconnects = remaining

    def _fail_pending_manual_reconnects(self, exc: BaseException) -> None:
        for _, waiter in self._pending_manual_reconnects:
            if not waiter.done():
                waiter.set_exception(exc)
        self._pending_manual_reconnects = []

    def _has_pending_manual_reconnects(self) -> bool:
        return any(not waiter.done() for _, waiter in self._pending_manual_reconnects)

    def _should_force_manual_reconnect(self, attempt_generation: int) -> bool:
        return self._manual_reconnect_generation > attempt_generation

    def _raise_if_called_from_callback_thread(self, operation: str) -> None:
        if threading.current_thread() is self._thread:
            raise MultiplayerError(
                f"Cannot call {operation}() from the multiplayer callback thread; "
                "hand work off to another thread or event loop first"
            )

    def _fire_update_callbacks(self) -> None:
        with self._callbacks_lock:
            callbacks = list(self._update_callbacks)
        if not callbacks:
            return
        state = self.get_state()
        for callback in callbacks:
            try:
                callback(state)
            except Exception:
                continue

    def _fire_state_callbacks(self, new_state: ConnectionState, previous_state: ConnectionState) -> None:
        with self._callbacks_lock:
            callbacks = list(self._state_callbacks)
        for callback in callbacks:
            try:
                callback(new_state, previous_state)
            except Exception:
                continue

    def _fire_disconnect_callbacks(self, code: int | None, reason: str | None) -> None:
        with self._callbacks_lock:
            callbacks = list(self._disconnect_callbacks)
        for callback in callbacks:
            try:
                callback(code, reason)
            except Exception:
                continue

    def _fire_reconnect_callbacks(self) -> None:
        with self._callbacks_lock:
            callbacks = list(self._reconnect_callbacks)
        for callback in callbacks:
            try:
                callback()
            except Exception:
                continue

    def _fire_fatal_error_callbacks(self, exc: BaseException) -> None:
        with self._callbacks_lock:
            callbacks = list(self._fatal_error_callbacks)
        for callback in callbacks:
            try:
                callback(exc)
            except Exception:
                continue

    def _shutdown_background_resources(self) -> None:
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread.is_alive():
            self._thread.join(timeout=5.0)
        if not self._loop.is_closed():
            self._loop.close()


class InteractiveWorkbookSession:
    """High-level live workbook client built around normal Python objects.

    This is the recommended API for most developers working in this package.
    It exposes the live workbook as ordinary Python structures and mirrors the
    low-level lifecycle state from :class:`WorkbookSession`.

    Lifecycle callbacks registered on this class are forwarded to the
    underlying :class:`WorkbookSession` and therefore also run on the
    background session thread.

    Example:
        access_token = "<personal-access-token>"
        client = NominalClient.from_profile("staging")
        workbook = client.get_workbook("ri.scout.gov-staging.notebook....")

        with InteractiveWorkbookSession.create(workbook, access_token=access_token) as session:
            session.subscribe(
                lambda title: print("title changed:", title),
                selector=lambda snapshot: snapshot["workbook"]["metadata"]["title"],
                emit_initial=True,
            )

            session.mutate_workbook(
                lambda workbook_json: workbook_json["metadata"].__setitem__("title", "Demo")
            )
    """

    @classmethod
    def create(
        cls,
        workbook: Workbook,
        *,
        access_token: str,
        timeout: float = DEFAULT_SYNC_TIMEOUT,
        ws_base_url: str | None = None,
        connection_timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        sync_timeout: float = DEFAULT_SYNC_TIMEOUT,
        reconnect: bool = True,
        schema_version: str | None = DEFAULT_SCHEMA_VERSION,
    ) -> InteractiveWorkbookSession:
        """Open a high-level session from a workbook.

        Args:
            workbook: Workbook whose multiplayer room should be opened.
            access_token: Raw personal access token used for websocket and
                Scout frontend API auth.
            timeout: Overall timeout for the initial synchronous wait.
            ws_base_url: Explicit websocket base URL.
            connection_timeout: Timeout for each websocket connect attempt.
            sync_timeout: Timeout budget for each sync handshake attempt.
            reconnect: Whether retryable disconnects should reconnect
                automatically.
            schema_version: Multiplayer schema version query parameter.

        Returns:
            A connected and initially synced high-level session.
        """
        session = WorkbookSession.create(
            workbook,
            access_token=access_token,
            timeout=timeout,
            ws_base_url=ws_base_url,
            connection_timeout=connection_timeout,
            sync_timeout=sync_timeout,
            reconnect=reconnect,
            schema_version=schema_version,
        )
        return cls(
            session=session,
            app_base_url=workbook._clients.app_base_url,
            workbook_rid=workbook.rid,
            access_token=access_token,
            notebook_supplier=workbook._get_latest_api,
        )

    def __init__(
        self,
        *,
        session: WorkbookSession,
        app_base_url: str,
        workbook_rid: str,
        access_token: str,
        notebook_supplier: Callable[[], scout_notebook_api.Notebook] | None = None,
    ) -> None:
        """Create a high-level wrapper around an existing low-level session.

        Args:
            session: Underlying low-level session.
            app_base_url: Scout app base URL used for workbook conversion APIs.
            workbook_rid: Workbook room identifier.
            access_token: Raw personal access token used for frontend API auth.
            notebook_supplier: Optional callable that fetches the latest
                notebook object for serialize operations.
        """
        self._session = session
        self._app_base_url = app_base_url
        self._workbook_rid = workbook_rid
        self._access_token = _validate_access_token(access_token)
        self._notebook_supplier = notebook_supplier
        self._subscriptions: list[dict[str, Any]] = []
        self._subscriptions_lock = threading.Lock()

        self._session.on_update(lambda _state: self._notify_subscribers())

    @property
    def state(self) -> ConnectionState:
        """ConnectionState: Current connection lifecycle state."""
        return self._session.state

    @property
    def is_connected(self) -> bool:
        """bool: Whether the underlying websocket connection is active."""
        return self._session.is_connected

    @property
    def is_synced(self) -> bool:
        """bool: Whether the session has completed the current sync handshake."""
        return self._session.is_synced

    @property
    def last_close_code(self) -> int | None:
        """Int | None: Most recent websocket close code."""
        return self._session.last_close_code

    @property
    def last_close_reason(self) -> str | None:
        """Str | None: Most recent websocket close reason."""
        return self._session.last_close_reason

    def on_state_change(self, callback: Callable[[ConnectionState, ConnectionState], None]) -> None:
        """Register a callback for lifecycle state transitions.

        Args:
            callback: Function invoked as ``callback(new_state, previous_state)``
                on every lifecycle transition.
        """
        self._session.on_state_change(callback)

    def on_disconnect(self, callback: Callable[[int | None, str | None], None]) -> None:
        """Register a callback for disconnect events.

        Args:
            callback: Function invoked as ``callback(close_code, close_reason)``
                whenever the underlying websocket disconnects.

        Notes:
            Manual :meth:`reconnect` calls also trigger this callback before
            the reconnect attempt starts.
        """
        self._session.on_disconnect(callback)

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """Register a callback for successful reconnects.

        Args:
            callback: Function invoked after a disconnected session returns to
                ``SYNCED``.
        """
        self._session.on_reconnect(callback)

    def on_fatal_error(self, callback: Callable[[BaseException], None]) -> None:
        """Register a callback for terminal failures.

        Args:
            callback: Function invoked once when the underlying session enters
                the terminal ``FATAL`` state.
        """
        self._session.on_fatal_error(callback)

    def get_snapshot(self) -> dict[str, Any]:
        """Return the full live room snapshot.

        Returns:
            A detached snapshot containing ``snapshotRid`` and ``workbook``.
        """
        return copy.deepcopy(self._session.get_state())

    def get_workbook_json(self) -> dict[str, Any]:
        """Return the current live workbook as frontend-shaped Python data.

        Returns:
            A detached copy of the current workbook payload.
        """
        snapshot = self.get_snapshot()
        return cast(dict[str, Any], snapshot["workbook"])

    def mutate_workbook(self, fn: Callable[[dict[str, Any]], Any]) -> dict[str, Any]:
        """Edit the live workbook using normal Python objects.

        Args:
            fn: Callback that mutates the copied workbook in place or returns a
                replacement workbook object.

        Returns:
            The updated live workbook payload after the mutation is applied.

        Raises:
            MultiplayerError: If called from the multiplayer callback thread.
        """
        workbook = self.get_workbook_json()
        result = fn(workbook)
        if result is not None:
            workbook = cast(dict[str, Any], result)
        updated = self.replace_workbook_json(workbook)
        return updated

    def replace_workbook_json(self, workbook: dict[str, Any]) -> dict[str, Any]:
        """Replace the live workbook with a frontend-shaped workbook payload.

        Args:
            workbook: Frontend workbook payload whose ``rid`` must match the
                current room.

        Returns:
            The updated live workbook payload.

        Raises:
            MultiplayerError: If called from the multiplayer callback thread.
        """
        candidate = copy.deepcopy(workbook)
        rid = candidate.get("rid")
        if rid != self._workbook_rid:
            raise ValueError(
                f"Workbook RID mismatch: expected {self._workbook_rid!r}, received {rid!r}"
            )

        def _apply(root: Map[Any]) -> None:
            if "workbook" in root:
                del root["workbook"]
            root["workbook"] = _to_yjs(candidate)

        self._session.mutate(_apply)
        self._notify_subscribers()
        return self.get_workbook_json()

    def deserialize_notebook(self, notebook: Any) -> dict[str, Any]:
        """Convert a conjure notebook object into frontend workbook JSON.

        Args:
            notebook: Conjure notebook object or notebook-shaped dictionary.

        Returns:
            Frontend workbook JSON for the supplied notebook.

        Raises:
            NotebookDeserializeError: If the deserialize endpoint rejects the
                request or cannot be reached.
        """
        return self._deserialize_notebook(notebook)

    def serialize_workbook_json(
        self,
        workbook: dict[str, Any] | None = None,
        *,
        base_notebook: scout_notebook_api.Notebook | None = None,
    ) -> scout_notebook_api.Notebook:
        """Convert frontend workbook JSON into a conjure notebook object.

        Args:
            workbook: Frontend workbook JSON to serialize. When omitted, the
                current live workbook is serialized.
            base_notebook: Optional latest notebook object to pair with the
                serialize response. When omitted, the session uses the supplier
                captured during :meth:`create`.

        Returns:
            A conjure notebook object built from the serialize response.

        Raises:
            NotebookSerializeError: If the serialize endpoint rejects the
                request, cannot be reached, or no base notebook is available.
        """
        source_workbook = self.get_workbook_json() if workbook is None else copy.deepcopy(workbook)
        notebook = base_notebook or self._get_latest_notebook()
        return self._serialize_workbook_json(source_workbook, notebook)

    def get_notebook(self, *, base_notebook: scout_notebook_api.Notebook | None = None) -> scout_notebook_api.Notebook:
        """Return the current live workbook as a conjure notebook object.

        Args:
            base_notebook: Optional latest notebook object to pair with the
                serialize response. When omitted, the session uses the supplier
                captured during :meth:`create`.

        Returns:
            A conjure notebook object representing the current live workbook.
        """
        return self.serialize_workbook_json(base_notebook=base_notebook)

    def replace_notebook(self, notebook: Any) -> dict[str, Any]:
        """Replace the live workbook from a conjure notebook object.

        Args:
            notebook: Conjure notebook object or notebook-shaped dictionary.

        Returns:
            The updated live workbook payload.

        Raises:
            NotebookDeserializeError: If the deserialize endpoint rejects the
                request or cannot be reached.
        """
        workbook = self.deserialize_notebook(notebook)
        return self.replace_workbook_json(workbook)

    def subscribe(
        self,
        callback: Callable[[Any], None],
        *,
        selector: Callable[[dict[str, Any]], Any] | None = None,
        emit_initial: bool = False,
    ) -> Callable[[], None]:
        """Subscribe to live workbook updates.

        Args:
            callback: Function invoked when the selected value changes.
            selector: Optional function that selects a smaller value from the
                full snapshot. When omitted, the callback receives the full
                snapshot.
            emit_initial: Whether to invoke the callback immediately with the
                current value.

        Returns:
            A function that removes the subscription.

        Notes:
            Subscriptions stay registered across reconnects. Selector-based
            subscriptions continue deduplicating by equality, so reconnecting
            without a meaningful value change does not trigger a duplicate
            callback.
        """
        subscription = {
            "callback": callback,
            "selector": selector,
            "last_value": _SUBSCRIPTION_SENTINEL,
        }
        with self._subscriptions_lock:
            self._subscriptions.append(subscription)

        if emit_initial:
            self._emit_subscription(subscription, self.get_snapshot())

        def unsubscribe() -> None:
            with self._subscriptions_lock:
                if subscription in self._subscriptions:
                    self._subscriptions.remove(subscription)

        return unsubscribe

    def reconnect(self) -> None:
        """Force a reconnect cycle and wait until the session is synced again.

        Notes:
            This method blocks until the underlying low-level session reaches
            ``SYNCED`` again. It intentionally uses the normal disconnect path,
            so registered disconnect callbacks fire before reconnect begins.
            Like the low-level API, this method must not be called from a
            lifecycle or update callback.
        """
        self._session.reconnect()

    def close(self) -> None:
        """Close the session and underlying websocket connection.

        Notes:
            Like the low-level API, this method must not be called from a
            lifecycle or update callback.
        """
        self._session.close()

    def __enter__(self) -> InteractiveWorkbookSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def _notify_subscribers(self) -> None:
        with self._subscriptions_lock:
            subscriptions = list(self._subscriptions)
        if not subscriptions:
            return
        snapshot = self.get_snapshot()
        for subscription in subscriptions:
            self._emit_subscription(subscription, snapshot)

    def _emit_subscription(self, subscription: dict[str, Any], snapshot: dict[str, Any]) -> None:
        selector = cast(Callable[[dict[str, Any]], Any] | None, subscription["selector"])
        callback = cast(Callable[[Any], None], subscription["callback"])
        value = copy.deepcopy(snapshot) if selector is None else selector(copy.deepcopy(snapshot))

        previous = subscription["last_value"]
        if previous is not _SUBSCRIPTION_SENTINEL and previous == value:
            return

        safe_value = copy.deepcopy(value)
        subscription["last_value"] = safe_value
        callback(copy.deepcopy(safe_value))

    def _get_latest_notebook(self) -> scout_notebook_api.Notebook:
        """Return the latest notebook object needed for serialize operations."""
        if self._notebook_supplier is None:
            raise NotebookSerializeError(
                "This interactive workbook session cannot serialize to a Notebook because no notebook supplier "
                "was configured. Pass `base_notebook=` explicitly or create the session from a Workbook object."
            )
        return self._notebook_supplier()

    def _deserialize_notebook(self, notebook: Any) -> dict[str, Any]:
        body = json.dumps(ConjureEncoder.do_encode(notebook)).encode("utf-8")
        request = urllib.request.Request(
            _app_base_url_to_deserialize_url(self._app_base_url),
            method="POST",
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            },
            data=body,
        )

        try:
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            self._raise_notebook_deserialize_http_error(exc)
        except urllib.error.URLError as exc:
            raise NotebookDeserializeError(f"Notebook deserialization request failed: {exc.reason}") from exc
        return cast(dict[str, Any], payload)

    def _serialize_workbook_json(
        self,
        workbook: dict[str, Any],
        base_notebook: scout_notebook_api.Notebook,
    ) -> scout_notebook_api.Notebook:
        request = urllib.request.Request(
            _app_base_url_to_serialize_url(self._app_base_url),
            method="POST",
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            },
            data=json.dumps(
                {
                    "workbook": workbook,
                    "latestSnapshotRid": base_notebook.snapshot_rid,
                }
            ).encode("utf-8"),
        )

        try:
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            self._raise_notebook_serialize_http_error(exc)
        except urllib.error.URLError as exc:
            raise NotebookSerializeError(f"Notebook serialization request failed: {exc.reason}") from exc

        update = ConjureDecoder().do_decode(payload, scout_notebook_api.UpdateNotebookRequest)
        return scout_notebook_api.Notebook(
            rid=base_notebook.rid,
            snapshot_rid=base_notebook.snapshot_rid,
            snapshot_author_rid=base_notebook.snapshot_author_rid,
            snapshot_created_at=base_notebook.snapshot_created_at,
            metadata=base_notebook.metadata,
            state_as_json=update.state_as_json,
            charts=update.charts,
            layout=update.layout,
            content=update.content,
            content_v2=update.content_v2 or base_notebook.content_v2,
            event_refs=update.event_refs,
            check_alert_refs=update.check_alert_refs,
        )

    def _raise_notebook_deserialize_http_error(self, exc: urllib.error.HTTPError) -> NoReturn:
        body_text = exc.read().decode("utf-8", errors="replace").strip()
        if exc.code == 401:
            raise NotebookDeserializeError(
                "Notebook deserialization failed with HTTP 401: token is not valid"
            ) from exc
        if exc.code == 403:
            raise NotebookDeserializeError(
                "Notebook deserialization failed with HTTP 403: access is denied"
            ) from exc
        if exc.code == 422:
            raise NotebookDeserializeError(
                f"Notebook deserialization failed with HTTP 422: {body_text or 'request payload was invalid'}"
            ) from exc
        excerpt = body_text[:200] if body_text else "no response body"
        raise NotebookDeserializeError(f"Notebook deserialization failed with HTTP {exc.code}: {excerpt}") from exc

    def _raise_notebook_serialize_http_error(self, exc: urllib.error.HTTPError) -> NoReturn:
        body_text = exc.read().decode("utf-8", errors="replace").strip()
        if exc.code == 401:
            raise NotebookSerializeError(
                "Notebook serialization failed with HTTP 401: token is not valid"
            ) from exc
        if exc.code == 403:
            raise NotebookSerializeError(
                "Notebook serialization failed with HTTP 403: access is denied"
            ) from exc
        if exc.code == 422:
            raise NotebookSerializeError(
                f"Notebook serialization failed with HTTP 422: {body_text or 'request payload was invalid'}"
            ) from exc
        excerpt = body_text[:200] if body_text else "no response body"
        raise NotebookSerializeError(f"Notebook serialization failed with HTTP {exc.code}: {excerpt}") from exc
