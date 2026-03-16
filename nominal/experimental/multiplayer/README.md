# Experimental Multiplayer Workbook Guide

This module provides a Python client for Nominal's workbook multiplayer service.
It is designed for cases where Python code wants to observe or manipulate a live workbook
while a user is interacting with that same workbook in the browser.

The library has two layers:

- `WorkbookSession`: low-level access to the live Yjs-backed room state
- `InteractiveWorkbookSession`: high-level access using plain Python `dict` and `list` objects

## Installation

Install the optional websocket dependencies:

```bash
pip install "nominal[websockets]"
```

## Mental Model

Each multiplayer room is keyed by the workbook RID.
Once connected, the shared root state looks like:

```python
{
    "snapshotRid": "ri.scout.main.snapshot....",
    "workbook": {
        "rid": "ri.scout.main.notebook....",
        "metadata": {...},
        "layout": {...},
        "content": {...},
    },
}
```

`snapshotRid` is server-owned.
Your code should read it, but should not try to set it.

## Which API Should I Use?

Use `InteractiveWorkbookSession` for most code.
It lets you:

- read the live workbook as normal Python objects
- mutate the workbook without importing `pycrdt`
- replace the whole workbook from frontend workbook JSON
- replace the whole workbook from a conjure notebook object
- subscribe to changes in the whole snapshot or a selected subtree

Use `WorkbookSession` only when you specifically want low-level control over the underlying Yjs map.

## Quick Start

```python
from nominal.core import NominalClient
from nominal.experimental.multiplayer import InteractiveWorkbookSession

access_token = "<personal-access-token>"
client = NominalClient.from_profile("production")
workbook = client.get_workbook("ri.scout.main.notebook.1234")

with InteractiveWorkbookSession.create(workbook, access_token=access_token) as session:
    snapshot = session.get_snapshot()
    print(snapshot["snapshotRid"])
    print(snapshot["workbook"]["metadata"]["title"])
```

By default the client connects using the current multiplayer schema version and waits up to 30 seconds for the initial sync to complete.
Pass a raw personal access token, not an API key and not an Authorization header value.

You can also override connection behavior when needed:

```python
session = InteractiveWorkbookSession.create(
    workbook,
    access_token=access_token,
    ws_base_url="wss://api.gov-staging.nominal.io/multiplayer",
    connection_timeout=10.0,
    sync_timeout=30.0,
    reconnect=True,
)
```

The `timeout` argument on `create()` is a real overall timeout for the initial
blocking create call; by default it is 30 seconds.

If you accidentally pass an API key such as a value starting with `nominal_api_key`,
the client raises `ValueError` and tells you to use a 24 hour personal access token instead.

For interactive debugging against Scout routes that require a user JWT, you can
ask the library to open the token settings page and wait for a personal access
token to appear in your clipboard:

```python
from nominal.experimental.multiplayer import prompt_for_user_jwt

access_token = prompt_for_user_jwt(workbook._clients.app_base_url)

with InteractiveWorkbookSession.create(workbook, access_token=access_token) as session:
    print(session.get_workbook_json()["metadata"]["title"])
```

This flow:

- opens `<app_base_url>/settings/user/tokens` in your browser
- polls the clipboard with `pyperclip` until it contains a valid JWT with Nominal user claims
- pins that JWT to the session so websocket reconnects and `replace_notebook()`
  keep using the same token

If you want the browser-and-clipboard step without creating a session yet, call
`prompt_for_user_jwt(app_base_url)` directly.

## Reading Live State

Get the full room snapshot:

```python
snapshot = session.get_snapshot()
```

Get just the frontend workbook JSON:

```python
workbook_json = session.get_workbook_json()
```

Both methods return plain Python data, not live proxy objects.
Mutating the returned value does not update the room until you call a write method.

## Mutating the Workbook

The easiest write path is `mutate_workbook()`.
It hands your callback a plain Python workbook dict, then writes the updated workbook back to the room.

```python
def rename(workbook: dict) -> None:
    workbook["metadata"]["title"] = "Flight Review"

with InteractiveWorkbookSession.create(workbook, access_token=access_token) as session:
    updated = session.mutate_workbook(rename)
    print(updated["metadata"]["title"])
```

You can also return a replacement workbook object:

```python
def strip_panels(workbook: dict) -> dict:
    new_workbook = dict(workbook)
    new_workbook["layout"] = {"tabs": []}
    return new_workbook

session.mutate_workbook(strip_panels)
```

## Replacing the Whole Workbook from Frontend JSON

If you already have a frontend-shaped workbook object, call `replace_workbook_json()`:

```python
new_workbook = session.get_workbook_json()
new_workbook["metadata"]["title"] = "Generated Workbook"

session.replace_workbook_json(new_workbook)
```

The incoming workbook must have the same workbook RID as the room you are connected to.
If it does not, the client raises `ValueError`.

## Replacing the Whole Workbook from a Conjure Notebook

If your upstream code produces a conjure notebook object, call `replace_notebook()`:

```python
# `notebook` can be a `scout_notebook_api.Notebook` object or a conjure-shaped dict
# that matches the notebook JSON expected by the notebook deserialize endpoint.
notebook = get_or_build_notebook_somehow()

session.replace_notebook(notebook)
```

This works by sending the notebook to Scout's
`/api/workbooks/deserialize` route on the app host.

If you want explicit conversion helpers without mutating the live workbook:

```python
workbook_json = session.deserialize_notebook(notebook)
notebook_again = session.serialize_workbook_json(workbook_json)
live_notebook = session.get_notebook()
```

## Subscribing to Updates

Use `subscribe()` to react to changes in the live room.

Subscribe to the whole snapshot:

```python
def on_snapshot(snapshot: dict) -> None:
    print("title:", snapshot["workbook"]["metadata"]["title"])

unsubscribe = session.subscribe(on_snapshot, emit_initial=True)
```

Subscribe to only one subtree with `selector=`:

```python
def on_event_refs(event_refs: list[str]) -> None:
    print("event refs changed:", event_refs)

unsubscribe = session.subscribe(
    on_event_refs,
    selector=lambda snapshot: snapshot["workbook"]["content"]["eventRefs"],
    emit_initial=True,
)
```

Selector subscriptions only fire when the selected value actually changes by equality.
This makes them a good fit for downstream code that wants to watch specific fields such as event refs, settings, or layout fragments.

Subscriptions remain registered across reconnects.
If the selected value is unchanged after reconnect, the subscription does not emit a duplicate callback.

Call the function returned by `subscribe()` to stop receiving updates:

```python
unsubscribe()
```

## Low-Level API

Use `WorkbookSession` when you need direct access to the Yjs-backed root map.

```python
from nominal.experimental.multiplayer import WorkbookSession

with WorkbookSession.create(workbook, access_token=access_token) as session:
    state = session.get_state()
    print(state["workbook"]["metadata"]["title"])

    def update(root) -> None:
        root["workbook"]["metadata"]["title"] = "Low-Level Title"

    session.mutate(update)
```

`WorkbookSession.mutate()` receives the root Yjs map for the room state, not just the workbook object.
That means the callback works against:

```python
root["snapshotRid"]
root["workbook"]
```

This API is useful when you need protocol-level control, but it intentionally exposes more of the underlying CRDT machinery.

## Lifecycle and Reconnects

Both `WorkbookSession` and `InteractiveWorkbookSession` expose the same lifecycle properties:

- `state`
- `is_connected`
- `is_synced`
- `last_close_code`
- `last_close_reason`

The lifecycle states are:

- `CONNECTING`: opening a websocket connection
- `SYNCING`: connected, but still completing the initial sync handshake
- `SYNCED`: connected and ready for reads and writes
- `RECONNECTING`: retrying after a non-fatal disconnect
- `CLOSED`: closed by user code
- `FATAL`: terminal failure; the client will not reconnect again

Register lifecycle callbacks like this:

```python
def on_state_change(state, previous_state):
    print(f"{previous_state.value} -> {state.value}")

def on_disconnect(code, reason):
    print("disconnected:", code, reason)

def on_reconnect():
    print("synced again")

def on_fatal_error(exc):
    print("fatal:", exc)

session.on_state_change(on_state_change)
session.on_disconnect(on_disconnect)
session.on_reconnect(on_reconnect)
session.on_fatal_error(on_fatal_error)
```

Callback semantics:

- `on_state_change(state, previous_state)` fires on every lifecycle transition
- `on_disconnect(code, reason)` fires when a previously connected socket drops, before reconnect logic begins
- `on_reconnect()` fires only after a disconnect and subsequent return to `SYNCED`
- `on_fatal_error(exc)` fires once when the session enters `FATAL`

All lifecycle callbacks run on the background multiplayer thread.
If your application has thread-sensitive code, hand work off to your own queue or event loop from inside the callback.
The blocking public methods `mutate()`, `mutate_workbook()`, `replace_workbook_json()`,
`reconnect()`, and `close()` are intentionally not callback-safe.
If you need to call them in response to a callback, schedule that work onto a
different thread or event loop first.

### Manual Reconnects

Call `reconnect()` to force a disconnect and resync:

```python
session.reconnect()
```

This method blocks until the session is back in `SYNCED` or fails terminally.
It intentionally reuses the normal disconnect path, which means:

- `on_disconnect(...)` fires first
- the session transitions through `RECONNECTING`
- `on_reconnect()` fires after sync completes again

By default, retryable disconnects also reconnect automatically.
Fatal authentication or protocol failures transition the session to `FATAL` and stop further retries.

## Notebook Replacement Errors

`replace_notebook()` uses Scout's `/api/workbooks/deserialize` route on the app host.
When that request fails, the client raises `NotebookDeserializeError`.

The most common cases are:

- `401`: the token is not valid
- `403`: the caller does not have access
- `422`: the notebook payload could not be deserialized

This client does not perform local conjure-to-frontend conversion.

## Builder Helpers

The existing workbook builder helpers still exist:

- `channel_variable()`
- `formula_variable()`
- `row()`
- `timeseries_panel()`
- `tab()`
- `layout_tab()`
- `chart_node()`
- `split_node()`

These are convenience helpers for constructing frontend workbook content.
They are most useful when your Python code is generating charts or layouts programmatically.

Example:

```python
from nominal.experimental.multiplayer import (
    InteractiveWorkbookSession,
    WorkbookSession,
    channel_variable,
    row,
    tab,
    timeseries_panel,
)

variables = {
    "speed": channel_variable(
        "speed",
        channel_name="vehicle.speed",
        data_source_ref_name="primary",
        run_rid="ri.scout.main.run.1234",
    ),
}

tabs = [
    tab("Flight Data", panels=[
        timeseries_panel(rows=[row(["speed"], title="Speed")]),
    ]),
]

with WorkbookSession.create(workbook, access_token=access_token) as session:
    session.set_workbook(variables, tabs)
```

## Recommended Usage Pattern

For most application code:

1. Open an `InteractiveWorkbookSession`
2. Read the initial snapshot with `get_snapshot()` or `get_workbook_json()`
3. Register one or more selector subscriptions for the specific fields you care about
4. Use `mutate_workbook()` for incremental changes
5. Use `replace_notebook()` when an upstream system has already produced a full conjure notebook

## Out of Scope

This first-pass client intentionally does not provide:

- awareness or presence publishing
- local conjure-to-frontend workbook conversion without the deserialize endpoint

## Current Scope and Limitations

This is still an experimental library.
Current design assumptions:

- the Galaxy multiplayer service is the source of truth
- awareness/presence is not exposed yet
- reconnect is supported for retryable disconnects
- the session uses one explicit personal access token for websocket and frontend API auth
- `replace_notebook()` and `serialize_workbook_json()` use Scout's `/api/workbooks/{deserialize,serialize}` routes

If you are writing downstream automation, prefer building on `InteractiveWorkbookSession`.
If you discover a workflow that requires direct CRDT access, drop to `WorkbookSession` for that specific operation.

## Detailed Lifecycle Diagrams

This section is intentionally a little more explicit than the rest of the guide.
It is meant to help a new contributor understand how the session actually moves
through connection, sync, update, reconnect, and shutdown.

### Layering

At a high level, the package has one transport-oriented layer and one
Python-native convenience layer:

```text
+---------------------------------------------------------------+
| InteractiveWorkbookSession                                    |
|                                                               |
| - get_snapshot() / get_workbook_json()                        |
| - mutate_workbook() / replace_workbook_json()                 |
| - replace_notebook()                                          |
| - subscribe()                                                 |
| - lifecycle property and callback passthrough                 |
+-------------------------------+-------------------------------+
                                |
                                v
+---------------------------------------------------------------+
| WorkbookSession                                               |
|                                                               |
| - websocket connect / close                                   |
| - sync handshake                                               |
| - reconnect supervisor                                         |
| - low-level mutate(Map)                                        |
| - document update callbacks                                    |
+-------------------------------+-------------------------------+
                                |
                                v
+---------------------------------------------------------------+
| Multiplayer websocket room                                    |
| room key = workbook RID                                       |
| shared root = {snapshotRid, workbook}                         |
+---------------------------------------------------------------+
```

The important design point is that `InteractiveWorkbookSession` does not manage
its own socket or reconnection policy.
It delegates all lifecycle behavior to one underlying `WorkbookSession`.

### Initial Create Flow

When you call `InteractiveWorkbookSession.create(workbook, access_token=...)` or
`WorkbookSession.create(workbook, access_token=...)`, the returned object is already synced or the
call raises.

```text
Caller
  |
  | create(workbook, access_token)
  v
+-------------------------+
| WorkbookSession.create  |
| - derive ws_base_url    |
| - validate access_token |
+-----------+-------------+
            |
            v
+-------------------------+
| WorkbookSession.__init__|
| - create Doc           |
| - start event loop     |
| - start supervisor     |
| - wait on first sync   |
+-----------+-------------+
            |
            v
+-------------------------+
| supervisor loop         |
| state = CONNECTING      |
+-----------+-------------+
            |
            v
+-------------------------+
| websocket connect       |
| using auth subprotocol  |
+-----------+-------------+
            |
            v
+-------------------------+
| state = SYNCING         |
| send SyncStep1          |
| resend every 2 seconds  |
| apply incoming updates  |
| wait for SyncDone       |
+-----------+-------------+
            |
            v
+-------------------------+
| state = SYNCED          |
| resolve create() wait   |
| send SyncDone ack       |
+-----------+-------------+
            |
            v
returned session
```

This means:

- `create()` is intentionally blocking
- reads and writes are only expected after the first sync completes
- callers do not need to manually wait for a ready event after construction

### Steady-State Update Flow

Once the session is synced, inbound server updates and local mutations both flow
through the same shared `Doc`.

```text
Server UPDATE / STEP2
  |
  v
+-----------------------------+
| WorkbookSession._handle()   |
| - decode room message       |
| - apply Yjs update to Doc   |
+--------------+--------------+
               |
               v
+-----------------------------+
| WorkbookSession.on_update   |
| callbacks fire              |
+--------------+--------------+
               |
               v
+-----------------------------+
| InteractiveWorkbookSession  |
| _notify_subscribers()       |
+--------------+--------------+
               |
               v
selector callbacks / full snapshot callbacks
```

Local writes go the other direction:

```text
mutate_workbook() / replace_workbook_json()
  |
  v
+-----------------------------+
| InteractiveWorkbookSession  |
| builds plain Python payload |
+--------------+--------------+
               |
               v
+-----------------------------+
| WorkbookSession.mutate()    |
| - mutate Doc transaction    |
| - compute Yjs diff          |
| - send UPDATE message       |
+--------------+--------------+
               |
               v
multiplayer room
```

### Automatic Reconnect Flow

If a synced or syncing socket drops in a retryable way, the supervisor keeps the
same `Doc`, records the close, and drives reconnection.

```text
connected session
  |
  | retryable close / transient failure
  v
+-----------------------------+
| supervisor records close    |
| last_close_code/reason set  |
+--------------+--------------+
               |
               v
+-----------------------------+
| on_disconnect(code, reason) |
| fires first                 |
+--------------+--------------+
               |
               v
+-----------------------------+
| state = RECONNECTING        |
| sleep with bounded backoff  |
| 1s -> 2s -> 5s -> 10s cap   |
+--------------+--------------+
               |
               v
+-----------------------------+
| reconnect websocket         |
| reuse access_token          |
+--------------+--------------+
               |
               v
+-----------------------------+
| state = SYNCING             |
| repeat sync handshake       |
+--------------+--------------+
               |
               v
+-----------------------------+
| state = SYNCED              |
| on_reconnect() fires        |
+-----------------------------+
```

Important consequences:

- the `Doc` survives reconnects
- selector subscriptions survive reconnects
- reconnect does not rebuild the high-level session object
- reconnect keeps using the explicit personal access token

### Manual Reconnect Flow

`reconnect()` is not a separate codepath.
It intentionally forces the normal disconnect path, then waits for the normal
reconnect path to finish.

```text
caller invokes reconnect()
  |
  v
+-----------------------------+
| mark manual reconnect       |
| register waiter             |
+--------------+--------------+
               |
               v
+-----------------------------+
| close current websocket     |
+--------------+--------------+
               |
               v
normal disconnect handling
  |
  +--> record close metadata
  |
  +--> fire on_disconnect(...)
  |
  +--> state = RECONNECTING
  |
  +--> reconnect + sync
  |
  +--> state = SYNCED
  |
  +--> fire on_reconnect()
  |
  +--> resolve reconnect() waiter
```

That means the public behavior is:

- `reconnect()` blocks until the session is synced again
- `on_disconnect(...)` does fire for manual reconnect
- `on_reconnect()` fires after the new sync completes

### Fatal Error Flow

Some failures are intentionally treated as terminal and stop the lifecycle.
Examples include authentication failures and protocol-invalid websocket states.

```text
fatal handshake / fatal close / fatal protocol error
  |
  v
+-----------------------------+
| classify as FATAL           |
+--------------+--------------+
               |
               v
+-----------------------------+
| state = FATAL               |
| fail initial create wait    |
| fail pending reconnect wait |
| fire on_fatal_error(exc)    |
+--------------+--------------+
               |
               v
no more reconnect attempts
```

Once a session reaches `FATAL`, it is terminal.
Callers should create a brand new session instead of trying to revive the old one.

### Shutdown Flow

Closing is also explicit and terminal:

```text
caller invokes close()
  |
  v
+-----------------------------+
| mark close requested        |
| cancel manual reconnects    |
| close socket if present     |
+--------------+--------------+
               |
               v
+-----------------------------+
| supervisor exits            |
| state = CLOSED              |
+--------------+--------------+
               |
               v
+-----------------------------+
| stop event loop thread      |
| close loop resources        |
+-----------------------------+
```

After `close()`:

- the session will not reconnect again
- `reconnect()` should be considered invalid
- downstream code should create a fresh session if it still needs live access
