# SDK contracts and resource wrappers

These conventions apply to new and changed code. Read the affected implementation and
callers alongside this guide; examples do not require unrelated code to be normalized.

## Resource and API contracts

- Preserve public signatures and behavior across releases; deprecate before removal.
  Treat defaults, mutation, return identity, exception types, and blocking/completion
  semantics as API contracts. Identify intentional migrations explicitly rather than
  silently changing them. Prefer required positional parameters and keyword-only
  optional configuration for new APIs; do not break existing signatures to normalize them.
- Preserve `None` versus explicit-empty semantics. Omitted values often delegate defaults
  to the backend, while empty collections clear a value. Avoid `value or default` when
  it collapses these states. Do not invent client-side defaults or validation owned by
  the service without a demonstrated client contract.
- Resource snapshots follow the existing frozen dataclass pattern: a narrow nested
  `_Clients` protocol, `_clients` excluded from repr, and centralized `_from_conjure`
  or `_from_proto` conversion. Keep generated transport objects behind the public SDK
  boundary. Builders, sessions, and value objects need not be resource snapshots.
- Reuse `RefreshableConjureMixin`, `RefreshableGrpcMixin`, or `RefreshableMixin` in
  `nominal/core/_utils/api_tools.py` as appropriate. Updates should refresh the same
  instance from the authoritative response through `_refresh_from_api`; do not add
  competing refresh helpers or call `update_dataclass` directly in update methods.
  Preserve context absent from transport responses, such as the object's workspace.
  See `nominal/core/attachment.py` and `tests/core/test_container_image.py`.
- Reuse existing RID, timestamp, pagination, label, and property conversion helpers.
  Keep transport conversion in the owning wrapper and domain validation in its owning
  layer. Translate only exceptions that the layer owns; preserve unrelated exceptions
  and their traceback rather than broadly catching and reclassifying them.
- Multi-step operations must have an explicit partial-failure contract. When a later
  request fails, account for already-created resources; do not imply atomicity the
  service does not provide. Recommend rollback only when its safety is established.

## Semantics that need explicit attention

- Frozen wrappers are snapshots with a supported in-place refresh path, not deeply
  immutable values. Keep read-only mappings/sequences consistent with neighboring wrappers;
  do not expose mutable transport collections or assume a cached derived value survives
  refresh. See [Attachment](../../nominal/core/attachment.py) and
  [refresh helpers](../../nominal/core/_utils/api_tools.py).
- Workspace selection is centralized. A resource's existing workspace, an explicit selector,
  the client's default, and an all-workspaces search are different contracts. Reuse
  [client selection](../../nominal/core/client.py) and
  [resolution](../../nominal/core/_clientsbunch.py); do not resolve a default during every
  construction or replace a resource's workspace merely because the client has another one.
- Protobuf presence is not truthiness: an absent field, zero, false, and an explicitly empty
  update can mean different things. Use the schema's `HasField`/`WhichOneof` semantics and
  existing update wrappers where applicable. Decide how unknown enum values behave at the
  specific boundary; read compatibility may differ from validating a new write. See
  [container image conversion](../../nominal/core/container_image.py).
- Timestamps and durations use different named aliases in [nominal.ts](../../nominal/ts/__init__.py),
  but both are runtime integers. Preserve units, UTC/relative meaning, and nanosecond precision
  using the existing conversion helpers. Do not round-trip integral nanoseconds through float
  seconds or datetime when losslessness is required; retain correct negative-time normalization.
  `datetime` conversion has a different precision contract and must not be described as lossless.
