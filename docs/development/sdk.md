# SDK contracts and resource wrappers

These conventions apply to new and changed code. Read the affected implementation and
callers alongside this guide; examples do not require unrelated code to be normalized.

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
