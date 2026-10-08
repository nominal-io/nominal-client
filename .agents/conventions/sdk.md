# SDK contracts and resource wrappers

These conventions apply to new and changed code. Read the affected implementation and
callers alongside this guide; examples do not require unrelated code to be normalized.

## Exposing a new API object

Start with the caller's workflow and the generated response type. Find a neighboring
wrapper using the same transport and lifecycle; copy its responsibilities, not its whole
shape. [Attachment](../../nominal/core/attachment.py) is a small Conjure example and
[Secret](../../nominal/core/secret.py) shows gRPC. A value object without identity or remote
state does not need service clients or refresh machinery.

1. **Define the SDK shape.** Expose useful Python fields and operations, keeping generated
   models internal. Decide what the response actually tells us: absent metadata is not
   necessarily an empty value, and a search summary may differ from a full resource.
   Avoid hidden network calls just to populate a field during conversion.
2. **Convert in one place.** Put `_from_conjure` or `_from_proto` on the wrapper and reuse
   it from every construction path. For refreshable resources, use the appropriate mixin
   described below. Preserve context the response omits; [ContainerImage](../../nominal/core/container_image.py)
   carries its workspace through conversion and refresh explicitly.
3. **Wire only what is needed.** Declare the services used by the wrapper in its `_Clients`
   protocol. If the service is new to the SDK, add it to
   [ClientsBunch](../../nominal/core/_clientsbunch.py) and its existing factory construction.
   Reuse that transport setup so authentication, configuration, and retries stay centralized.
4. **Make the object reachable.** Add the relevant client or parent-resource method and
   export the public type from [nominal.core](../../nominal/core/__init__.py). Check existing
   construction paths such as search, clone, and template creation when extending an object.
   Add only the entry points the feature needs, not a complete CRUD surface by habit.

Document how callers obtain and use the object. Test the conversion or lifecycle behavior
that could go wrong: field presence, units, context preservation, or refreshing the same
instance. [Container image tests](../../tests/core/test_container_image.py) show these cases;
there is no need to reproduce their entire suite for a smaller object.

## Adding an API route

Read the generated request, response, and service method before designing the Python
signature. If the installed bindings lack the route, follow the
[API upgrade skill](../skills/reviewing-nominal-api-bumps/SKILL.md) rather than inventing a
parallel HTTP client or editing generated code.

Put an operation on the object whose identity it uses: `secret.update(...)` rather than
making callers pass its RID back to the client. Creation, lookup, and search usually belong
on `NominalClient`; child operations may belong on their parent resource. Follow neighboring
names and accept SDK objects or RIDs where that is already the convention.

Keep the implementation easy to trace: normalize SDK inputs, construct the request, call
the service, and convert the response. Use the existing workspace resolver, pagination,
and gRPC error translation where applicable. Update methods should use the authoritative
response and refresh path rather than issue a second fetch when the response is sufficient.
Check the omission, empty-value, and failure contracts below before choosing defaults.

Describe caller-visible behavior in the docstring, including what completion means for
asynchronous operations. Test meaningful SDK decisions rather than forwarding alone:
[secret tests](../../tests/core/test_secret.py) cover omitted versus cleared fields,
error translation, and pagination. Follow the [test](testing.md) and
[documentation](documentation.md) guides, and use the validation commands in `AGENTS.md`.

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
- Expose the smallest useful public API for the caller's workflow. Keep transport-only
  arguments and orchestration helpers private rather than exporting every backend operation.
  A cleanup must preserve established aliases and signatures unless explicitly deprecated.
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
- Validate known local preconditions before uploads or destructive changes. For overwrite
  operations, account for input/output aliases before deleting a destination. Serialization
  refactors must preserve readable persisted state, including field names and defaults;
  removing a dependency does not remove its compatibility obligations.

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
