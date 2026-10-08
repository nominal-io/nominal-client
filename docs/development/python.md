# Python implementation quality

Default to idiomatic, maintainable, well-typed, efficient Python. Use
[PEP 8](https://peps.python.org/pep-0008/) and
[PEP 257](https://peps.python.org/pep-0257/) as the baseline, with this repository's
explicit choices taking precedence. This is guidance for authoring good code, not a
request for reviewers to repeat formatter diagnostics or normalize untouched files.

## Compatibility and readable code

- Read [pyproject.toml](../../pyproject.toml): the supported floor is Python 3.10,
  Ruff uses 120 columns and Google docstrings, and mypy is strict. A successful run on
  your newest local interpreter does not establish compatibility. Use existing
  `typing_extensions` and `exceptiongroup` backports where needed; do not introduce
  newer syntax or standard-library APIs without a compatible path. For example, the
  local [batched helper](../../nominal/_utils/iterator_tools.py) supports the older floor.
- Prefer descriptive names, explicit control flow, and small cohesive functions. A reader
  should be able to follow inputs, side effects, and results without tracing a framework.
  Name intermediate request/result values when nested construction hides those stages.
  Use comprehensions for simple transformations, ordinary loops for branching or side
  effects, and context managers for resources. Fewer lines is not itself a simplification.
- Annotate new function interfaces. Prefer built-in collection generics and `T | None`
  in new code; keep imports consistent with the module and let Ruff order them. Use
  `Iterable`, `Sequence`, or `Mapping` according to the operations actually required:
  an iterable may be one-shot, while a sequence promises indexing and repeated traversal.
- Model meaningful states with focused types, protocols, and discriminated unions when
  they simplify callers. Avoid broad `Any`, `getattr` fallbacks, or casts that conceal a
  broken boundary. A narrow cast/overload or generated-binding exception can be warranted;
  explain the invariant instead of building a generic framework to eliminate every cast.
- Prefer top-level imports. Local imports can protect optional dependencies or resolve a
  real cycle; `TYPE_CHECKING` is only for dependencies not needed at runtime. Deferred
  annotations do not eliminate runtime name lookup by reflection or documentation tools.
- Comments should explain a constraint, invariant, or non-obvious decision. Avoid narrating
  straightforward code, speculative future-proofing, and broad suppression of lint/type
  errors. Use the existing narrow configuration for untyped dependencies where appropriate.

## Ownership and library behavior

- Keep generic Python utilities in [nominal/_utils](../../nominal/_utils/README.md) and
  transport/API helpers in [nominal/core/_utils](../../nominal/core/_utils/README.md).
  Feature-specific helpers stay with their feature until actual callers justify sharing.
- `nominal` is a namespace shared by multiple distributions, including generated
  `nominal.protos` and the [TDMS extension](../../packages/nominal-tdms/pyproject.toml).
  Do not add `nominal/__init__.py`. Add dependencies to the owning distribution and
  preserve platform/extra guards; an optional integration must not make unrelated imports
  fail. Do not promote experimental APIs or dependencies into the core as incidental cleanup.
- Justify new runtime dependencies by the capability they provide and their installation,
  version-resolution, and maintenance costs. Prefer the standard library for small, direct
  implementations; use optional extras for optional integrations. Base version bounds on
  required features and compatibility, and explain exact runtime pins. Reproducible tool
  locks and generated-binding compatibility constraints serve different purposes.
- Export public APIs from the appropriate package, following
  [nominal/core/__init__.py](../../nominal/core/__init__.py). Use leading underscores for
  implementation privacy, not new per-module export inventories. Preserve public import
  paths and object identity when moving implementations or adding compatibility aliases.
- Library code uses module loggers; CLI/application setup owns handlers, verbosity, and
  process exit. Keep ordinary SDK calls usable inside another application. An explicit
  runner entrypoint may own exit/reporting behavior, as the extractor runner does.
- Use lazy logging arguments for hot paths and avoid computing expensive diagnostics when
  disabled. Never add credentials or signed URL query strings to diagnostics. Reuse the
  existing deprecation helpers and narrowly scoped warning conventions; do not substitute
  logging, repeated warnings, or new exception classes without considering caller behavior.

## Performance follows the contract

- Consider algorithmic cost, request count, allocation/copy volume, and maximum resident
  data before tuning syntax. Avoid repeated lookups or per-item network calls when a
  compatible batch operation exists. Preserve ordering, laziness, and public return types;
  replacing a list result with a generator is an API change, not a free optimization.
- Reuse [pagination helpers](../../nominal/core/_utils/pagination_tools.py) and existing
  batching. Do not materialize an entire stream merely to count or partition it. Count
  buffered items/bytes and pending futures as well as active workers: a fixed worker count
  alone does not bound memory. Keep explicit unbounded modes deliberate and documented.
- Trace retry ownership through [HTTP](../../nominal/core/_utils/networking.py),
  [gRPC](../../nominal/core/_utils/grpc_tools.py), and the operation's existing retry layer
  before adding another loop. Budget total attempts and time, distinguish transient errors
  from permanent failures, and establish replay safety. Upload/migration layers may need
  additional recovery; "the transport retries" is not proof that every failure is covered.
- Concurrent code needs explicit ownership of queues, files, workers, cancellation, and
  shutdown. Preserve failure propagation and the documented difference between queued,
  uploaded, and ingested data. Use monotonic time for elapsed deadlines. Do not make a
  path concurrent just because calls look independent; account for ordering and shared state.
- Keep worker-local state inside its worker where practical, with explicit queue/event/lock
  communication. Prefer stable configuration and references over shared mutable attributes;
  a frozen wrapper around mutable state does not establish thread safety. Prefer blocking
  synchronization over sleep-based polling when the producer can signal readiness. Trace
  future failures to the caller and stop producers before closing the executors they submit to.
- Cache only with a defined lifetime, invalidation policy, and concurrency contract.
  Frozen resource wrappers can refresh in place. The default-workspace cache in
  [ClientsBunch](../../nominal/core/_clientsbunch.py) is deliberately client-scoped;
  do not replace it with a global cache or add redundant resolution to each resource.
- For performance claims, compare equivalent inputs and outputs and measure the relevant
  cost (requests, peak memory, throughput, or latency). Keep deterministic correctness tests
  separate from machine-dependent timing assertions. Profiling supports complex optimization;
  an obvious avoidable full copy or quadratic scan need not wait for a benchmark to improve.

These are authoring defaults and review criteria, not a claim that all existing paths meet
them. The source links identify ownership and constraints; they are not endorsements of
every implementation detail in those files. Judge deviations by concrete consequences.
