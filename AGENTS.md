# Repository conventions

These conventions apply to new and changed code. Read neighboring implementations and
scoped instructions before choosing a pattern. Do not demand unrelated cleanup or undo
an explicitly agreed design decision without new evidence. Examples below illustrate
contracts; they are not templates every class or test must reproduce.

## Local development with AI

These instructions guide implementation and local review as well as CI review. Start with
`CONTRIBUTING.md` and `just --list` for setup and commands; do not maintain a second set of
build commands here. The root package is `nominal`; extension distributions live in
`packages/` and share the uv workspace.

1. Before editing, inspect the working tree, relevant callers/tests, and a neighboring
   implementation. Identify the existing owner of the behavior and any public contract
   being changed. For substantial design changes, explain the proposed boundary and what
   complexity it removes before implementing; keep routine fixes lightweight.
2. Implement the smallest cohesive change, updating behavior, meaningful tests, and
   caller-facing documentation together. Follow the conventions below while writing code,
   rather than waiting for a review to find violations. Preserve unrelated local changes.
3. Validate according to the change: focused `uv run pytest <path>` during iteration,
   `just verify` for Python changes before handoff, and `just build-docs` plus representative
   rendered-page inspection for documentation rendering changes. Instruction/workflow-only
   changes need link/configuration checks, not the Python suite. Report commands actually run,
   failures, and omitted checks; do not claim validation from inspection alone.
4. Before handing off substantial changes, review the final diff against the conventions
   below and use the thermo-nuclear review skill for structure. Fix supported findings and
   remove unnecessary scaffolding; report unresolved decisions and validation gaps. If asked
   for review only, keep the working tree unchanged. A local review does not authorize posting
   GitHub comments or changing PR state.

Skills live in `.agents/skills/`; `.claude/skills/` exposes the same files to Claude.
Use `reviewing-nominal-api-bumps` when changing `nominal-api` or `nominal-api-protos`, and
`thermo-nuclear-code-quality-review` for deep structural review. If skill invocation is not
available in the current tool, read the relevant `SKILL.md` directly.

The CI review job is explicitly static and read-only; its no-execution rule does not apply
to local implementation work. An explicit review-only/no-execution request still takes
precedence locally. Keep personal credentials and machine-specific preferences out of these
shared instructions.

## Public APIs and resource wrappers

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

## Tests that earn their maintenance cost

- Test observable behavior and meaningful contracts. For nontrivial new logic or fixes,
  cover the relevant regression, boundary, or failure case; identify the specific behavior
  that an omitted test would protect. Do not require coverage for thin endpoint forwarding,
  trivial accessors, or unchanged dependency behavior merely to increase test count.
- Do not add tests that only assert a mock returns its configured value, restate a helper's
  implementation, or check unmodified exception pass-through. Boundary request assertions
  are useful when they protect units, field mapping, omission versus clearing, pagination,
  or another real SDK contract. Assert the resulting public state/identity when it matters.
- Use real SDK objects and generated request/response models where practical; mock I/O
  boundaries rather than the method or conversion under test. Prefer `MagicMock` for service
  clients, configured only as needed. `monkeypatch` is appropriate for environment, clock,
  or process-global state; do not rewrite such tests just to replace the tool.
- Give each new or substantially changed test a one-line docstring describing the behavior
  it protects. Keep arrange/act/assert straightforward. Parameterize cases sharing the same
  behavior and assertion; separate cases with different setup or contracts instead of
  building a branching test interpreter.
- Keep fixtures and builders small and local until genuinely shared. Avoid large generated
  fixture matrices, global fixtures, or helper layers for a single straightforward case.
  Use deterministic inputs and controlled clocks/synchronization rather than network access,
  wall-clock assumptions, or arbitrary sleeps in unit tests.

## Docstrings and rendered documentation

- Public APIs need concise Google-style docstrings describing caller-visible behavior.
  Include `Args`, `Returns`, and `Raises` where they add information; simple properties or
  obvious accessors do not need boilerplate sections. Annotations own type declarations.
- Document consequential defaults, `None`/empty distinctions, units and time zones,
  in-place mutation, completion semantics, and actionable failure conditions. Examples
  must match the actual API. Do not narrate request-building internals as user documentation.
- Preserve notes and caveats, including those inside argument descriptions. Keep a caveat
  next to the parameter it qualifies and retain a distinct paragraph or supported admonition.
  Do not flatten useful notes just to appease a generator.
- Follow the renderer configured on the branch and any scoped docs instructions. Google
  section indentation matters; Markdown versus reStructuredText inline/code-block syntax
  is renderer-dependent. Do not mechanically normalize single/double backticks or migrate
  every docstring to fix a local rendering problem. Confirm how the parser handles the
  construct before alleging a defect.
- Prefer native documentation-generator features and package exports over custom parsers,
  duplicated API catalogs, or manually synchronized member lists. A workaround must solve
  a demonstrated limitation with a bounded scope. Rendering changes need representative
  output inspection, including notes, signatures, navigation, and links; a warning-free build
  alone does not establish correct presentation.

## Keep the implementation direct

- Reuse the canonical implementation before adding a helper. Small duplication is preferable
  to a generic abstraction that hides differing contracts or adds unions, modes, and casts.
  Point to the concrete code that could disappear when proposing a structural simplification.
- Keep feature-specific decisions in their owning module rather than scattering checks
  through shared paths. Do not add pass-through wrappers or inheritance layers without
  a clear responsibility. Apply the structural review skill in
  `.agents/skills/thermo-nuclear-code-quality-review/SKILL.md` with this evidence bar.
- Prefer top-level imports, but retain justified local imports for genuine cycles or optional
  dependencies. Preserve the `nominal` namespace package; do not add a root `__init__.py` to
  paper over an import problem. Use leading underscores for implementation privacy and
  explicit exports in the appropriate package `__init__.py`, not new per-module `__all__` lists.
- Repository development commands use `uv run` (or existing `just` recipes). `uv run python
  -m ...` and subprocesses using `sys.executable` are valid; do not flag them as bare Python.
  Ruff and mypy own mechanical formatting and type errors; review type boundaries rather
  than repeating their diagnostics.
