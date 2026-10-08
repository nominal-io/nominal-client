# Test authoring and review

These conventions apply to new and changed code. Read the affected implementation and
callers alongside this guide; examples do not require unrelated code to be normalized.

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
