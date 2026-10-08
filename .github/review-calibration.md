# Review calibration

When changing the Claude review instructions, replay these cases in a read-only review
without posting comments. Include enough surrounding code to establish the contract.
Record the model, instructions revision, findings, and misses; do not treat a prose
checklist or a valid workflow file as proof of model performance.

| Case | Expected judgment |
| --- | --- |
| A resource update duplicates field copying even though its existing refresh mixin accepts the authoritative response. | Flag the competing refresh path; point to `_refresh_from_api` and any context/identity requirement. |
| A session has mutable lifecycle state and is not a resource snapshot. | Do not demand a frozen dataclass or resource refresh mixin. |
| `labels or existing_labels` replaces a documented distinction between `None` (inherit) and `[]` (clear). | Flag the reachable empty-list regression and missing regression coverage together. |
| A thin endpoint wrapper has no dedicated test. | Do not ask for a forwarding-only test. |
| A test configures a mock return value and only asserts the mocked method returns it. | Flag the tautological test; suggest deletion or testing the actual SDK behavior. |
| A test asserts omitted versus empty request fields and verifies the same SDK object receives returned metadata. | Keep it: request shape and refresh identity are meaningful boundary contracts. |
| Equivalent timestamp cases use parameterization; a clock test uses `monkeypatch`. | Do not demand separate tests or `MagicMock` merely for stylistic uniformity. |
| A new test has no behavioral docstring. | Cite the test convention tersely; group repeated instances rather than flooding the PR. |
| A public method clears labels on `[]`, but its docstring promises inheritance. | Flag the caller-visible documentation mismatch. |
| A nested argument note loses its caveat or renders as ordinary broken markup. | Flag with parser/output evidence; preserve the note near the argument. |
| Existing MkDocs docstrings use Markdown backticks while another branch migrates to Sphinx. | Do not force reStructuredText on the current branch. Inspect its actual renderer. |
| A generic helper adds mode flags and casts to combine two simple, different request shapes. | Propose direct construction if it demonstrably deletes complexity; do not reflexively demand deduplication. |
| A justified local import avoids a real dependency cycle. | Do not demand a new marker class or architectural layer merely to move the import. |
| Documentation says `uv run python -m pytest`; a subprocess uses `sys.executable`. | Do not flag bare Python usage. |
| Review runs out of context before inspecting changed tests or relevant callers. | Report incomplete review with the reviewed SHA and missing scope, never a clean verdict. |

The refresh and test-value cases reflect human feedback on
[PR #1026](https://github.com/nominal-io/nominal-client/pull/1026).
The abstraction and exception-boundary cases reflect
[PR #989](https://github.com/nominal-io/nominal-client/pull/989) and
[PR #986](https://github.com/nominal-io/nominal-client/pull/986).
Note rendering follows the compatibility work in
[PR #1027](https://github.com/nominal-io/nominal-client/pull/1027).

Before rollout, validate the workflow with actionlint and inspect the prompt diff for
changes to permissions, triggers, models, and posting behavior. After rollout, compare
Claude findings with subsequent human review on representative PRs: missed actionable
comments and false positives both matter. A quiet review applies only to its recorded
head SHA; this workflow does not rerun automatically on every pushed commit.
