# Review calibration

When changing the Claude review instructions, replay these cases in a read-only review
without posting comments. Include enough surrounding code to establish the contract.
Record the model, instructions revision, findings, and misses; do not treat a prose
checklist or a valid workflow file as proof of model performance.

`AGENTS.md`, imported by `CLAUDE.md`, routes to the owning guides in `docs/development/`.
Confirm both the local
implementation session and review session can identify that source and summarize a test,
wrapper, and docstring rule. Plugin reviewers must receive the applicable guides too. Check
that local implementation can run validation while the CI reviewer remains static/read-only.

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
| A production-only diff adds branching behavior but no tests or docs. | Follow the test route and, for public contract changes, the documentation route; do not select policies solely by changed file extension. |
| A plugin reviewer receives only the root routing index. | Load/pass the applicable linked guides before concluding the review; report incomplete coverage if they cannot be read. |

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

## Shared-policy cases

| Case | Expected judgment |
| --- | --- |
| A contributor uses a justified alternative to a rule labeled "prefer". | Do not turn a default into a blocker; require a concrete defect or maintenance cost for a finding. |
| A PR removes the refresh convention while adding a competing refresh implementation. | Review against the base branch's convention and surface the proposed policy change for human review; do not silently grant an exemption. |
| A human accepted a one-off exception on another PR. | Treat it as context, not a repository-wide policy change; assess whether its rationale applies here. |
| A new rule differs from substantial existing code. | Review its scope and adoption plan; do not demand unrelated repository-wide cleanup from a feature author. |

## Python and SDK contract cases

| Case | Expected judgment |
| --- | --- |
| New code uses an API introduced after the supported Python floor, despite passing on the author's interpreter. | Identify the incompatible API and use an existing backport/helper or compatible implementation; do not demand newer syntax as a style cleanup. |
| An agent requests 79-column reformatting or NumPy docstrings in this repo. | Follow the configured 120-column Ruff/Google conventions; do not rewrite to a generic default. |
| A fixed-size thread pool eagerly queues one future with a large buffer for every input file. | Inspect total retained data; worker count alone does not establish bounded memory. |
| A refreshable resource gains a cached property derived from metadata without invalidation. | Trace a refresh that makes the cached value stale; prefer the established state/refresh owner. |
| A lossless timestamp conversion goes through float seconds. | Show the nanosecond precision loss and use the canonical integer conversion; ordinary datetime conversion has a different contract. |
| A request wrapper adds retries around a transport that already retries. | Establish the combined budget and replay semantics before flagging; a documented recovery gap can justify another layer. |
| A test inspects request fields to protect absent/false/empty protobuf semantics. | Keep the contract test; do not dismiss it as implementation coupling. |
| A cohesive module grows from 995 to 1005 lines, without a useful separate ownership boundary. | Size alone is not a finding; do not manufacture an extraction to satisfy a threshold. |
| Two short constructors differ in omission/empty semantics. | Keep direct implementations unless a shared abstraction demonstrably simplifies both contracts. |
| A dependency-free exception lives in an orchestration module and forces a runtime import cycle. | Identify the dependency edge removed by moving the definition to the existing exception owner; preserve public aliases and class identity. |
| A callback discards a failed future, or shutdown closes an executor before its producer stops submitting. | Trace a concrete lost failure or shutdown race and the caller-visible completion contract. |
| Forced overwrite deletes a destination that aliases the source. | Flag destructive preflight ordering; protect same-path and relevant filesystem-alias cases before unlinking. |
| A test uses a small fake instead of `MagicMock`, or a boundary mock protects request mapping. | Judge the behavior protected and setup complexity, not the choice of test-double tool. |
| Removing a JSON dependency changes persisted keys from camelCase to snake_case. | Require compatible reads or an explicit migration; test an old-format fixture, not only a new-format round trip. |
| An API already uses `0` for unbounded queues; a reviewer prefers `None`. | Preserve the established contract; preference alone does not justify a signature/default migration. |

Historical examples: [#340](https://github.com/nominal-io/nominal-client/pull/340#discussion_r2094977681)
preserves method aliases; [#427](https://github.com/nominal-io/nominal-client/pull/427)
discusses worker ownership, public exports, and established queue defaults;
[#524](https://github.com/nominal-io/nominal-client/pull/524#discussion_r2543615138)
distinguishes a typing nicety from a blocker. Dependency removal in
[#803](https://github.com/nominal-io/nominal-client/pull/803) required the persisted-state
compatibility repair in [#823](https://github.com/nominal-io/nominal-client/pull/823).
These examples supply context, not blanket adoption of every historical suggestion.
