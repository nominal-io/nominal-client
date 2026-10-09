---
name: thermo-nuclear-code-quality-review
description: Run an extremely strict maintainability review for abstraction quality, giant files, and spaghetti-condition growth. Use for a thermo-nuclear code quality review, thermonuclear review, deep code quality audit, or especially harsh maintainability review.
x-atlas-id: 8bc3dd07-f2f1-448b-b6b2-a6908d3e5cf5
visibility: public
category: code-review
departments:
  - epd
tags:
  - code-review
  - maintainability
  - refactoring
  - code-quality
---

# Thermo-Nuclear Code Quality Review

Run an unusually rigorous structural review. Look beyond local polish for changes that
preserve behavior while deleting whole branches, competing implementations, modes, or
layers. Be ambitious about simplification and precise about why it helps.

## Establish the contract

Read `AGENTS.md` and the applicable owning guides before reviewing. Inspect neighboring
implementations, callers, and existing helpers, not just the changed lines. Distinguish
merged requirements from proposed policy and author preferences. A review-only request
does not authorize edits or posting comments; follow the invoking workflow's execution rules.

Identify the behavior and public contracts that must survive any restructuring, including
identity, omission versus clearing, failure propagation, and completion semantics.

## Search for substantial simplification

For each meaningful change:

1. Find the existing owner. Trace duplicate conversion, refresh, validation, exception,
   and lifecycle paths. Search for a canonical implementation before suggesting a helper.
   A dependency-free definition in an orchestration module can be the wrong boundary even
   when a local import avoids the immediate cycle; identify the dependency edge to remove.
2. Count concepts, not lines. Look for mode flags, casts, scattered feature checks, and
   pass-through layers that make callers understand implementation details. Name the code
   that could disappear with a simpler model or ownership boundary.
3. Examine growing modules for independent responsibilities. Crossing 1,000 lines is an
   investigation prompt, not a blocker or an automatic extraction request. A split should
   improve cohesion or dependency direction, not merely distribute the same complexity.
4. Challenge abstractions as well as duplication. Two short implementations with different
   contracts can be clearer than a generic helper with unions, modes, and casts. Ordinary
   branches are appropriate for genuinely different behavior. Extract only when the result
   reduces the reader's work and preserves those differences.
5. Trace state and resource ownership through orchestration. Account for partial failure,
   cancellation, queued work, and shutdown. Parallel execution needs a demonstrated benefit
   and safe ordering, bounded resources, and shared-state handling; it is not inherently a
   simplification. Do not promise atomicity that the underlying service cannot provide.

Prefer removing indirection, reusing an existing owner, or narrowing a boundary before
adding a new abstraction. A focused helper or module is valuable when it has a coherent
responsibility, even with one caller. Keep a few repeated lines when that is clearer than
a shared layer. Avoid infrastructure whose only purpose is imagined future flexibility.

## Evidence bar

For every structural finding, provide:

- The changed code and what makes it harder to understand: unnecessary concepts, competing
  sources of truth, avoidable coupling, or an obscured invariant.
- A viable simpler alternative and the branches, layers, or dependencies it removes.
- The contracts that alternative must preserve and any tradeoff requiring human judgment.

Request changes when a clearly better implementation substantially simplifies the same
behavior, or when unnecessary wrappers, defensive scaffolding, or test machinery obscure it.
Working code is not sufficient for approval; a predicted bug or quantified maintenance cost
is not required to uphold this design bar. Show a viable alternative and explain the gain.

Keep equally good alternatives and speculative rewrites as suggestions. File size or reuse
count alone does not establish a design problem. Match severity to the finding: a required
design improvement is not automatically an urgent correctness defect. Stay within the change
and preserve accepted decisions unless new evidence warrants revisiting them.

## Report and verify

Prioritize supported structural regressions and substantial simplifications over cosmetic
nits. Use a small number of actionable findings, with exact locations and concrete remedies.
If inspection supports no findings, say so; do not invent work to make the review seem strict.
Identify uninspected scope and unresolved tradeoffs instead of issuing an unqualified approval.

When implementing an authorized simplification, inspect the final diff for complexity actually
removed and validate the affected contracts according to `AGENTS.md`. Passing tests alone
neither proves a better design nor authorizes a behavior change.
