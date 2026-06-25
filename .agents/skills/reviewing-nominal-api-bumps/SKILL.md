---
name: reviewing-nominal-api-bumps
description: Use when bumping nominal-api or nominal-api-protos (a "transport" version bump / dependency upgrade) and you need to know what changed in the generated bindings and what in nominal-client must change. Covers conjure (nominal-api) and proto (nominal-api-protos). Triggers on questions like "what changed between version X and Y", "is this bump safe", "breaking changes", migration impact.
visibility: public
category: dependency-migration
tags:
  - nominal-api
  - protos
  - conjure
  - migration
  - breaking-changes
  - dependency-bump
---

# Reviewing nominal API version bumps

Compare two published versions of the generated Nominal bindings and report what changed
and what it costs `nominal-client` to migrate. Works for both transports:

- **conjure** → `nominal-api` (the Conjure HTTP bindings; one big `nominal_api/_impl.py`)
- **proto** → `nominal-api-protos` (gRPC/protobuf; `nominal/protos/**/*_pb2.pyi` + `*_pb2_grpc.py`)

The output classifies every delta as **⚠️ Breaking**, **➕ Additive**, or **ℹ️ Behavioral**, and —
critically — scans the client so a breaking change with **0 references** is flagged as a safe bump,
while an additive with 0 references is flagged as a new capability that isn't wired up yet.

## When to use

- Bumping the `nominal-api` / `nominal-api-protos` pin in `pyproject.toml`.
- Answering "what changed between 0.1282.0 and 0.1286.0?" or "is this bump safe to take?"
- Deciding what migration work a dependency upgrade implies before doing it.

Not for: editing the client to perform the migration (this skill only reports), or non-Nominal deps.

## Quick reference

```bash
SKILL=.agents/skills/reviewing-nominal-api-bumps

# One-shot: pinned-in-pyproject -> latest on PyPI, scanning this repo
python $SKILL/bump_scan.py conjure --repo .
python $SKILL/bump_scan.py proto   --repo .
python $SKILL/bump_scan.py both    --repo . --out ./reports

# Explicit versions
python $SKILL/bump_scan.py conjure --repo . --old 0.1282.0 --new 0.1286.0
```

`bump_scan.py` resolves versions (old = pin in `pyproject.toml`, new = latest on PyPI), downloads and
unpacks both wheels, extracts each surface, diffs them, scans `nominal/` + `tests/` for references,
prints the report, and writes `migration-report-<transport>-<old>-to-<new>.md` to `--out` (default `.`).

## Workflow

1. **Run the scan.** Use `bump_scan.py <transport> --repo <client-root>`. Default `--out` is the cwd;
   prefer writing the report to a scratch/temp dir so it doesn't dirty the repo. For a full bump,
   run `both`.
2. **Read the verdict line.** `✅ No breaking change references the client` ⇒ the pin bump is safe on
   its own. `⚠️ N breaking change(s) touch the client` ⇒ open the Breaking table and address each.
3. **Work the Breaking table.** For each row with non-zero **Client refs**, open the listed
   `file:` paths and update call sites for the exact change in the `Change` column
   (e.g. `~ inputs: List→Dict[str,…]`, `− fill_strategy + alignment_configuration (req)`).
4. **Skim Additive for opportunities.** Additive rows with non-zero refs mean the client already
   touches that type and could adopt the new optional field/endpoint. Rows with 0 refs are net-new
   capabilities — adopt only if there's a reason.
5. **Behavioral** rows are enum members added to *existing* enums — revisit any exhaustive
   `match`/`if` over that enum in the client.
6. **Relay** the verdict + the Breaking table inline, and link the written report path.

## Interpreting the report

- **Client refs** are textual matches of the element's **leaf name** in `nominal/` + `tests/`. They are
  *candidates*, not proof: common names (`File`, `Error`, `State`) over-match unrelated code — verify the
  import context before trusting a count. A field-level change (e.g. a removed field) is scanned by the
  enclosing type's name, so also `grep` the field name when triaging.
- Rows whose key contains `Internal` are internal/server-side services, sorted last; they rarely appear
  in the public client.
- `Visitor` classes are skipped — a change to a union shows up on the union row itself.

## The tools (run individually if needed)

`bump_scan.py` calls these in-process; run them standalone only for a custom flow.

- `extract_surface.py {conjure|proto} <unpacked-wheel-root>` → the structured surface as **JSON Lines**
  (one element per line, sorted by key). Line-diffable and greppable; types kept fully qualified so
  `List→Dict` is visible. Importable as `extract(transport, root) -> list[Element]`.
- `diff_surface.py <old.jsonl> <new.jsonl> --label "..." [--repo <root>]` → the markdown report. Classifies
  field-level deltas (removed field / new required field / type change / optional↔required) and renders
  the three severity tables. Importable as `build_report(old, new, label, repo) -> str`.

## Common mistakes

- **Trusting a ref count for a generic name.** `Error`/`File`/`State` over-match. Open the file.
- **Treating an additive optional field as breaking.** New `Optional[...]` fields and new endpoints
  never break existing callers — they live in the Additive table for a reason.
- **Forgetting `pip index versions` is needed for the default `new`.** If offline or it fails, pass
  `--new` explicitly.
- **Running `proto` against the conjure wheel (or vice-versa).** The transport selects both the PyPI
  package and the extractor; don't mix them.
