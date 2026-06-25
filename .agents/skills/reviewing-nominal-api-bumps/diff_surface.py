#!/usr/bin/env python3
r"""Diff two structured API surfaces (from extract_surface) and render a migration report.

`build_report(old, new, label, repo)` is the entry point bump_scan calls in-process.
The CLI reads two JSONL surfaces (from `extract_surface.py`) and prints the report:

  diff_surface.py <old.jsonl> <new.jsonl> --label "nominal-api 0.1282.0 -> 0.1286.0" \\
      [--repo <client-root>] [--internal-substr Internal]

Each element is matched by key, then classified:
  REMOVED element ............................. BREAKING
  CHANGED element:
    removed field / type change / optional->required ... BREAKING
    new required field ................................. BREAKING
    new optional field / required->optional ............ additive
    enum: members removed .............................. BREAKING
    enum: members added (existing enum) ................ behavioral
  ADDED element ............................... additive

With `--repo`, scans <root>/nominal and <root>/tests for references by leaf name so a
breaking change with 0 call sites reads as a safe bump, and an unreferenced additive
reads as an unwired capability.
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from extract_surface import Element, Field, from_jsonl, use_utf8_stdout


@dataclass
class Row:
    """One element's place in the report: the element plus its rendered change text."""
    element: Element
    change: str


@dataclass
class Diff:
    old_count: int
    new_count: int
    added: int
    removed: int
    changed: int
    buckets: dict[str, list[Row]]   # "breaking" | "additive" | "behavioral" -> rows


def short(t: str) -> str:
    """Strip quotes and conjure/proto module prefixes for display."""
    return re.sub(r"\b[a-z][a-zA-Z0-9_]*_(?=[A-Z])", "", t.replace("'", ""))


# ---- change classification (structured in, severity + display string out) ----
def _fieldmap(e: Element) -> dict[str, Field]:
    return {f.name: f for f in e.fields}


def is_changed(a: Element, b: Element) -> bool:
    if a.kind != b.kind:
        return True
    if a.kind == "enum":
        return sorted(a.members) != sorted(b.members)
    if a.kind in ("bean", "union", "message"):
        return [(f.name, f.type, f.optional) for f in a.fields] != [(f.name, f.type, f.optional) for f in b.fields]
    if a.kind == "service":
        return (a.params, a.returns) != (b.params, b.returns)
    return False  # rpc carries no payload


def _endpoint_sig(e: Element) -> str:
    sig = "(" + ", ".join(e.params) + ")" + (f" -> {e.returns}" if e.returns else "")
    return short(sig)


def classify_change(old: Element, new: Element) -> tuple[str, str]:
    """Return (severity, change-cell text) for a changed element."""
    if old.kind == "enum":
        om, nm = set(old.members), set(new.members)
        added, removed = sorted(nm - om), sorted(om - nm)
        if removed:
            return "breaking", f"− {{{', '.join(removed)}}}" + (f"; + {{{', '.join(added)}}}" if added else "")
        return "behavioral", f"+ {{{', '.join(added)}}}"
    if old.kind in ("bean", "union", "message"):
        of, nf = _fieldmap(old), _fieldmap(new)
        deltas, sev = [], "additive"
        for name in sorted(set(nf) - set(of)):
            f = nf[name]
            deltas.append(f"+ {name}:{short(f.type)}" + ("" if f.optional else " (req)"))
            if not f.optional:
                sev = "breaking"
        for name in sorted(set(of) - set(nf)):
            deltas.append(f"− {name}:{short(of[name].type)}")
            sev = "breaking"
        for name in sorted(set(of) & set(nf)):
            o, n = of[name], nf[name]
            if o.type != n.type:
                deltas.append(f"~ {name}: {short(o.type)}→{short(n.type)}")
                sev = "breaking"
            elif o.optional != n.optional:
                to_required = o.optional and not n.optional
                deltas.append(f"~ {name}: now required" if to_required else f"~ {name}: now optional")
                if to_required:
                    sev = "breaking"
        return sev, "; ".join(deltas) or "(signature changed)"
    return "breaking", f"sig: {_endpoint_sig(old)} → {_endpoint_sig(new)}"  # service


def added_summary(e: Element) -> str:
    if e.kind in ("bean", "union", "message"):
        return "new: (" + ", ".join(f.name for f in e.fields) + ")"
    if e.kind == "enum":
        return "new enum: {" + ", ".join(sorted(e.members)) + "}"
    if e.kind == "service":
        return "new endpoint " + _endpoint_sig(e)
    return "new"


def diff(old: list[Element], new: list[Element]) -> Diff:
    """Match elements by key and sort each into a severity bucket. No rendering, no I/O."""
    old_by = {e.key: e for e in old}
    new_by = {e.key: e for e in new}
    added_keys, removed_keys, common = set(new_by) - set(old_by), set(old_by) - set(new_by), set(old_by) & set(new_by)
    buckets: dict[str, list[Row]] = {"breaking": [], "additive": [], "behavioral": []}
    for k in sorted(removed_keys):
        buckets["breaking"].append(Row(old_by[k], "**removed**"))
    changed = 0
    for k in sorted(common):
        if is_changed(old_by[k], new_by[k]):
            changed += 1
            severity, change = classify_change(old_by[k], new_by[k])
            buckets[severity].append(Row(new_by[k], change))
    for k in sorted(added_keys):
        buckets["additive"].append(Row(new_by[k], added_summary(new_by[k])))
    return Diff(len(old_by), len(new_by), len(added_keys), len(removed_keys), changed, buckets)


# ---- repo reference scan ----------------------------------------------------
def scan_refs(names: set[str], repo: Path) -> dict[str, list[str]] | None:
    roots = [r for r in (repo / "nominal", repo / "tests") if r.exists()]
    if not roots:
        print(f"warning: no nominal/ or tests/ under {repo}; skipping client ref scan", file=sys.stderr)
        return None
    pats = {n: re.compile(rf"\b{re.escape(n)}\b") for n in names}
    hits: dict[str, list[str]] = {n: [] for n in names}
    for root in roots:
        for py in root.rglob("*.py"):
            try:
                text = py.read_text(encoding="utf-8")
            except Exception:
                continue
            rel = py.relative_to(repo).as_posix()
            for n, pat in pats.items():
                if pat.search(text):
                    hits[n].append(rel)
    return hits


def _refs_cell(name: str, hits: dict[str, list[str]] | None) -> str:
    if hits is None:
        return "—"
    files = hits.get(name, [])
    if not files:
        return "0"
    shown = ", ".join(f"`{f}`" for f in files[:3])
    return f"**{len(files)}** ({shown}{', …' if len(files) > 3 else ''})"


# ---- rendering --------------------------------------------------------------
def _table(rows: list[Row], hits: dict[str, list[str]] | None, internal_substr: str) -> str:
    if not rows:
        return "_none_\n"
    rows = sorted(rows, key=lambda r: (internal_substr in r.element.key, r.element.display))
    out = ["| Element | Kind | Change | Client refs |", "|---|---|---|---|"]
    for r in rows:
        cell = r.change.replace("|", "\\|")  # escape pipes so they don't break the table
        out.append(f"| `{r.element.display}` | {r.element.kind} | {cell} | {_refs_cell(r.element.leaf, hits)} |")
    return "\n".join(out) + "\n"


def build_report(old: list[Element], new: list[Element], label: str, repo: str | None = None,
                 internal_substr: str = "Internal") -> str:
    d = diff(old, new)
    every_row = [r for rows in d.buckets.values() for r in rows]
    hits = scan_refs({r.element.leaf for r in every_row}, Path(repo)) if repo else None
    n_break_refd = sum(1 for r in d.buckets["breaking"] if hits and hits.get(r.element.leaf))

    lines: list[str] = []
    lines.append(f"# Migration report — {label}\n")
    lines.append(
        f"Surface: {d.old_count} → {d.new_count} elements · "
        f"**{d.added} added · {d.removed} removed · {d.changed} changed**"
        + (f" · client scan: `{repo}`" if repo else "") + "\n"
    )
    if hits is not None:
        lines.append(
            (f"⚠️ **{n_break_refd} breaking change(s) touch the client — action required.**"
             if n_break_refd else
             "✅ **No breaking change references the client — the bump is safe.**") + "\n"
        )
    for heading, name in (("⚠️ Breaking", "breaking"), ("➕ Additive", "additive"), ("ℹ️ Behavioral", "behavioral")):
        prefix = "" if name == "breaking" else "\n"
        lines.append(f"{prefix}## {heading} ({len(d.buckets[name])})\n")
        lines.append(_table(d.buckets[name], hits, internal_substr))
    if internal_substr:
        lines.append(f"\n_Rows containing `{internal_substr}` (internal services) are sorted last._")
    if hits is not None:
        lines.append(
            "\n_Client refs are textual matches of the element's leaf name in `nominal/` + `tests/`. "
            "Verify import context for common names (e.g. `File`, `Error`, `State`) — they over-match._"
        )
    return "".join(s + "\n" for s in lines)


def main() -> None:
    use_utf8_stdout()
    ap = argparse.ArgumentParser()
    ap.add_argument("old")
    ap.add_argument("new")
    ap.add_argument("--label", default="API surface diff")
    ap.add_argument("--repo", default=None)
    ap.add_argument("--internal-substr", default="Internal")
    args = ap.parse_args()
    old = from_jsonl(Path(args.old).read_text(encoding="utf-8"))
    new = from_jsonl(Path(args.new).read_text(encoding="utf-8"))
    sys.stdout.write(build_report(old, new, args.label, args.repo, args.internal_substr))


if __name__ == "__main__":
    main()
