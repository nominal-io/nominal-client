#!/usr/bin/env python3
"""Diff two API-surface files (from extract_surface.py) and render a migration report.

Keys every element by `[kind] dotted.name`, then classifies each delta:

  REMOVED element ............................. BREAKING
  CHANGED element:
    removed field / type change / optional->required ... BREAKING
    new required field ................................. BREAKING
    new optional field / required->optional ............ additive
    enum: members removed .............................. BREAKING
    enum: members added (existing enum) ................ behavioral
  ADDED element ............................... additive

With `--repo <root>` it scans <root>/nominal and <root>/tests for references to each
element (by its leaf name) so the report shows where in the client a change lands —
0 refs on a breaking change means the bump is safe; 0 refs on an additive means it's
not wired up yet.

Usage:
  diff_surface.py <old.txt> <new.txt> --label "nominal-api 0.1282.0 -> 0.1286.0" \\
      [--repo <client-root>] [--internal-substr Internal]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

KEY_RE = re.compile(r"(\[\w+\] [\w.]+)")


def short(t: str) -> str:
    """Strip quotes and conjure/proto module prefixes for display."""
    t = t.replace("'", "")
    return re.sub(r"\b[a-z][a-zA-Z0-9_]*_(?=[A-Z])", "", t)


def load(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        m = KEY_RE.match(line)
        out[m.group(1) if m else line] = line
    return out


def split_top(inner: str) -> list[str]:
    parts, buf, depth = [], "", 0
    for ch in inner:
        if ch in "[{(":
            depth += 1
        elif ch in "]})":
            depth -= 1
        if ch == "," and depth <= 0:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
    if buf.strip():
        parts.append(buf)
    return parts


def fields(sig: str) -> dict[str, tuple[str, bool]]:
    """name -> (type, optional) from a bean/union/message signature."""
    candidates = [sig.find(c) for c in "({" if sig.find(c) >= 0]
    open_idx = min(candidates) if candidates else -1
    if open_idx < 0:
        return {}
    inner = sig[open_idx + 1 : sig.rfind(")") if sig[open_idx] == "(" else sig.rfind("}")]
    out: dict[str, tuple[str, bool]] = {}
    for p in split_top(inner):
        p = p.strip()
        if ":" not in p:
            continue
        name, rest = p.split(":", 1)
        optional = "= ..." in rest
        typ = rest.replace("= ...", "").strip()
        out[name.strip()] = (typ, optional)
    return out


def enum_members(sig: str) -> set[str]:
    inner = sig[sig.find("{") + 1 : sig.rfind("}")]
    return {m.strip() for m in inner.split(",") if m.strip()}


def kind_of(key: str) -> str:
    return key[1 : key.index("]")]


def leaf(key: str) -> str:
    return key.split("]", 1)[1].strip().split(".")[-1]


def display(key: str) -> str:
    return key.split("]", 1)[1].strip()


# ---- classify a single CHANGED element -> (severity, change_str) ------------
def classify_change(key: str, old: str, new: str) -> tuple[str, str]:
    kind = kind_of(key)
    if kind == "enum":
        om, nm = enum_members(old), enum_members(new)
        added, removed = sorted(nm - om), sorted(om - nm)
        if removed:
            return "breaking", f"− {{{', '.join(removed)}}}" + (f"; + {{{', '.join(added)}}}" if added else "")
        return "behavioral", f"+ {{{', '.join(added)}}}"
    if kind in ("bean", "union", "message"):
        of, nf = fields(old), fields(new)
        deltas, sev = [], "additive"
        for f in sorted(set(nf) - set(of)):
            typ, opt = nf[f]
            deltas.append(f"+ {f}:{short(typ)}" + ("" if opt else " (req)"))
            if not opt:
                sev = "breaking"
        for f in sorted(set(of) - set(nf)):
            deltas.append(f"− {f}:{short(of[f][0])}")
            sev = "breaking"
        for f in sorted(set(of) & set(nf)):
            ot, oo = of[f]
            nt, no = nf[f]
            if ot != nt:
                deltas.append(f"~ {f}: {short(ot)}→{short(nt)}")
                sev = "breaking"
            elif oo != no:
                if oo and not no:
                    deltas.append(f"~ {f}: now required")
                    sev = "breaking"
                else:
                    deltas.append(f"~ {f}: now optional")
        return sev, "; ".join(deltas) or "(signature changed)"
    # service endpoint signature change
    return "breaking", f"sig: {display_sig(old)} → {display_sig(new)}"


def display_sig(line: str) -> str:
    i = line.find("(")
    return short(line[i:]) if i >= 0 else line


def added_summary(key: str, line: str) -> str:
    kind = kind_of(key)
    if kind in ("bean", "union", "message"):
        return "new: (" + ", ".join(fields(line)) + ")"
    if kind == "enum":
        return "new enum: {" + ", ".join(sorted(enum_members(line))) + "}"
    if kind == "service":
        return "new endpoint " + display_sig(line)
    return "new"


# ---- repo reference scan ----------------------------------------------------
def scan_refs(names: set[str], repo: Path) -> dict[str, list[str]]:
    roots = [repo / "nominal", repo / "tests"]
    roots = [r for r in roots if r.exists()] or [repo]
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


def refs_cell(name: str, hits: dict[str, list[str]] | None) -> str:
    if hits is None:
        return "—"
    files = hits.get(name, [])
    if not files:
        return "0"
    shown = ", ".join(f"`{f}`" for f in files[:3])
    return f"**{len(files)}** ({shown}{', …' if len(files) > 3 else ''})"


def table(rows: list[tuple[str, str, str, str]]) -> str:
    if not rows:
        return "_none_\n"
    out = ["| Element | Kind | Change | Client refs |", "|---|---|---|---|"]
    for el, kind, change, refs in rows:
        change = change.replace("|", "\\|")
        out.append(f"| `{el}` | {kind} | {change} | {refs} |")
    return "\n".join(out) + "\n"


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # report is UTF-8 (arrows, ✅/⚠️) on any platform
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("old")
    ap.add_argument("new")
    ap.add_argument("--label", default="API surface diff")
    ap.add_argument("--repo", default=None)
    ap.add_argument("--internal-substr", default="Internal")
    args = ap.parse_args()

    old, new = load(args.old), load(args.new)
    ok, nk = set(old), set(new)
    added_keys = sorted(nk - ok)
    removed_keys = sorted(ok - nk)
    changed_keys = sorted(k for k in ok & nk if old[k] != new[k])

    breaking: list[tuple] = []   # (el, kind, change, leafname, internal)
    additive: list[tuple] = []
    behavioral: list[tuple] = []

    for k in removed_keys:
        breaking.append((display(k), kind_of(k), "**removed**", leaf(k), args.internal_substr in k))
    for k in changed_keys:
        sev, change = classify_change(k, old[k], new[k])
        row = (display(k), kind_of(k), change, leaf(k), args.internal_substr in k)
        {"breaking": breaking, "additive": additive, "behavioral": behavioral}[sev].append(row)
    for k in added_keys:
        additive.append((display(k), kind_of(k), added_summary(k, new[k]), leaf(k), args.internal_substr in k))

    # reference scan
    hits = None
    if args.repo:
        names = {r[3] for r in breaking + additive + behavioral}
        hits = scan_refs(names, Path(args.repo))

    def rows(recs, sort_internal_last=True):
        recs = sorted(recs, key=lambda r: (r[4] if sort_internal_last else False, r[0]))
        return [(el, kind, change, refs_cell(name, hits)) for el, kind, change, name, _ in recs]

    n_break_refd = sum(1 for r in breaking if hits and hits.get(r[3])) if hits else 0
    print(f"# Migration report — {args.label}\n")
    print(
        f"Surface: {len(ok)} → {len(nk)} elements · "
        f"**{len(added_keys)} added · {len(removed_keys)} removed · {len(changed_keys)} changed**"
        + (f" · client scan: `{args.repo}`" if args.repo else "")
        + "\n"
    )
    if hits is not None:
        verdict = (
            f"⚠️ **{n_break_refd} breaking change(s) touch the client — action required.**"
            if n_break_refd
            else "✅ **No breaking change references the client — the bump is safe.**"
        )
        print(verdict + "\n")

    print(f"## ⚠️ Breaking ({len(breaking)})\n")
    print(table(rows(breaking)))
    print(f"\n## ➕ Additive ({len(additive)})\n")
    print(table(rows(additive)))
    print(f"\n## ℹ️ Behavioral ({len(behavioral)})\n")
    print(table(rows(behavioral)))
    if args.internal_substr:
        print(f"\n_Rows containing `{args.internal_substr}` (internal services) are sorted last._")
    if hits is not None:
        print(
            "\n_Client refs are textual matches of the element's leaf name in `nominal/` + `tests/`. "
            "Verify import context for common names (e.g. `File`, `Error`, `State`) — they over-match._"
        )


if __name__ == "__main__":
    main()
