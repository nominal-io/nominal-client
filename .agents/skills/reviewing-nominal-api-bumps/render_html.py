#!/usr/bin/env python3
"""Render diffed surfaces into one self-contained HTML migration report.

`render_page(reports)` takes one entry per transport (each with its `Diff` and ref-scan
`hits`) and returns a standalone HTML string — no external assets, so it can be opened
from disk or served with `serve_report.py`. Layout/styling mirror the markdown report:
a verdict banner and severity-coded tables per transport.
"""
from __future__ import annotations

import html
import re
from pathlib import Path

from diff_surface import Diff, Row, breaking_refd

SEV = {  # bucket -> (heading, accent, tint)
    "breaking": ("⚠️ Breaking", "#b42318", "#fef3f2"),
    "additive": ("➕ Additive", "#067647", "#ecfdf3"),
    "behavioral": ("ℹ️ Behavioral", "#1d4ed8", "#eff4ff"),
}


def _inline(s: str, color_markers: bool = False) -> str:
    s = html.escape(s)
    s = re.sub(r"`([^`]+)`", r"<code>\1</code>", s)
    s = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", s)
    if color_markers:
        s = re.sub(r"(^|; )\+ ", r'\1<span class="mk add">+</span> ', s)
        s = re.sub(r"(^|; )− ", r'\1<span class="mk rem">−</span> ', s)
        s = re.sub(r"(^|; )~ ", r'\1<span class="mk chg">~</span> ', s)
    return s


def _refs_cell(leaf: str, hits: dict[str, list[str]] | None) -> str:
    if hits is None:
        return '<span class="refs none">—</span>'
    files = hits.get(leaf, [])
    if not files:
        return '<span class="refs zero">0</span>'
    shown = ", ".join(f"<code>{html.escape(f)}</code>" for f in files[:3])
    more = ", …" if len(files) > 3 else ""
    return f'<span class="refs hit"><b>{len(files)}</b> ({shown}{more})</span>'


def _table(rows: list[Row], hits: dict[str, list[str]] | None, internal_substr: str) -> str:
    if not rows:
        return '<p class="empty">none</p>'
    rows = sorted(rows, key=lambda r: (internal_substr in r.element.key, r.element.display))
    body = []
    for r in rows:
        body.append(
            f"<tr><td class='el'><code>{html.escape(r.element.display)}</code></td>"
            f"<td><span class='kind'>{html.escape(r.element.kind)}</span></td>"
            f"<td class='chg'>{_inline(r.change, color_markers=True)}</td>"
            f"<td>{_refs_cell(r.element.leaf, hits)}</td></tr>"
        )
    return ("<div class='tablewrap'><table>"
            "<thead><tr><th>Element</th><th>Kind</th><th>Change</th><th>Client refs</th></tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table></div>")


def _chip(label: str, n: int, kind: str) -> str:
    return f'<span class="chip {kind}"><b>{n}</b> {label}</span>'


def _report_section(rep: dict, internal_substr: str) -> str:
    d: Diff = rep["diff"]
    hits = rep["hits"]
    scanned = hits is not None
    n_ref = breaking_refd(d, hits)
    if not scanned:
        banner_cls, mark, verdict = "neutral", "•", "Client not scanned."
    elif n_ref:
        banner_cls, mark = "action", "⚠"
        verdict = f"{n_ref} breaking change(s) touch the client — action required."
    else:
        banner_cls, mark, verdict = "safe", "✓", "No breaking change references the client — the bump is safe."
    chips = (_chip("added", d.added, "add") + _chip("removed", d.removed, "rem") + _chip("changed", d.changed, "chg"))
    sections = []
    for name, (heading, accent, tint) in SEV.items():
        rows = d.buckets[name]
        sections.append(
            f'<h3 class="sevhead" style="--sev:{accent};--sevtint:{tint}">{heading} '
            f'<span class="n">{len(rows)}</span></h3>{_table(rows, hits, internal_substr)}'
        )
    return f"""
    <section class="report">
      <div class="rhead">
        <div><span class="transport">{html.escape(rep['transport'])}</span>
          <h2>{html.escape(rep['pkg'])}</h2>
          <div class="vers">{html.escape(rep['old'])} → {html.escape(rep['new'])}</div></div>
        <div class="surface"><span>{d.old_count}</span> → <span class="s-new">{d.new_count}</span>
          <small>elements</small></div>
      </div>
      <div class="banner {banner_cls}">{mark}&nbsp;&nbsp;{html.escape(verdict)}</div>
      <div class="chips">{chips}</div>
      {''.join(sections)}
    </section>"""


CSS = (Path(__file__).parent / "report.css").read_text(encoding="utf-8")


def render_page(reports: list[dict], internal_substr: str = "Internal") -> str:
    pairs = {(r["old"], r["new"]) for r in reports}
    vlabel = (f'<span class="v">{html.escape(reports[0]["old"])} → {html.escape(reports[0]["new"])}</span>'
              if len(pairs) == 1 else "version bump")
    title = (f'Nominal API bump {reports[0]["old"]} → {reports[0]["new"]}'
             if len(pairs) == 1 else "Nominal API bump")
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
  <header>
    <p class="eyebrow">API surface migration report</p>
    <h1>Nominal bindings bump<br>{vlabel}</h1>
    <p class="lede">Structural diff of the generated bindings, with a scan of <code>nominal/</code> +
      <code>tests/</code> for references — a breaking change with zero call sites is a safe bump;
      an unreferenced additive is an unwired capability.</p>
    <div class="legend">
      <span><b style="color:#b42318">⚠ Breaking</b> removed / type change / new required</span>
      <span><b style="color:#067647">+ Additive</b> new element or optional field</span>
      <span><b style="color:#1d4ed8">ℹ Behavioral</b> enum members added</span>
    </div>
    <hr class="rule">
  </header>
  {''.join(_report_section(r, internal_substr) for r in reports)}
  <p class="foot">Rows whose key contains <code>{html.escape(internal_substr)}</code> (internal services)
    are sorted last. Client refs are textual matches of the element's leaf name in
    <code>nominal/</code> + <code>tests/</code> — verify import context for common names
    (e.g. <code>File</code>, <code>Error</code>, <code>State</code>); they over-match.</p>
</div>
</body>
</html>"""
