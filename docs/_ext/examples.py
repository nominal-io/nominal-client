"""Generate the Examples section (``docs/src/examples/``, gitignored) from the example scripts.

Each ``examples/**/*.py`` becomes a page showing the script, titled from the first
line of its module docstring, and the index links them all. Nothing here is
hand-maintained: add a script and it appears. Files are only rewritten when their
content changes, so live preview doesn't loop.
"""

import ast
import os
from pathlib import Path
from typing import Any

from sphinx.application import Sphinx


def _script_docstring(py: Path) -> tuple[str, str]:
    """Extract a title and the remaining summary with one parse of the script."""
    doc = ast.get_docstring(ast.parse(py.read_text(encoding="utf-8"))) or ""
    lines = doc.strip().splitlines()
    title = lines[0].strip() if lines else py.stem
    if title.lower().startswith("example:"):
        title = title[len("example:") :].strip()
    return title.rstrip(".") or py.stem, "\n".join(lines[1:]).strip()


def _source_url(app: Sphinx, relative: str, kind: str = "blob") -> str:
    """Use the configured repository and revision for every generated source link."""
    ctx = app.config.html_context
    return f"https://github.com/{ctx['source_user']}/{ctx['source_repo']}/{kind}/{ctx['source_version']}/{relative}"


def _page(app: Sphinx, py: Path, repo: Path, title: str, summary: str) -> str:
    rel = py.relative_to(repo).as_posix()
    include = Path(os.path.relpath(py, app.srcdir)).as_posix()
    return (
        f"# {title}\n\n"
        + (f"{summary}\n\n" if summary else "")
        + f"```{{literalinclude}} /{include}\n:caption: {py.name}\n:language: python\n```\n\n"
        f"Source: [`{rel}`]({_source_url(app, rel)})\n"
    )


def generate(app: Sphinx) -> None:
    repo = Path(app.confdir).parent
    out = Path(app.srcdir) / "examples"
    files: dict[Path, str] = {}

    entries: list[tuple[str, str]] = []
    for py in sorted((repo / "examples").rglob("*.py")):
        # Preserve the .py suffix so index.py never overwrites the landing page
        # or collides with dirhtml's special handling of index documents.
        name = py.relative_to(repo / "examples").as_posix()
        title, summary = _script_docstring(py)
        files[out / f"{name}.md"] = _page(app, py, repo, title, summary)
        entries.append((title, name))

    links = "".join(f"- [{title}]({name}.md)\n" for title, name in entries)
    toc = "".join(f"{name}\n" for _, name in entries)
    introduction = (
        "Full, runnable scripts using the Nominal Python SDK. The source is in "
        f"[`examples/`]({_source_url(app, 'examples', 'tree')})."
        if entries
        else "No example scripts have been added yet. Add a script under `examples/` to generate its page."
    )
    files[out / "index.md"] = (
        f"# Examples\n\n{{.lead}}\n{introduction}\n\n{links}\n```{{toctree}}\n:hidden:\n\n{toc}```\n"
    )

    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            path.write_text(text, encoding="utf-8")
    for stale in out.rglob("*.md"):
        if stale not in files:
            stale.unlink()
            docname = stale.relative_to(app.srcdir).with_suffix("").as_posix()
            Path(app.builder.get_outfilename(docname)).unlink(missing_ok=True)


def _edit_link(app: Sphinx, pagename: str, templatename: str, context: dict[str, Any], doctree: Any) -> None:
    """Point the sidebar's "Edit this page" at the script; the generated .md isn't in the repo."""
    if not pagename.startswith("examples/"):
        return
    if pagename == "examples/index":
        context["page_source_suffix"] = ""
        return
    url = _source_url(app, pagename)
    context["edit_source_link"] = lambda filename: url


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("builder-inited", generate)
    # after the theme's own hook (priority 500), which installs edit_source_link
    app.connect("html-page-context", _edit_link, priority=600)
    return {"parallel_read_safe": True}
