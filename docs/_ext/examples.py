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

# pagename -> repo-relative script path, filled by generate() for the edit link
_SOURCES: dict[str, str] = {}


def _script_title(py: Path) -> str:
    doc = ast.get_docstring(ast.parse(py.read_text(encoding="utf-8"))) or ""
    first = doc.strip().splitlines()[0].strip() if doc.strip() else py.stem
    if first.lower().startswith("example:"):
        first = first[len("example:") :].strip()
    return first.rstrip(".") or py.stem


def _script_summary(py: Path) -> str:
    """The docstring after its title line, shown above the script."""
    doc = ast.get_docstring(ast.parse(py.read_text(encoding="utf-8"))) or ""
    return "\n".join(doc.strip().splitlines()[1:]).strip()


def _page(py: Path, repo: Path, source: Path, title: str) -> str:
    rel = py.relative_to(repo).as_posix()
    include = Path(os.path.relpath(py, source)).as_posix()
    summary = _script_summary(py)
    return (
        f"# {title}\n\n"
        + (f"{summary}\n\n" if summary else "")
        + f"```{{literalinclude}} /{include}\n:caption: {py.name}\n:language: python\n```\n\n"
        f"Source: [`{rel}`](https://github.com/nominal-io/nominal-client/blob/main/{rel})\n"
    )


def generate(app: Sphinx) -> None:
    repo = Path(app.confdir).parent
    out = Path(app.srcdir) / "examples"
    files: dict[Path, str] = {}

    entries: list[tuple[str, str]] = []
    for py in sorted((repo / "examples").rglob("*.py")):
        # pages keep the script's path under examples/, so same-named scripts in
        # different subfolders don't collide
        name = py.relative_to(repo / "examples").with_suffix("").as_posix()
        title = _script_title(py)
        files[out / f"{name}.md"] = _page(py, repo, Path(app.srcdir), title)
        _SOURCES[f"examples/{name}"] = py.relative_to(repo).as_posix()
        entries.append((title, name))

    links = "".join(f"- [{title}]({name}.md)\n" for title, name in entries)
    toc = "".join(f"{name}\n" for _, name in entries)
    introduction = (
        "Full, runnable scripts using the Nominal Python SDK. The source is in "
        "[`examples/`](https://github.com/nominal-io/nominal-client/tree/main/examples)."
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


def _edit_link(app: Sphinx, pagename: str, templatename: str, context: dict[str, Any], doctree: Any) -> None:
    """Point the sidebar's "Edit this page" at the script; the generated .md isn't in the repo."""
    if not pagename.startswith("examples/"):
        return
    script = _SOURCES.get(pagename)
    if script is None:  # the index has no single source
        context["page_source_suffix"] = ""
        return
    ctx = app.config.html_context
    url = f"https://github.com/{ctx['source_user']}/{ctx['source_repo']}/blob/{ctx['source_version']}/{script}"
    context["edit_source_link"] = lambda filename: url


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("builder-inited", generate)
    # after the theme's own hook (priority 500), which installs edit_source_link
    app.connect("html-page-context", _edit_link, priority=600)
    return {"parallel_read_safe": True}
