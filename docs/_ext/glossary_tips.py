"""Hover boxes for glossary terms.

Each ``{term}`` link shows the first paragraph of its glossary definition in a Tippy box.
The build writes the definitions to ``_static/glossary-terms.js``; ``glossary-tips.js``
attaches the boxes.
"""

import json
from pathlib import Path
from typing import Any

from docutils import nodes
from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment

TIPPY_JS = (
    "https://cdnjs.cloudflare.com/ajax/libs/popper.js/2.11.8/umd/popper.min.js",
    "https://cdnjs.cloudflare.com/ajax/libs/tippy.js/6.3.7/tippy-bundle.umd.min.js",
)


def _definitions(env: BuildEnvironment) -> dict[str, dict[str, str]]:
    """Return the definitions by docname, keyed by term ID."""
    definitions: dict[str, dict[str, str]] = env.__dict__.setdefault("glossary_tips", {})
    return definitions


def collect_definitions(app: Sphinx, doctree: nodes.document) -> None:
    terms: dict[str, str] = {}
    for glossary in doctree.findall(addnodes.glossary):
        for item in glossary.findall(nodes.definition_list_item):
            first = next(item.findall(nodes.definition)).next_node(nodes.paragraph)
            text = " ".join(first.astext().split())
            for term in item.findall(nodes.term):
                terms.update(dict.fromkeys(term["ids"], text))
    if terms:
        _definitions(app.env)[app.env.docname] = terms


def purge_definitions(app: Sphinx, env: BuildEnvironment, docname: str) -> None:
    _definitions(env).pop(docname, None)


def merge_definitions(app: Sphinx, env: BuildEnvironment, docnames: set[str], other: BuildEnvironment) -> None:
    _definitions(env).update(_definitions(other))


def write_definitions(app: Sphinx, exception: Exception | None) -> None:
    if exception or app.builder.format != "html":
        return
    terms = {term_id: text for terms in _definitions(app.env).values() for term_id, text in terms.items()}
    path = Path(app.outdir, "_static", "glossary-terms.js")
    path.write_text(f"window.glossaryTerms = {json.dumps(terms, ensure_ascii=False)};\n", encoding="utf-8")


def setup(app: Sphinx) -> dict[str, Any]:
    for url in TIPPY_JS:
        app.add_js_file(url)
    app.add_js_file("glossary-terms.js")
    app.add_js_file("glossary-tips.js")
    app.connect("doctree-read", collect_definitions)
    app.connect("env-purge-doc", purge_definitions)
    app.connect("env-merge-info", merge_definitions)
    app.connect("build-finished", write_definitions)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
