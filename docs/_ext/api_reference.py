"""SDK presentation hooks: readable examples, public signatures and source links."""

import inspect
import re

from sphinx.application import Sphinx
from sphinx.util.inspect import stringify_signature


def _drop_private_params(app, what, name, obj, options, signature, return_annotation):
    """Hide underscore parameters (dataclass fields like ``_clients``) from class signatures.

    Most nominal classes are dataclasses built by the client, not by users, so their
    private fields would otherwise lead the constructor signature.
    """
    if what != "class" or not signature or "_" not in signature:
        return None
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError):
        return None
    params = [p for p in sig.parameters.values() if not p.name.startswith("_")]
    if len(params) == len(sig.parameters):
        return None
    return stringify_signature(sig.replace(parameters=params)), return_annotation


def _hide_edit_link_on_stubs(app, pagename, templatename, context, doctree):
    """Autosummary stubs under generated/ are gitignored, so "Edit this page" would 404."""
    if "/generated/" in pagename:
        context["page_source_suffix"] = ""


# Only code fences are adapted; Napoleon and Sphinx parse all other markup.
# Matching indentation keeps fences inside argument descriptions and notes.
_CODE_FENCE = re.compile(
    r"^(?P<indent> *)```(?P<language>[\w.+-]*)[ \t]*\n"
    r"(?P<code>(?:(?P=indent)[^\n]*\n|[ \t]*\n)*?)^(?P=indent)```[ \t]*$",
    re.MULTILINE,
)


def _expand_code_fence(match: re.Match[str]) -> str:
    body = "\n".join("    " + line if line.strip() else "" for line in match["code"].splitlines())
    return f"{match['indent']}.. code-block:: {match['language'] or 'text'}\n\n{body}\n"


def _process_code_fences(app, what, name, obj, options, lines):
    lines[:] = _CODE_FENCE.sub(_expand_code_fence, "\n".join(lines)).split("\n")


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("autodoc-process-docstring", _process_code_fences, priority=400)  # before Napoleon
    app.connect("autodoc-process-signature", _drop_private_params)
    app.connect("html-page-context", _hide_edit_link_on_stubs, priority=600)
    return {"parallel_read_safe": True}
