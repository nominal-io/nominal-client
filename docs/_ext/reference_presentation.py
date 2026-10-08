"""Display dataclass resource signatures using their declared public fields."""

from dataclasses import fields, is_dataclass
from inspect import signature

from docutils import nodes
from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.util.inspect import stringify_signature


def _dataclass_signature(
    app: Sphinx, what: str, name: str, obj: object, options: object, args: str | None, retann: str | None
) -> tuple[str, str | None] | None:
    if what != "class" or not isinstance(obj, type) or not is_dataclass(obj):
        return None
    # Dataclasses generate docstring signatures with quoted annotations; format the
    # declared signature instead, keeping repr-hidden implementation fields out.
    hidden = {field.name for field in fields(obj) if not field.repr}
    sig = signature(obj)
    sig = sig.replace(parameters=[param for param in sig.parameters.values() if param.name not in hidden])
    return stringify_signature(sig, show_return_annotation=False, unqualified_typehints=True), retann


def _api_contents(app: Sphinx, doctree: nodes.document) -> None:
    """Keep reference targets while omitting non-callable objects from navigation."""
    for node in doctree.findall(addnodes.desc):
        if node.get("domain") == "py" and node.get("objtype") in {"attribute", "property", "data", "type"}:
            node["no-contents-entry"] = True


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("autodoc-process-signature", _dataclass_signature)
    app.connect("doctree-read", _api_contents, priority=400)
    return {"parallel_read_safe": True}
