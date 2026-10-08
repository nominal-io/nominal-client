"""Format public API signatures, class member order, and navigation."""

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


def _api_layout(app: Sphinx, doctree: nodes.document) -> None:
    """Show attributes first and keep callable reference targets in navigation."""
    for node in doctree.findall(addnodes.desc):
        if node.get("domain") == "py" and node.get("objtype") in {"attribute", "property", "data", "type"}:
            node["no-contents-entry"] = True
        if node.get("domain") == "py" and node.get("objtype") in {"class", "exception"}:
            content = node[-1]
            members = [child for child in content if isinstance(child, addnodes.desc)]
            members.sort(key=lambda child: child.get("objtype") not in {"attribute", "property"})
            ordered = iter(members)
            # Reorder only member slots; preserve introductory prose and authored sections.
            for index, child in enumerate(content):
                if isinstance(child, addnodes.desc):
                    content[index] = next(ordered)


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("autodoc-process-signature", _dataclass_signature)
    app.connect("doctree-read", _api_layout, priority=400)
    return {"parallel_read_safe": True}
