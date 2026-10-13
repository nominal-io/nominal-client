"""Format public API signatures, class member order, and navigation."""

from dataclasses import fields, is_dataclass
from inspect import signature
from pkgutil import resolve_name

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


def _api_layout(app: Sphinx, domain: str, objtype: str, content: addnodes.desc_content) -> None:
    """Use dataclass fields for visibility and ordering; keep callable targets in navigation."""
    if domain != "py":
        return
    node = content.parent
    if objtype in {"attribute", "property", "data", "type"}:
        node["no-contents-entry"] = True
    if objtype not in {"class", "exception"}:
        return
    sig = node[0]
    # Handwritten directives need not describe an importable Python object.
    try:
        obj = resolve_name(f"{sig['module']}:{sig['fullname']}") if sig["module"] else None
    except (ImportError, AttributeError):
        obj = None
    if obj is None:
        return

    class_fields = fields(obj) if is_dataclass(obj) else ()
    field_order = {field.name: index for index, field in enumerate(class_fields)}
    hidden = {field.name for field in class_fields if not field.repr}
    hidden_targets: set[str] = set()
    for member in list(content):
        if isinstance(member, addnodes.desc) and member.get("objtype") == "attribute":
            member_sig = member[0]
            if member_sig["fullname"].rsplit(".", 1)[-1] in hidden:
                hidden_targets.update(member_sig["ids"])
                app.env.domains.python_domain.objects.pop(f"{member_sig['module']}.{member_sig['fullname']}", None)
                content.remove(member)
    # Remove index entries before Sphinx collects them, as well as the body and cross-reference targets.
    for index_node in content.findall(addnodes.index):
        index_node["entries"] = [entry for entry in index_node["entries"] if entry[2] not in hidden_targets]

    members = [child for child in content if isinstance(child, addnodes.desc)]
    members.sort(
        key=lambda child: (
            child.get("objtype") not in {"attribute", "property"},
            field_order.get(child[0]["fullname"].rsplit(".", 1)[-1], len(field_order)),
        )
    )
    ordered = iter(members)
    # Reorder only member slots; preserve introductory prose and authored sections.
    for index, child in enumerate(content):
        if isinstance(child, addnodes.desc):
            content[index] = next(ordered)


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("autodoc-process-signature", _dataclass_signature)
    app.connect("object-description-transform", _api_layout)
    return {"parallel_read_safe": True}
