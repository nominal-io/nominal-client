"""API presentation hooks and incremental native autosummary generation."""

import inspect
from pathlib import Path

from sphinx.application import Sphinx
from sphinx.ext.autosummary.generate import generate_autosummary_docs
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


def _generate_api_stubs(app: Sphinx) -> None:
    """Keep current native stubs unchanged and remove only unreachable generated pages."""
    # Never seed generation with old stubs: they can reference removed Python objects.
    pending = [
        str(app.env.doc2path(name))
        for name in sorted(app.env.found_docs)
        if name.startswith("reference/") and "/generated/" not in name and app.env.doc2path(name).is_file()
    ]
    generated: set[Path] = set()
    while pending:
        paths = generate_autosummary_docs(pending, app=app)
        discovered = set(paths) - generated
        generated.update(discovered)
        # Native recursion visits newly written files only. Visit existing ones
        # too, so an API edit updates members even if the parent stub is unchanged.
        pending = sorted(str(path) for path in discovered)

    for stale in (Path(app.srcdir) / "reference").rglob("generated/*.rst"):
        if stale not in generated:
            stale.unlink()
            docname = stale.relative_to(app.srcdir).with_suffix("").as_posix()
            Path(app.builder.get_outfilename(docname)).unlink(missing_ok=True)


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("builder-inited", _generate_api_stubs, priority=600)
    app.connect("autodoc-process-signature", _drop_private_params)
    app.connect("html-page-context", _hide_edit_link_on_stubs, priority=600)
    return {"parallel_read_safe": True}
