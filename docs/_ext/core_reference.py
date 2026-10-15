"""Keep split Core references aligned with public exports and existing links."""

import json
from pkgutil import resolve_name
from typing import Any

from docutils import nodes
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.errors import ExtensionError

from nominal import core


def _public_members(app: Sphinx, what: str, name: str, obj: object, skip: bool, options: object) -> bool | None:
    """Use the package's public exports instead of exposing implementation-module helpers."""
    if what == "module" and app.env.docname.startswith("sdk/core/"):
        if name not in core.__all__ or getattr(core, name) is not obj:
            return True
    return None


def _public_targets(app: Sphinx, env: BuildEnvironment) -> None:
    """Resolve public import names and their members to the single owning module page."""
    objects = env.domains.python_domain.objects
    for name in core.__all__:
        public = f"nominal.core.{name}"
        if public in objects:
            continue
        obj = getattr(core, name)
        original = f"{getattr(obj, '__module__', '')}.{getattr(obj, '__qualname__', name)}"
        # Generic aliases describe their origin class in __module__/__qualname__;
        # autodoc registers them at the assignment's module instead.
        if original not in objects:
            for target, entry in objects.items():
                if entry.objtype == "data" and target.rsplit(".", 1)[-1] == name and resolve_name(target) is obj:
                    original = target
                    break
        for target, entry in list(objects.items()):
            if target == original or target.startswith(original + "."):
                objects[public + target[len(original) :]] = entry._replace(aliased=True)
        if public not in objects:
            raise ExtensionError(f"Public Core export {public} has no reference page")


def _legacy_bookmarks(
    app: Sphinx, pagename: str, templatename: str, context: dict[str, Any], doctree: nodes.document | None
) -> None:
    """Forward old aggregate-page fragments using the destinations Sphinx already knows."""
    if pagename != "sdk/core":
        return
    objects = app.env.domains.python_domain.objects
    targets: dict[str, list[str]] = {}
    for name in core.__all__:
        public = f"nominal.core.{name}"
        entry = objects[public]
        targets[public] = [app.builder.get_relative_uri(pagename, entry.docname), entry.node_id]
    app.add_js_file(
        None,
        body="""
(() => {
    const targets = %s;
    function forwardBookmark() {
        const fragment = decodeURIComponent(window.location.hash.slice(1));
        const name = fragment.split('.').slice(0, 3).join('.');
        const target = targets[name];
        if (target) {
            window.location.replace(target[0] + window.location.search + '#' + target[1] + fragment.slice(name.length));
        }
    }
    forwardBookmark();
    window.addEventListener('hashchange', forwardBookmark);
})();
"""
        % json.dumps(targets),
    )


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("autodoc-skip-member", _public_members)
    app.connect("env-updated", _public_targets)
    app.connect("html-page-context", _legacy_bookmarks)
    return {"parallel_read_safe": True}
