"""Sphinx config for the Nominal Python SDK docs: guides (guides/), examples, and the API reference (sdk/)."""

import sys
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE / "_ext"))

project = "Nominal Python SDK"
copyright = "Nominal, Inc."

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_click",
    "sphinx_design",
    "sphinx_copybutton",
    "markdown_docstrings",
    "examples",
    "sections",
    "video",
]

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    # contributor docs, at the top level and in any folder
    *[
        f"{prefix}{name}"
        for prefix in ("", "**/")
        for name in ("README.md", "AGENTS.md", "CLAUDE.md", "CONTRIBUTING.md")
    ],
    "DOCS_MIGRATION.md",
    # partials pulled into guide pages with {include}, not pages of their own
    "guides/_snippets",
]

# Single backticks in docstrings (`Dataset`) link to the named object when it
# resolves and render as code otherwise.
default_role = "py:obj"

# -- MyST ---------------------------------------------------------------------
myst_enable_extensions = ["colon_fence", "deflist", "attrs_inline", "attrs_block", "fieldlist"]
myst_heading_anchors = 6

# -- autodoc -------------------------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": True,  # show_if_no_docstring
    # stop at these bases so enum/exception/builtin internals stay out
    "inherited-members": "object,BaseException,Enum,str,int,float",
    "member-order": "bysource",
    "show-inheritance": True,
}
autoclass_content = "both"  # merge_init_into_class
autodoc_typehints = "signature"
autodoc_preserve_defaults = True
python_maximum_signature_line_length = 72  # separate_signature + line_length
python_use_unqualified_type_names = True  # show_root_full_path: false
toc_object_entries_show_parents = "hide"

# Class pages list members in summary tables; each member gets its own page
# (the scikit-rf layout). Stubs are written to <page dir>/generated/.
autosummary_generate = True
autosummary_generate_overwrite = True
autosummary_context = {
    # private methods to document alongside the public ones (supported extension points)
    "extra_methods": {},
}

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_rtype = False
napoleon_use_ivar = True  # Attributes: sections as a field list, not duplicate targets

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# -- HTML: Shibuya, styled like the Fern docs site (docs.nominal.io) ---------------
html_theme = "shibuya"
html_title = "Nominal Python SDK"
html_static_path = ["_static"]
html_css_files = [
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap",
    "custom.css",
]
html_js_files = ["external-links.js"]
html_favicon = "_static/favicon.png"
html_copy_source = False

html_context = {
    "source_type": "github",
    "source_user": "nominal-io",
    "source_repo": "nominal-client",
    "source_version": "main",
    "source_docs_path": "/docs/",
}

# Right sidebar: on-page contents and edit link, no GitHub repo-stats box.
html_sidebars = {"**": ["sidebars/localtoc.html", "sidebars/edit-this-page.html"]}

html_theme_options = {
    "accent_color": "gray",
    # dark unless the visitor picks light with the theme switch (remembered in localStorage)
    "color_mode": "dark",
    "light_logo": "_static/logo/nominal-logo-light.svg",
    "dark_logo": "_static/logo/nominal-logo-dark.svg",
    "github_url": "https://github.com/nominal-io/nominal-client",
    # section tabs; the sidebar follows them (_ext/sections.py)
    "nav_links": [
        {"title": "Guides", "url": "index"},
        {"title": "Examples", "url": "examples/index"},
        {"title": "SDK", "url": "sdk/index"},
        {"title": "GitHub", "url": "https://github.com/nominal-io/nominal-client"},
    ],
    # right of the search box, "Get demo" and "Open app" replace the social icons
    # (_templates/partials/nav-socials.html)
    "toctree_titles_only": True,
    # left nav lists pages only; generated class/member pages are reached from their tables
    "toctree_maxdepth": 1,
}
add_module_names = False


def _alias_reexports(app, env):
    """Resolve re-exported names to the documented original.

    Points nominal.core.Dataset at nominal.core.dataset.Dataset, for example. Without
    this, a type annotation using the re-exported name falls back to a fuzzy match
    and can pick an unrelated object of the same name.
    """
    py = env.get_domain("py")
    for modname, mod in list(sys.modules.items()):
        if not modname.startswith("nominal") or mod is None:
            continue
        for attr, obj in list(vars(mod).items()):
            origin = getattr(obj, "__module__", None)
            qualname = getattr(obj, "__qualname__", None)
            if attr.startswith("_") or not origin or not qualname or origin == modname:
                continue
            target, alias = f"{origin}.{qualname}", f"{modname}.{attr}"
            if target in py.objects and alias not in py.objects:
                py.objects[alias] = py.objects[target]._replace(aliased=True)


def _drop_private_params(app, what, name, obj, options, signature, return_annotation):
    """Hide underscore parameters (dataclass fields like ``_clients``) from class signatures.

    Most nominal classes are dataclasses built by the client, not by users, so their
    private fields would otherwise lead the constructor signature.
    """
    import inspect

    from sphinx.util.inspect import stringify_signature

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


def _drop_object_base(app, name, obj, options, bases):
    """Drop `object` from the "Bases:" line; it says nothing."""
    bases[:] = [b for b in bases if b is not object]


def _hide_edit_link_on_stubs(app, pagename, templatename, context, doctree):
    """Autosummary stubs under generated/ are gitignored, so "Edit this page" would 404."""
    if "/generated/" in pagename:
        context["page_source_suffix"] = ""


def setup(app):
    app.connect("autodoc-process-signature", _drop_private_params)
    app.connect("autodoc-process-bases", _drop_object_base)
    app.connect("env-updated", _alias_reexports)
    app.connect("html-page-context", _hide_edit_link_on_stubs, priority=600)
