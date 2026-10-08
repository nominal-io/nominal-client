"""Sphinx config for the Nominal Python SDK docs: guides (src/), examples, and the API reference (src/reference/)."""

import shutil
import sys
from pathlib import Path

from nominal_sphinx_theme import theme_options
from pygments.lexers import TextLexer

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
    "video",
    "nominal_sphinx_theme",
]

templates_path = [str(HERE / "_templates")]
exclude_patterns = [
    "_build",
    # contributor docs, at the top level and in any folder
    *[
        f"{prefix}{name}"
        for prefix in ("", "**/")
        for name in ("README.md", "AGENTS.md", "CLAUDE.md", "CONTRIBUTING.md")
    ],
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
# Public package exports are the API catalog; do not repeat them in docs pages.
autosummary_ignore_module_all = False

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_rtype = False
napoleon_use_ivar = True  # Attributes: sections as a field list, not duplicate targets

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# -- HTML: nominal-sphinx-theme (Shibuya, styled like the other Nominal docs) ----
html_theme = "shibuya"
html_title = "Nominal Python SDK"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_copy_source = False

# published on its own (GitHub Pages), not under the docs hub, so the Nominal logo leads to the hub
nominal_hub_url = "https://dev.nominal.io/"
nominal_ga_id = "G-XJXZ2G04E3"  # the Fern docs site's GA4 measurement ID

html_context = {
    "source_type": "github",
    "source_user": "nominal-io",
    "source_repo": "nominal-client",
    "source_version": "main",
    "source_docs_path": "/docs/src/",
}

# Right sidebar: on-page contents and edit link, no GitHub repo-stats box.
html_sidebars = {"**": ["sidebars/localtoc.html", "sidebars/edit-this-page.html"]}

html_theme_options = theme_options(
    github_url="https://github.com/nominal-io/nominal-client",
    nav_socials=["github"],
    # header tabs; the sidebar shows only the current tab's toctree groups
    nav_links=[
        {"title": "Guides", "url": "index"},
        {"title": "Examples", "url": "examples/index"},
        {"title": "SDK", "url": "reference/toplevel"},
    ],
    # left nav lists pages only; generated class/member pages are reached from their tables
    toctree_maxdepth=1,
)
add_module_names = False


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


def _clean_autosummary_stubs(app):
    """Remove stale generated API pages before autosummary runs, including during live preview."""
    for directory in (Path(app.srcdir) / "reference").rglob("generated"):
        if directory.is_dir():
            shutil.rmtree(directory)


def setup(app):
    # CSV examples are data, with no Pygments CSV lexer; retain their existing fences.
    app.add_lexer("csv", TextLexer)
    app.connect("builder-inited", _clean_autosummary_stubs, priority=100)
    app.connect("autodoc-process-signature", _drop_private_params)
    app.connect("autodoc-process-bases", _drop_object_base)
    app.connect("html-page-context", _hide_edit_link_on_stubs, priority=600)
