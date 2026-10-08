"""Sphinx config for the Nominal Python SDK docs: guides (src/), examples, and the API reference (src/reference/)."""

import sys
from pathlib import Path

from nominal_sphinx_theme import theme_options

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
    "api_reference",
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
# api_reference calls the native generator and prunes only obsolete stubs.
autosummary_generate = False
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
html_static_path = []
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
