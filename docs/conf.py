"""Sphinx config for the Nominal Python SDK docs: the existing Markdown pages and inline API reference."""

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
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_click",
    "sphinx_copybutton",
    "docstring_fences",
    "reference_presentation",
    "nominal_sphinx_theme",
]

# Single backticks in docstrings (`Dataset`) link to the named object when it
# resolves and render as code otherwise.
default_role = "py:obj"

# -- MyST ---------------------------------------------------------------------
myst_heading_anchors = 6

# -- autodoc -------------------------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": True,  # show_if_no_docstring
    # stop at these bases so enum/exception/builtin internals stay out
    "inherited-members": (
        "object,BaseException,BaseExceptionGroup,ExceptionGroup,Enum,str,int,float,dict,tuple,"
        "Handler,StreamHandler,Filterer"
    ),
    "member-order": "bysource",
    "show-inheritance": True,
}
autoclass_content = "both"  # merge_init_into_class
autodoc_typehints = "signature"
autodoc_preserve_defaults = True
python_use_unqualified_type_names = True  # show_root_full_path: false
toc_object_entries_show_parents = "hide"

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_rtype = False
napoleon_use_param = False  # keep the first paragraph inline when an argument contains a note

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

nominal_section_sidebar = False

html_theme_options = theme_options(
    github_url="https://github.com/nominal-io/nominal-client",
    nav_socials=["github"],
    nav_links=[
        {"title": "Documentation", "url": "https://docs.nominal.io/core/sdk/python-client/quickstart"},
        {"title": "Nominal", "url": "https://nominal.io"},
    ],
    # Keep the existing page-level navigation; reference objects appear in the local contents.
    toctree_maxdepth=1,
)
add_module_names = False
