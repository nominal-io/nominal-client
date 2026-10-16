"""Sphinx config for the Nominal Python SDK guides, examples, and API reference."""

import sys
from pathlib import Path

from nominal_sphinx_theme import theme_options

import nominal.exceptions

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
    "sphinx_click",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxext.rediraffe",
    "examples",
    "video",
    "docstring_fences",
    "reference_presentation",
    "nominal_sphinx_theme",
]

templates_path = [str(HERE / "_templates")]
autosummary_generate = [str(HERE / "autosummary.rst")]
autosummary_filename_map = {"nominal.exceptions": "exceptions"}
autosummary_context = {
    "warning_names": [
        name for name, obj in vars(nominal.exceptions).items() if isinstance(obj, type) and issubclass(obj, Warning)
    ],
}

# Partials are included by guide pages rather than built as standalone pages.
exclude_patterns = ["guides/_snippets"]

# Keep published links working when reference pages move.
rediraffe_redirects = "redirects.txt"

# Single backticks in docstrings (`Dataset`) link to the named object when it
# resolves and render as code otherwise.
default_role = "py:obj"

# -- MyST ---------------------------------------------------------------------
myst_heading_anchors = 6
myst_enable_extensions = ["colon_fence", "deflist", "attrs_inline", "attrs_block", "fieldlist"]

# -- autodoc -------------------------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": True,  # show_if_no_docstring
    # stop at these bases so enum/exception/builtin/Ibis backend internals stay out
    "inherited-members": (
        "object,BaseException,BaseExceptionGroup,ExceptionGroup,Enum,str,int,float,dict,tuple,"
        "Handler,StreamHandler,Filterer,"
        "SQLBackend,BaseBackend,_FileIOHandler,CacheHandler,NoUrl"
    ),
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
napoleon_use_param = False

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# -- HTML: nominal-sphinx-theme (Shibuya, styled like the other Nominal docs) ----
html_theme = "shibuya"
html_title = "Nominal Python SDK"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_copy_source = False

nominal_hub_url = "https://docs.nominal.io/"
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
    toctree_maxdepth=1,
    nav_socials=["github"],
    nav_links=[
        {"title": "Guides", "url": "index"},
        {"title": "Examples", "url": "examples/index"},
        {"title": "SDK", "url": "sdk/index"},
    ],
)
