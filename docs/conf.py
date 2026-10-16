"""Sphinx config for the Nominal Python SDK guides, examples, and API reference."""

import sys
from inspect import get_annotations
from pathlib import Path
from pkgutil import resolve_name
from typing import TypeAlias, TypeVar, get_origin

from nominal_sphinx_theme import theme_options
from sphinx.ext.autosummary.generate import find_autosummary_in_files
from sphinx.util.inspect import isenumclass, object_description
from sphinx.util.typing import restify

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
autosummary_ignore_module_all = False
autosummary_filename_map = {"nominal.exceptions": "exceptions"}
autosummary_context = {
    "module_data": {},
    "warning_names": [
        name for name, obj in vars(nominal.exceptions).items() if isinstance(obj, type) and issubclass(obj, Warning)
    ],
}

# Read the same module seed as autosummary; template context must contain plain data.
for _entry in find_autosummary_in_files(autosummary_generate):
    _module = resolve_name(_entry.name)
    _annotations = get_annotations(_module, eval_str=True)
    # Preserve code quotes when Sphinx applies typography to inline type expressions.
    _type_aliases = {
        _name: restify(_value, mode="smart").replace("'", r"\'").replace('"', r"\"")
        for _name, _value in vars(_module).items()
        if get_origin(_value) is not None or _annotations.get(_name) is TypeAlias
    }
    autosummary_context["module_data"][_entry.name] = {
        "exports": getattr(_module, "__all__", []),
        "enums": [_name for _name, _value in vars(_module).items() if isenumclass(_value)],
        "type_aliases": _type_aliases,
        "constants": {
            _name: f"``{object_description(_value)}``"
            for _name, _value in vars(_module).items()
            if _name.isupper()
            and not _name.startswith("_")
            and _name not in _type_aliases
            and not isinstance(_value, TypeVar)
        },
    }

# Partials are included by guide pages rather than built as standalone pages.
exclude_patterns = ["guides/_snippets", "sdk/_summaries"]

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
html_js_files = ["reference-links.js"]
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
