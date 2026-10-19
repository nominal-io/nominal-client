"""Sphinx config for the Nominal Python SDK guides, examples, and API reference."""

import sys
from collections import Counter
from inspect import get_annotations
from pathlib import Path
from pkgutil import resolve_name
from typing import TypeAlias, TypeVar, get_origin

from nominal_sphinx_theme import theme_options
from sphinx.errors import ConfigError
from sphinx.ext.autosummary.generate import find_autosummary_in_files
from sphinx.util.inspect import isenumclass, object_description
from sphinx.util.typing import restify

import nominal.core
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

# Curate topic ownership by module; names still come from nominal.core.__all__.
core_topic_modules = {
    "client": ("client", "workspace", "user", "_utils.networking"),
    "assets-and-runs": ("asset", "run", "attachment", "comment"),
    "datasets-and-files": ("dataset", "dataset_file", "video_dataset_file", "filetype", "ingestion_job"),
    "channels-and-units": ("channel", "unit"),
    "streaming": ("_stream.write_stream", "log"),
    "containerized-extractors": ("containerized_extractor", "container_image", "secret"),
    "events": ("event", "_event_types"),
    "automatic-validation": ("checklist", "_checklist_types", "data_review", "integration"),
    "workbooks": ("workbook", "workbook_template"),
    "markings": ("marking", "elements"),
    "shared-types": ("bounds", "_utils.api_tools", "_utils.query_tools", "_utils.api_types"),
}
# Keep deprecated object links working without listing them in the directory.
core_legacy_modules = ("video", "video_file")
core_module_exports = {}
for _suffix in [name for modules in core_topic_modules.values() for name in modules] + list(core_legacy_modules):
    _module_name = f"nominal.core.{_suffix}"
    _module = resolve_name(_module_name)
    _annotations = get_annotations(_module, eval_str=True)
    core_module_exports[_module_name] = [
        name
        for name in nominal.core.__all__
        if vars(_module).get(name) is getattr(nominal.core, name)
        and (
            getattr(getattr(nominal.core, name), "__module__", None) == _module_name
            or _annotations.get(name) is TypeAlias
        )
    ]

autosummary_context["core_topics"] = {
    slug: {
        "modules": [f"nominal.core.{module}" for module in modules],
        "exports": [name for module in modules for name in core_module_exports[f"nominal.core.{module}"]],
    }
    for slug, modules in core_topic_modules.items()
}
autosummary_context["core_legacy_exports"] = [
    name for module in core_legacy_modules for name in core_module_exports[f"nominal.core.{module}"]
]
_coverage = Counter(name for names in core_module_exports.values() for name in names)
_missing = sorted(set(nominal.core.__all__) - _coverage.keys())
_duplicates = sorted(name for name, count in _coverage.items() if count > 1)
if _missing or _duplicates:
    raise ConfigError(f"Core topic coverage: unassigned exports {_missing}, repeated exports {_duplicates}")

# Read the same module seed as autosummary; template context must contain plain data.
_seed_modules = [_entry.name for _entry in find_autosummary_in_files(autosummary_generate)]
for _module_name in _seed_modules:
    _module = resolve_name(_module_name)
    _annotations = get_annotations(_module, eval_str=True)
    # Preserve code quotes when Sphinx applies typography to inline type expressions.
    _type_aliases = {
        _name: restify(_value, mode="smart").replace("'", r"\'").replace('"', r"\"")
        for _name, _value in vars(_module).items()
        if get_origin(_value) is not None or _annotations.get(_name) is TypeAlias
    }
    autosummary_context["module_data"][_module_name] = {
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

autosummary_context["module_data"]["nominal.core"]["canonical_names"] = {
    name: f"{module}.{name}" for module, names in core_module_exports.items() for name in names
}
autosummary_context["core_reference_pages"] = {
    f"#nominal.core.{name}": (
        f"{slug}/#nominal.core.{name}"
        if name in autosummary_context["module_data"]["nominal.core"]["type_aliases"]
        else f"../generated/nominal.core.{name}/#nominal.core.{name}"
    )
    for slug, topic in autosummary_context["core_topics"].items()
    for name in topic["exports"]
}
autosummary_context["core_reference_pages"].update(
    {
        f"#nominal.core.{name}": f"../generated/nominal.core.{name}/#nominal.core.{name}"
        for name in autosummary_context["core_legacy_exports"]
    }
)

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
