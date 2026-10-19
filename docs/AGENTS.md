# Documentation

Markdown pages stay in `src/`, with guide, example, reference and project
navigation declared in `src/index.md`. Sphinx reads configuration from
`conf.py`; MyST parses Markdown, and `nominal-sphinx-theme` supplies the styling.

Header tabs are Guides, Examples and SDK. The shared theme limits the sidebar to
the active tab: `examples/` pages belong to Examples, `sdk/` pages to SDK,
and other pages to Guides. Keep each captioned root toctree within one section.

- `just build-docs` builds `docs/_build/dirhtml` and fails on warnings, including in CI.
- `just serve-docs` previews at http://127.0.0.1:8000 and watches pages, examples, docstrings and included files.
  Both commands remove generated example/reference pages and HTML before rebuilding.
- The `docs` dependency group requires Python >=3.12. uv selects a compatible interpreter
  and may recreate an older project environment; the SDK still supports Python >=3.11.
- Autodoc imports the live-video bindings. Linux builds need the GStreamer runtime;
  the Ubuntu docs job installs `libgstreamer-plugins-bad1.0-0`.
- Test docstring and signature rendering with
  `uv run --all-packages --all-extras --group docs pytest docs/_tests --no-cov`.

API references use native `autosummary` with templates under `_templates/autosummary/`.
Members come from Python exports; do not maintain member lists or generated source pages.
The exceptions template separates errors and warnings without a member list.
The overview (`src/sdk/exceptions.rst`) and detail pages (`src/sdk/generated/`) are ignored
by Git and the preview watcher. The left sidebar shows only the overview.
Core, timestamp, experiment, and integration pages include native module summaries
from `src/sdk/_summaries/`. Their module entries live in `autosummary.rst`; Sphinx discovers
members from `__all__` when present, otherwise from public local definitions. The module
summary and object templates generate tables and detail pages without member lists.
Core's landing page links to authored topic pages, with only Core SDK in the left sidebar.
`core_topic_modules` in `conf.py` assigns defining modules to topics. Each topic includes its
section of the native Core summary, combining the modules' public package exports into one
set of class, enum, function, constant, and type-alias tables. All SDK references share the
same table template. Keep topic prose in its Markdown page, not in generator hooks or SDK
module docstrings. Internal owning modules may contribute public exports; implementation
helpers and the `nominal.core.exceptions` shim stay out. The strict build rejects unassigned
or repeated exports. Legacy standalone-video objects retain reference targets for existing
links but are absent from the topic directory. Native canonical targets preserve defining
module names alongside public import names.
Timestamp and Ibis pages render module docstrings above the API reference. Their seeds use
`module-summary-no-prose.rst` to register modules without repeating that prose in the tables.
Classes, enums, and functions get detail pages. Constants and type aliases use separate
compact tables with values and linked type definitions. `conf.py` reads the same native
module seed to supply enum names, constant values, and type aliases as plain template data;
alias discovery includes explicit `TypeAlias` annotations, including aliases of concrete types;
Sphinx formats definitions with `restify`. Constant rows come from `__all__` or
Sphinx's documented local attributes, keeping imported implementation constants out.
Native `py:data` targets preserve their links without typesetting a second definition block.
Do not dump implementation type variables into the reference.
Summary fragments are excluded from page discovery and ignored by Git and the watcher;
only the Markdown landing page belongs in the sidebar.
`_static/reference-links.js` forwards missing class/member fragments using the native
summary links, preserving query strings and leaving existing on-page targets alone.
The Core directory supplies generated reference destinations so old object bookmarks
reach their detail page or their topic's type-alias table directly.
Test this behavior with `node --test docs/_tests/reference-links.test.mjs` (Node 22; no npm dependencies).
Packages with `__all__` need no `:imported-members:` option.
Classes list attributes and properties first, then alphabetical methods. Dataclass
fields follow declaration order (including inherited fields), with
properties after the fields. Methods and attributes render on their class's detail page.
The reference presentation extension uses native
Sphinx contents flags to omit attributes and aliases from navigation, preserving their body
and link targets. Integrations and Experimental pages are listed directly under
captioned root toctrees. Their folder indexes remain standalone overview pages for
existing links, with ordinary `{doc}` links and `orphan` metadata. Leave toctrees
out of those overviews so missing sidebar entries fail the strict build. New
reference pages need an entry in the root `index.md` toctree. Explicit `autodata`
directives cover imported type aliases and constants that native `automodule` omits.

Author SDK docstrings using the shared [documentation policy](../.agents/conventions/documentation.md#docstring-markup),
including when changing SDK files outside `docs/`. `_ext/docstring_fences.py` converts
fenced blocks before Napoleon parses sections and uses sphinx-click's description event
for CLI help.

`_ext/reference_presentation.py` displays unquoted dataclass annotations and omits
`repr=False` fields from both constructors and reference bodies, using dataclass
metadata without per-field documentation markers.


Guides live in `src/guides/`, with reusable prose in `_snippets/` and code fragments
in `_snippets/code/`. Use native MyST `{include}` and `{literalinclude}` directives
to include them. Partials are excluded from page discovery. Keep images, recordings
and downloadable data under `src/guides/`; link API objects with Python roles such
as `` {py:class}`~nominal.core.Dataset` ``.

MyST colon fences support nested admonitions and Sphinx Design tabs, dropdowns and
cards. Outer directives need more colons than nested directives.

Scripts under `docs/examples/` get pages automatically from
`_ext/examples.py`. The first line of the module docstring supplies the title;
the remainder supplies the introduction. Generated `src/examples/` pages are
ignored by Git and the preview watcher. Edit the script rather than its page.

Use `{video}` to embed local recordings or remote URLs. Local recordings use
Sphinx's download collector for copying and missing-file warnings; `:loop:`
plays muted like a GIF, otherwise the player shows controls. `_static/custom.css`
keeps recordings within the content column; the shared theme owns site styling.

API reference pages live under `src/sdk/`. Keep retired public URLs in
`src/redirects.txt`; `sphinxext.rediraffe` builds redirects and preserves query
strings and section anchors. These entries record moved URLs, not an API inventory.
