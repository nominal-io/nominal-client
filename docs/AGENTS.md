# Documentation

Markdown pages stay in `src/`, with guide, example, reference and project
navigation declared in `src/index.md`. Sphinx reads configuration from
`conf.py`; MyST parses Markdown, and `nominal-sphinx-theme` supplies the styling.

Header tabs are Guides, Examples and SDK. The shared theme limits the sidebar to
the active tab: `examples/` pages belong to Examples, `sdk/` pages to SDK,
and other pages to Guides. Keep each captioned root toctree within one section.

- `just build-docs` builds `docs/_build/dirhtml` and fails on warnings, including in CI.
- `just serve-docs` previews at http://127.0.0.1:8000 and watches pages, examples, docstrings and included files.
  Both commands remove generated example pages and HTML before rebuilding.
- The `docs` dependency group requires Python >=3.12. uv selects a compatible interpreter
  and may recreate an older project environment; the SDK still supports Python >=3.11.
- Autodoc imports the live-video bindings. Linux builds need the GStreamer runtime;
  the Ubuntu docs job installs `libgstreamer-plugins-bad1.0-0`.
- Test docstring and signature rendering with
  `uv run --all-packages --all-extras --group docs pytest docs/_tests --no-cov`.

API pages use native `automodule`/`autoclass` directives in MyST `{eval-rst}` blocks.
Members come from Python exports and render inline; do not maintain member lists or
generated source pages. Packages with `__all__` need no `:imported-members:` option.
Classes list attributes and properties first, then alphabetical methods. Dataclass
fields follow declaration order (including inherited fields), with
properties after the fields. Core keeps class/function-level local contents (`tocdepth: 2`);
methods and attributes render inline. The reference presentation extension uses native
Sphinx contents flags to omit attributes and aliases from navigation, preserving their body
and link targets. Integrations and Experimental pages are listed directly under
captioned root toctrees. Their folder indexes remain standalone overview pages for
existing links, with native glob toctrees and `orphan` metadata. New reference pages
need an entry in the root `index.md` toctree. Explicit `autodata` directives cover
imported type aliases and constants that native `automodule` omits.

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
