# Documentation

The existing Markdown pages stay in `src/`, with the Home, Reference, Guides and
Development navigation declared in `src/index.md`. Sphinx reads configuration from
`conf.py`; MyST parses Markdown, and `nominal-sphinx-theme` supplies the styling.

- `just build-docs` builds `docs/_build/dirhtml` and fails on warnings, including in CI.
- `just serve-docs` previews at http://127.0.0.1:8000 and watches pages, docstrings and included files.
- The `docs` dependency group requires Python >=3.12. uv selects a compatible interpreter
  and may recreate an older project environment; the SDK still supports Python >=3.10.
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
and link targets. Integrations and Experimental have folder indexes with native glob
toctrees: adding a Markdown reference page to either folder automatically adds it to
the group. Explicit `autodata` directives cover imported type aliases and constants
that native `automodule` omits. Document public constants with attribute docstrings
immediately after their assignment; keep implementation comments ordinary.
Reference pages outside those groups need an entry in the root `index.md` toctree.

Docstrings retain Google sections and reStructuredText markup. Single backticks link
resolvable Python objects; double backticks render literal code. Prefer triple-backtick
code fences with a language. `_ext/docstring_fences.py` converts only fenced blocks
before Napoleon parses sections and uses sphinx-click's description event for CLI help.
Standalone notes use Google `Note:` sections; notes within argument descriptions use
native `.. note::` directives with an indented body. Preserve their position and
formatting. See the [documentation policy](../.agents/conventions/documentation.md).

`_ext/reference_presentation.py` displays unquoted dataclass annotations and omits
`repr=False` fields from both constructors and reference bodies, using dataclass
metadata without per-field documentation markers. Keep maintainer TODOs in code comments.
