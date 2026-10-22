# Documentation site

- Writing rules for pages and docstrings come from the `nominal-docs` skill. See the root [AGENTS.md](../AGENTS.md).
- This file covers this site's folders, build commands and layout.

## Contents

- [Build](#build)
- [Layout](#layout)
- [Guides](#guides)
- [Examples](#examples)
- [Reference](#reference)
  - [Pages and navigation](#pages-and-navigation)
  - [Core topics](#core-topics)
  - [Members and tables](#members-and-tables)
  - [Links](#links)
- [Docstring rendering](#docstring-rendering)

## Build

- `just build-docs` builds `docs/_build/dirhtml` and fails on warnings, including in CI.
- `just serve-docs` previews at http://127.0.0.1:8000. It watches pages, examples, docstrings and included files.
- Both commands remove generated example and reference pages and HTML before rebuilding.
- The `docs` dependency group needs Python >=3.12. uv picks a compatible interpreter and may recreate an older project environment. The SDK still supports Python >=3.11.
- Autodoc imports the live-video bindings. Linux builds need the GStreamer runtime; the Ubuntu docs job installs `libgstreamer-plugins-bad1.0-0`.
- Docstring and signature rendering tests: `uv run --all-packages --all-extras --group docs pytest docs/_tests --no-cov`.
- Reference-link script tests: `node --test docs/_tests/reference-links.test.mjs` (Node 22, no npm dependencies).

## Layout

- Pages are Markdown in `src/`, parsed by MyST. Sphinx config is `conf.py`; `nominal-sphinx-theme` supplies the styling.
- `src/index.md` declares guide, example, reference and project navigation.
- Header tabs: Guides, Examples and SDK. The sidebar shows only the active tab.
  - `examples/` pages belong to Examples, `sdk/` pages to SDK, everything else to Guides.
  - Keep each captioned root toctree within one section.
- Keep retired public URLs in `src/redirects.txt`. `sphinxext.rediraffe` builds the redirects, keeping query strings and section anchors. The entries record moved URLs, not an API inventory.
- `_static/custom.css` keeps recordings within the content column. The shared theme owns all other styling.

## Guides

- Guides live in `src/guides/`. Keep their images, recordings and downloadable data there too.
- Reusable prose goes in `src/guides/_snippets/`, code fragments in `src/guides/_snippets/code/`. Include them with MyST `{include}` and `{literalinclude}`. Partials are excluded from page discovery.
- MyST colon fences support nested admonitions and Sphinx Design tabs, dropdowns and cards. Outer directives need more colons than nested ones.
- `{video}` embeds local recordings or remote URLs.
  - Local recordings go through Sphinx's download collector, which copies them and warns on missing files.
  - `:loop:` plays muted like a GIF. Without it, the player shows controls.

## Examples

- Scripts in `docs/examples/` get pages automatically from `_ext/examples.py`.
- The first line of the module docstring is the title. The rest is the introduction.
- Generated `src/examples/` pages are ignored by Git and the preview watcher. Edit the script, not its page.

## Reference

### Pages and navigation

- Reference pages live in `src/sdk/` and use native `autosummary`, with templates in `_templates/autosummary/`.
- Members come from Python exports. Don't maintain member lists or generated source pages.
- Generated files are ignored by Git and the preview watcher: the exceptions overview (`src/sdk/exceptions.rst`), detail pages (`src/sdk/generated/`) and summary fragments (`src/sdk/_summaries/`).
  - The sidebar shows only the exceptions overview. Its template splits errors and warnings, without a member list.
  - Summary fragments are excluded from page discovery. Only the Markdown landing page belongs in the sidebar.
- New reference pages need an entry in the root `index.md` toctree.
- Integrations and Experimental pages are listed directly under captioned root toctrees.
  - Their folder indexes stay as standalone overview pages for existing links, with ordinary `{doc}` links and `orphan` metadata.
  - Leave toctrees out of those overviews, so a missing sidebar entry fails the strict build.

### Core topics

- Core, timestamp, experiment and integration pages include native module summaries from `src/sdk/_summaries/`.
  - Their module entries live in `autosummary.rst`.
  - Sphinx finds members from `__all__` when present, otherwise from public local definitions.
  - Packages with `__all__` need no `:imported-members:` option.
- Core's landing page links to authored topic pages, with only Core SDK in the sidebar.
- `core_topic_modules` in `conf.py` assigns defining modules to topics.
  - Each topic includes its section of the Core summary, combining the modules' public package exports into one set of tables.
  - Internal owning modules may contribute public exports. Implementation helpers and the `nominal.core.exceptions` shim stay out.
  - The strict build rejects unassigned or repeated exports.
- Keep topic prose in its Markdown page, not in generator hooks or SDK module docstrings.
- Topic pages tell readers to import from `nominal.core`.
- Legacy standalone-video objects keep reference targets for existing links but are left out of the topic directory.
- Timestamp and Ibis pages render module docstrings above the API reference. Their seeds use `module-summary-no-prose.rst` so the tables don't repeat that prose.

### Members and tables

- Classes, enums and functions get detail pages. All SDK references share one table template.
- Constants and type aliases get compact tables with values and linked type definitions.
  - `conf.py` reads the native module seed to supply enum names, constant values and type aliases as template data.
  - Alias discovery includes explicit `TypeAlias` annotations, including aliases of concrete types. Sphinx formats definitions with `restify`.
  - Constant rows come from `__all__` or Sphinx's documented local attributes, so imported implementation constants stay out.
  - Explicit `autodata` directives cover imported type aliases and constants that `automodule` omits.
  - Don't put implementation type variables in the reference.
- Class pages list attributes and properties first, then methods alphabetically.
  - Dataclass fields follow declaration order, including inherited fields, with properties after them.
  - Methods and attributes render on their class's detail page.
- `_ext/reference_presentation.py`:
  - Uses Sphinx contents flags to leave attributes and aliases out of navigation, keeping their bodies and link targets.
  - Shows unquoted dataclass annotations.
  - Omits `repr=False` fields from constructors and reference bodies, using dataclass metadata.

### Links

- Canonical targets keep defining object names alongside public import names, without registering implementation modules.
- `py:data` targets keep their links without a second definition block.
- The Core directory includes a hidden, HTML-only autosummary table without a toctree. Sphinx resolves its links to detail pages and type-alias tables.
- `_static/reference-links.js` uses those links to forward missing class and member fragments.
  - It keeps query strings and leaves existing on-page targets alone.
  - It doesn't duplicate builder paths or output filenames.

## Docstring rendering

- `_ext/docstring_fences.py` converts fenced blocks before Napoleon parses sections. For CLI help, it hooks sphinx-click's description event.
