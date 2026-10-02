# Docs: agent and contributor notes

Scope: `docs/`, one Sphinx site (Shibuya theme, MyST Markdown) holding the guides (`guides/`), generated examples, and the API reference (`sdk/`), deployed from `main` to GitHub Pages. The layout and styling follow `nominal-io/instro`'s docs.

## Build

- `just build-docs`: clean build into `docs/_build/dirhtml`; warnings are errors. CI (`build-deploy-docs.yml`) runs it on PRs, uploads the site as a `docs-site` artifact, and deploys on merge to `main`.
- `just serve-docs`: live preview on http://127.0.0.1:8000, rebuilding on page, example and docstring edits.
- Toolchain: the `docs` dependency group, which needs Python >=3.12. If the project env is older: `uv sync --python 3.13 --all-packages --all-extras --group docs`.
- URLs are folder-style (`/guides/quickstart/`), so browse a finished build through a server: `python -m http.server -d docs/_build/dirhtml`.

## Structure

| Path | Contents |
|---|---|
| `conf.py`, `_ext/`, `_templates/`, `_static/` | Config, extensions, autosummary templates, header partials (Get demo / Open app, analytics), CSS, logos. |
| `index.md` | Home page, and every sidebar group: one hidden `toctree` per caption, for all three sections. |
| `guides/` | How-to guides, migrated from the Fern docs. File paths mirror the old `docs.nominal.io/core/sdk/python-client/<path>` slugs. |
| `guides/_snippets/` | Partials pulled in with `{include}` (excluded as pages), and `code/` scripts pulled in with `{literalinclude}`. |
| `guides/images/`, `guides/data/` | Images, screen recordings (`.mp4`) and downloadable sample data. |
| `examples/` | Generated at build time from the repo's `examples/*.py` by `_ext/examples.py` (gitignored). Never edit. |
| `sdk/` | API reference pages. Autosummary writes a page per class and member into gitignored `sdk/generated/`. |

- **Sections:** the header tabs (Guides, Examples, SDK) are sections; `_ext/sections.py` trims the sidebar to the current one by URL prefix (`sdk/`, `examples/`, else guides).
- **Page head:** `# Title`, then an optional `{.lead}` paragraph (grey subtitle), with `myst: html_meta: description:` in front matter.

## Guide pages (MyST Markdown)

- Link pages as `/guides/path.md` or `/guides/path.md#anchor`; link API objects with `` {py:class}`~nominal.core.Dataset` `` or `` {py:meth}`~nominal.core.Dataset.add_tabular_data` ``, never by URL.
- Components: `:::{note}` / `tip` / `warning` admonitions; `::::{tab-set}` + `:::{tab-item}`; `:::{dropdown}`; `::::{grid}` + `:::{grid-item-card}`; `::::{container} steps` with `:::{container} step`; `` {abbr}`term (definition)` `` tooltips; `{button-link}`; `` {download}`text <path>` ``. An outer directive needs more colons than the ones it contains.
- Videos: `` ```{video} /guides/images/x.mp4 `` (`:loop:` for GIF-like recordings, `:alt:`), or a URL. Don't commit GIFs; convert them to MP4.

## Examples

Add a script under the repo's `examples/`; its page appears under Examples, titled from the first line of its module docstring (the rest of the docstring is shown above the code). Ruff lints `examples/`.

## API pages (`sdk/`)

- Each page is `# Title`, a sentence, then `{eval-rst}` `autosummary` blocks with `:toctree: generated`. Autodoc emits reST, so API directives go in `{eval-rst}` blocks.
- `_templates/autosummary/class.rst`: enums and classes without public methods get one page; other classes get member tables with a page per member.
- Docstrings are Google style and may use Markdown fences and `[text](url)` links: `_ext/markdown_docstrings.py` converts them before napoleon. Other Markdown (indented code blocks, `_emphasis_`, lists without a blank line before) breaks the strict build.
- A new public module needs a page here and an entry in the SDK toctrees in `index.md`.
