# Docs: agent and contributor notes

Scope: `docs/`, one Sphinx site ([nominal-sphinx-theme](https://github.com/nominal-io/nominal-sphinx-theme), on Shibuya; MyST Markdown) holding the existing Networking & TLS guide (`src/networking-tls.md`), generated examples, and the API reference (`src/reference/`), deployed from `main` to GitHub Pages. The layout and styling follow `nominal-io/instro`'s docs.

## Build

- `just build-docs`: clean build into `docs/_build/dirhtml`; warnings are errors. CI (`build-deploy-docs.yml`) runs it on PRs, uploads the site as a `docs-site` artifact, and deploys on merge to `main`.
- Extension regression tests: `uv run --all-packages --all-extras --group docs pytest docs/_tests --no-cov`.
- `just serve-docs`: live preview on http://127.0.0.1:8000, rebuilding on page, example and docstring edits.
- Toolchain: the `docs` dependency group, which needs Python >=3.12. If the project env is older: `uv sync --python 3.13 --all-packages --all-extras --group docs`.
- URLs are folder-style (`/reference/core/`), so browse a finished build through a server: `uv run python -m http.server -d docs/_build/dirhtml`.

## Structure

| Path | Contents |
|---|---|
| `conf.py`, `_ext/`, `_templates/` | Config, extensions, autosummary templates. The header, logos, fonts, analytics and the rest of the styling come from nominal-sphinx-theme. |
| `src/index.md` | Home page, and every sidebar group: one hidden `toctree` per caption, for all three sections. |
| `src/networking-tls.md` | The existing Networking & TLS guide. Additional guides and their assets are migrated in a separate PR. |
| `src/examples/` | Generated at build time from the repo's `examples/*.py` by `_ext/examples.py` (gitignored). Never edit. |
| `src/reference/` | API reference pages. Autosummary writes a page per class and member into gitignored `src/reference/**/generated/`. |

- **Sections:** the header tabs (Guides, Examples, SDK) are sections; the theme trims the sidebar to the current one by the tab URL's folder (`reference/`, `examples/`, else guides).
- **Source paths:** pages stay under `src/`, preserving the existing MkDocs URLs. Sphinx loads configuration from `docs/` with `-c docs`; edit links point at `docs/src/`.
- **Page head:** `# Title`, then an optional `{.lead}` paragraph (grey subtitle), with `myst: html_meta: description:` in front matter.

## Guide pages (MyST Markdown)

- Link pages as `/guides/path.md` or `/guides/path.md#anchor`; link API objects with `` {py:class}`~nominal.core.Dataset` `` or `` {py:meth}`~nominal.core.Dataset.add_tabular_data` ``, never by URL.
- Components: `:::{note}` / `tip` / `warning` admonitions; `::::{tab-set}` + `:::{tab-item}`; `:::{dropdown}`; `::::{grid}` + `:::{grid-item-card}`; `::::{container} steps` with `:::{container} step`; `` {abbr}`term (definition)` `` tooltips; `{button-link}`; `` {download}`text <path>` ``. An outer directive needs more colons than the ones it contains.

## Examples

Add a script under the repo's `examples/`; its page appears under Examples, titled from the first line of its module docstring (the rest of the docstring is shown above the code). Ruff lints `examples/`.

## API pages (`src/reference/`)

- Each page is `# Title`, a sentence, then a module-level `{eval-rst}` `autosummary` block with `:toctree: generated`. Sphinx discovers public objects from the module, respecting `__all__` when present. Add exports in Python, not a second object inventory here. Autodoc emits reST, so API directives go in `{eval-rst}` blocks.
- `_templates/autosummary/class.rst`: enums and classes without public methods get one page; other classes get member tables with a page per member.
- Docstrings use Google sections (`Args:`, `Returns:`, `Raises:` without NumPy underlines) and reStructuredText markup: `*emphasis*`, double backticks for literals, and `::` plus an indented block for code examples. Standalone notes use Google `Note:` sections with indented bodies. Notes nested within parameter and return descriptions use native reST `.. note::` directives, with a blank line before their indented body; this preserves the argument section for the docstring linter. Keep consecutive caveats for the same parameter in one note box, separated by blank lines. Continuation prose stays at its paragraph indent; leave a blank line before lists and code blocks. `_ext/markdown_docstrings.py` preserves existing Markdown fences and HTTP links during migration; it is not a general Markdown parser.
- A new public module needs a page here and an entry in the SDK toctrees in `src/index.md`.
