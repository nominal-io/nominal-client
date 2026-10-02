# Docs migration: mkdocs + Fern → Sphinx

`docs/` is now one Sphinx site with three sections: **Guides** (the Python SDK pages, migrated from Fern), **Examples** (generated from `examples/`), and **SDK** (the API reference, migrated from mkdocs). Theme, layout and tooling follow `nominal-io/instro`'s `docs/` (Shibuya + MyST); the header follows the Fern site (Nominal logo, "Get demo", "Open app"). Conventions for working in `docs/` are in [`AGENTS.md`](./AGENTS.md).

Out of scope: hosting, domains and redirects. The `fern-docs` repo is unchanged; its pages were copied, not moved.

## What changed

- **Toolchain.** The mkdocs dependencies are replaced by a `docs` dependency group (Sphinx 9, Shibuya, MyST, sphinx-design, sphinx-copybutton, sphinx-click), which needs Python >=3.12; the package itself still supports 3.10+. `docs/mkdocs.yml` and `docs/src/` are gone.
- **Build and CI.** `just build-docs` (strict: warnings fail) and `just serve-docs` (live preview). `build-deploy-docs.yml` builds with Python 3.13, uploads a `docs-site` artifact on PRs for preview, and deploys `docs/_build/dirhtml` to GitHub Pages from `main` as before.
- **SDK reference (`docs/sdk/`).** Every page from the mkdocs nav is ported, grouped the same way, plus a polars page that was missing. `nominal.core` is split into topic sections. Classes get a page with member tables and a page per method (the instro layout); enums and plain data classes get one page.
- **Docstrings.** Fixing the strict build meant correcting about 40 docstrings: numpydoc `----` underlines under Google headers, indented example code (now fenced), over-indented `NOTE:` continuation lines, a missing `Args:` header, and a few others. Markdown fences and `[text](url)` links stay as they are (they render on IDE hover); `_ext/markdown_docstrings.py` converts them for Sphinx.
- **Guides (`docs/guides/`).** All 32 pages of the Fern Python section (`fern/products/core.yaml`), plus Networking & TLS from the mkdocs site. Navigation follows Fern's sections. File paths mirror the Fern slugs, so `docs.nominal.io/core/sdk/python-client/<path>` maps to `guides/<path>/`.
  - MDX components became MyST: admonitions, tab sets, dropdowns, steps, figures, buttons, `{abbr}` tooltips, `{download}`.
  - The `<Code>` snippets (112 scripts) are copied to `guides/_snippets/code/` and shown with `literalinclude`; the 30 shared `<Markdown>` snippets became `{include}` partials in `guides/_snippets/` (glossary snippets became tooltip text).
  - Links to other Python pages are checked `{doc}`-style links; links to the old mkdocs reference became checked `{py:obj}` cross-references; links to the rest of the Fern site are absolute `https://docs.nominal.io/…` URLs.
  - The quickstart's interactive variables panel (Fern-only) became a table of placeholders.
  - Images are copied from Fern's LFS store. GIFs over 1 MB became MP4 (the largest, 45 MB, is now 1 MB), shown with a new `{video}` directive (`_ext/video.py`), which also replaced the raw `<video>` tags.
- **Examples.** `examples/fsae_asset_upload.py` (the quickstart's script) is the first example; the quickstart includes it rather than keeping a copy. `_ext/examples.py` generates a page per script and a flat index. Ruff now lints `examples/` and `docs/`.

## Open items

- [ ] **Sample CSV.** `guides/data/racecar_dataset.csv` (9.7 MB, the quickstart download) is not committed yet: decide whether to commit it, ship it gzipped (2.9 MB), or link to a hosted copy.
- [ ] **External links, once the site has its final URL.** These still point at the Fern site, which keeps serving the old pages:
  - `README.md` (two links);
  - `docs_link` in `nominal/cli/util/verify_connection.py`;
  - `Documentation` in `pyproject.toml`'s `[project.urls]`.
- [ ] **Branch protection.** Require the docs build on `main`.
- [ ] **Fern.** When the new site is live, retire or redirect the Fern Python section (`fern-docs` repo).

## Verified

- `just build-docs`: strict, zero warnings.
- `just check` (ruff format and lint, mypy), `just test`.
- All 32 Fern Python pages and every module page from the mkdocs nav exist in the new site.
- Home, a guide, an API page and the example rendered and compared against instro's look.
