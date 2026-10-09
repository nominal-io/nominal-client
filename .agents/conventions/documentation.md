# Docstrings and rendered documentation

These conventions apply to new and changed code. Read the affected implementation and
callers alongside this guide; examples do not require unrelated code to be normalized.

The site uses Sphinx autodoc and Napoleon for SDK docstrings, and MyST for Markdown pages.
Read the [site setup](../../docs/AGENTS.md) and [renderer configuration](../../docs/conf.py)
even when only SDK source files change. This guide owns authoring conventions; the site
guide owns reference layout, navigation, and build setup.

## Caller-facing behavior

- Public APIs need concise Google-style docstrings describing caller-visible behavior.
  Include `Args`, `Returns`, and `Raises` where they add information; simple properties or
  obvious accessors do not need boilerplate sections. Annotations own type declarations.
  Ruff intentionally disables several missing-docstring rules in `pyproject.toml`; a clean
  lint result does not establish that a new public API has adequate documentation.
- Document consequential defaults, `None`/empty distinctions, units and time zones,
  in-place mutation, completion semantics, and actionable failure conditions. Examples
  must match the actual API. Keep request-building details and maintainer TODOs in ordinary
  implementation comments.

## Docstring markup

- Use standard Google sections and their indentation. Keep parameter caveats inside the
  corresponding `Args:` description as separate paragraphs starting with `**Note:**`.
  Keep the separating blank lines, additional paragraphs, and examples attached to that
  parameter. Do not replace these with nested `Note:` sections or `.. note::` directives.
- Use a top-level Google `Note:` section for a standalone caveat; Sphinx renders it as a
  note box. A return-value caveat can follow `Returns:` in its own `Note:` section.
  Do not invent section names or custom aliases to work around parsing or lint.
- Prefer triple-backtick fences with a language such as `python`, `matlab`, or `text` for
  new or edited code examples, rather than `.. code-block::` or a prose line ending in
  `::`. Match opening and closing fence indentation and preserve the code's own indentation.
  The existing [fence extension](../../docs/_ext/docstring_fences.py) handles this syntax
  before Napoleon parses sections, including CLI help.
- Docstring prose uses reStructuredText inline markup, not arbitrary Markdown. Single
  backticks use the configured Python object role: resolvable objects become links,
  otherwise they render as code. Double backticks force literal code, such as parameter
  names and expressions. Choose by meaning; do not mechanically normalize the repository.
  Markdown pages under `docs/src/` use MyST syntax directly.
- Put placeholders and literal expressions in supported inline code or fenced blocks;
  unescaped `<placeholder>` text can disappear as HTML. Preserve useful operators and
  examples rather than banning angle brackets.
- Document public constants with attribute docstrings immediately after their assignments,
  as in [nominal.ts](../../nominal/ts/__init__.py). Keep ordinary comments ordinary:
  do not turn them into `#:` documentation comments or add `:meta private:` markers.
  Reference visibility already follows existing dataclass `repr=False` metadata.

Example layout, using the behavior of [Channel.update](../../nominal/core/channel.py):

````python
"""Replace channel metadata and refresh the local instance.

Args:
    unit: Unit symbol to apply to the channel.

        **Note:** Passing ``None`` clears the unit. Omitting it leaves the unit unchanged.

Note:
    Only supplied metadata is replaced.

Example:
    ```python
    channel = channel.update(unit="m/s")
    ```
"""
````

## Generation and review

- Prefer native Sphinx features and package exports over custom parsers, duplicated API
  catalogs, or manually synchronized member lists. Use the current
  [reference setup](../../docs/AGENTS.md) for ordering and visibility; do not reorder SDK
  definitions or change runtime metadata just to alter the rendered presentation.
- When a docstring does not fit the supported conventions, first fix its structure while
  preserving its meaning and parameter-note placement. Use the existing fence extension
  and native markup before adding parser hooks, compatibility adapters, or per-object
  plumbing. A workaround must solve a demonstrated limitation with a bounded scope.
- Review changed documentation against the implementation and these conventions. Older
  supported spellings in untouched docstrings are not defects solely for differing from
  the current authoring style. Confirm the configured parser's behavior before alleging a
  rendering defect, and keep justified exceptions explicit.
- Validate according to [AGENTS.md](../../AGENTS.md) and the site guide. Rendering changes
  need representative output inspection, including notes, examples, signatures, navigation,
  and links; a warning-free build alone does not establish correct presentation. Static CI
  review follows its no-execution rules and reports missing evidence without claiming it
  ran tests or inspected rendered pages.
