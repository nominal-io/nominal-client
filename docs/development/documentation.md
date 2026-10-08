# Docstrings and rendered documentation

These conventions apply to new and changed code. Read the affected implementation and
callers alongside this guide; examples do not require unrelated code to be normalized.

- Public APIs need concise Google-style docstrings describing caller-visible behavior.
  Include `Args`, `Returns`, and `Raises` where they add information; simple properties or
  obvious accessors do not need boilerplate sections. Annotations own type declarations.
  Ruff intentionally disables several missing-docstring rules in `pyproject.toml`; a clean
  lint result does not establish that a new public API has adequate documentation.
- Document consequential defaults, `None`/empty distinctions, units and time zones,
  in-place mutation, completion semantics, and actionable failure conditions. Examples
  must match the actual API. Do not narrate request-building internals as user documentation.
- Preserve notes and caveats, including those inside argument descriptions. Keep a caveat
  next to the parameter it qualifies and retain a distinct paragraph or supported admonition.
  Do not flatten useful notes just to appease a generator.
- Follow the renderer configured on the branch and any scoped docs instructions. Google
  section indentation matters; Markdown versus reStructuredText inline/code-block syntax
  is renderer-dependent. Do not mechanically normalize single/double backticks or migrate
  every docstring to fix a local rendering problem. Confirm how the parser handles the
  construct before alleging a defect.
- Prefer native documentation-generator features and package exports over custom parsers,
  duplicated API catalogs, or manually synchronized member lists. A workaround must solve
  a demonstrated limitation with a bounded scope. Rendering changes need representative
  output inspection, including notes, signatures, navigation, and links; a warning-free build
  alone does not establish correct presentation.
