"""Keep triple-backtick docstring examples literal before Napoleon parses Google sections."""

import re

from sphinx.application import Sphinx

_CODE_FENCE = re.compile(
    r"^(?P<indent> *)```(?P<language>[\w.+-]*)[ \t]*\n"
    r"(?P<code>(?:(?P=indent)[^\n]*\n|[ \t]*\n)*?)^(?P=indent)```[ \t]*$",
    re.MULTILINE,
)


def _expand_code_fence(match: re.Match[str]) -> str:
    body = "\n".join("    " + line if line.strip() else "" for line in match["code"].splitlines())
    return f"{match['indent']}.. code-block:: {match['language'] or 'text'}\n\n{body}\n"


def _process_code_fences(app: Sphinx, what: str, name: str, obj: object, options: object, lines: list[str]) -> None:
    lines[:] = _CODE_FENCE.sub(_expand_code_fence, "\n".join(lines)).split("\n")


def _process_cli_code_fences(app: Sphinx, ctx: object, lines: list[str]) -> None:
    lines[:] = _CODE_FENCE.sub(_expand_code_fence, "\n".join(lines)).split("\n")


def setup(app: Sphinx) -> dict[str, bool]:
    app.setup_extension("sphinx_click")
    app.connect("autodoc-process-docstring", _process_code_fences, priority=400)  # before Napoleon
    app.connect("sphinx-click-process-description", _process_cli_code_fences)
    return {"parallel_read_safe": True}
