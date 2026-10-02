"""Read the Markdown that nominal's docstrings use, ahead of napoleon.

The docstrings were written for mkdocstrings (Markdown) and render on IDE hover as
Markdown, so they keep it: ```lang fences become ``code-block`` directives and
``[text](url)`` links become reST links. Everything else (Google sections,
backticked names) napoleon and ``default_role`` already handle.
"""

import re

from sphinx.application import Sphinx

_FENCE = re.compile(r"^(?P<indent>\s*)```(?P<lang>[\w+-]*)\s*$")
_LINK = re.compile(r"(?<!`)\[(?P<text>[^\]\n]+)\]\((?P<url>https?://[^)\s]+)\)")


def _convert(lines: list[str]) -> list[str]:
    out: list[str] = []
    fence: str | None = None  # indent of the open fence
    for line in lines:
        match = _FENCE.match(line)
        if fence is None and match:
            fence = match["indent"]
            out += [f"{fence}.. code-block:: {match['lang'] or 'text'}", ""]
        elif fence is not None and match and match["indent"] == fence:
            fence = None
            out.append("")
        elif fence is not None:
            # dedent to the fence, then indent under the directive
            body = line[len(fence) :] if line.startswith(fence) else line.lstrip()
            out.append(f"{fence}    {body}" if body.strip() else "")
        else:
            out.append(_LINK.sub(r"`\g<text> <\g<url>>`__", line))
    return out


def _process(app: Sphinx, what: str, name: str, obj: object, options: object, lines: list[str]) -> None:
    lines[:] = _convert(lines)


def setup(app: Sphinx) -> dict[str, bool]:
    # before napoleon (default priority 500), so it sees reST
    app.connect("autodoc-process-docstring", _process, priority=400)
    return {"parallel_read_safe": True}
