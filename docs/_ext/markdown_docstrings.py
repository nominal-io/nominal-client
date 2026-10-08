"""Adapt legacy Markdown fences and HTTP links before Napoleon parses docstrings.

This deliberately supports only the two forms used by existing SDK examples.
New docstrings use Google sections and reStructuredText inline markup and literal
blocks, which Sphinx and Napoleon parse directly.
"""

import re

from pygments.lexers import TextLexer
from sphinx.application import Sphinx

_FENCE = re.compile(r"^(?P<indent>\s*)```(?P<lang>[\w+-]*)\s*$")
_LINK = re.compile(
    r"(?P<code>(?P<ticks>`+).*?(?P=ticks))|"
    r"\[(?P<text>[^\]\n]+)\]\((?P<url>https?://[^)\s]+)\)"
)


def _convert_link(match: re.Match[str]) -> str:
    return match["code"] or f"`{match['text']} <{match['url']}>`__"


def _convert(lines: list[str]) -> list[str]:
    out: list[str] = []
    fence: str | None = None  # indent of the open fence
    for line in lines:
        match = _FENCE.match(line)
        if fence is None and match:
            fence = match["indent"]
            out += [f"{fence}.. code-block:: {match['lang'] or 'text'}", ""]
        elif fence is not None and match and not match["lang"]:
            fence = None
            out.append("")
        elif fence is not None:
            # dedent to the fence, then indent under the directive
            body = line[len(fence) :] if line.startswith(fence) else line.lstrip()
            out.append(f"{fence}    {body}" if body.strip() else "")
        else:
            out.append(_LINK.sub(_convert_link, line))
    return out


def _process(app: Sphinx, what: str, name: str, obj: object, options: object, lines: list[str]) -> None:
    lines[:] = _convert(lines)


def setup(app: Sphinx) -> dict[str, bool]:
    # CSV examples are data, with no Pygments CSV lexer.
    app.add_lexer("csv", TextLexer)
    # before napoleon (default priority 500), so it sees reST
    app.connect("autodoc-process-docstring", _process, priority=400)
    return {"parallel_read_safe": True}
