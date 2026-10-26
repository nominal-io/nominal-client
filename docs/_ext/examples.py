"""The ``example-gallery`` directive: a card grid and hidden toctree for the notebooks beside the page.

A notebook is a MyST Markdown file with ``kernelspec`` in its front matter. Its card shows the
first ``#`` heading as the title and the optional front-matter ``icon`` as an octicon.
"""

from pathlib import Path
from typing import Any

import yaml
from docutils import nodes
from docutils.statemachine import StringList
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.util.docutils import SphinxDirective

GALLERY = """\
::::{{grid}} 1 1 2 2
:gutter: 2

{cards}
::::

```{{toctree}}
:hidden:

{toc}
```
"""

CARD = """\
:::{{grid-item-card}} {label}
:link: {name}
:link-type: doc
:::
"""


def _notebook_card(path: Path) -> tuple[str, str] | None:
    """Return (title, icon) for a MyST notebook, or None for any other page."""
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return None
    front, _, body = text[4:].partition("\n---\n")
    meta = yaml.safe_load(front) or {}
    if "kernelspec" not in meta:
        return None
    title = next((line[2:].strip() for line in body.splitlines() if line.startswith("# ")), path.stem)
    return title, meta.get("icon", "")


class ExampleGallery(SphinxDirective):
    def run(self) -> list[nodes.Node]:
        folder = Path(self.env.doc2path(self.env.docname)).parent
        cards, toc = [], []
        for path in sorted(folder.glob("*.md")):
            card = _notebook_card(path)
            if card is None:
                continue
            self.env.note_dependency(path)
            title, icon = card
            label = f"{{octicon}}`{icon}` {title}" if icon else title
            cards.append(CARD.format(label=label, name=path.stem))
            toc.append(path.stem)
        self.env.example_galleries = getattr(self.env, "example_galleries", set()) | {self.env.docname}

        markup = GALLERY.format(cards="\n".join(cards), toc="\n".join(toc))
        container = nodes.Element()
        source = self.get_source_info()[0]
        self.state.nested_parse(StringList(markup.splitlines(), source), self.content_offset, container)
        return container.children


def _purge(app: Sphinx, env: BuildEnvironment, docname: str) -> None:
    getattr(env, "example_galleries", set()).discard(docname)


def _merge(app: Sphinx, env: BuildEnvironment, docnames: set[str], other: BuildEnvironment) -> None:
    env.example_galleries = getattr(env, "example_galleries", set()) | getattr(other, "example_galleries", set())


def _outdated(app: Sphinx, env: BuildEnvironment, added: set[str], changed: set[str], removed: set[str]) -> list[str]:
    """Rebuild a gallery when a page is added to or removed from its folder."""
    folders = {docname.rpartition("/")[0] for docname in added | removed}
    return [doc for doc in getattr(env, "example_galleries", set()) if doc.rpartition("/")[0] in folders]


def setup(app: Sphinx) -> dict[str, Any]:
    app.add_directive("example-gallery", ExampleGallery)
    app.connect("env-purge-doc", _purge)
    app.connect("env-merge-info", _merge)
    app.connect("env-get-outdated", _outdated)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
