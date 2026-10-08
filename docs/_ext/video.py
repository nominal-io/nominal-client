"""``{video}`` directive: an HTML5 video from a file in the docs or a URL.

Local files are copied into the build like ``{download}`` targets, so pages can
show screen recordings without raw HTML. ``:loop:`` plays it muted on a loop,
like a GIF; otherwise it shows the player controls.
"""

import posixpath
from html import escape

from docutils import nodes
from docutils.parsers.rst import directives
from sphinx.application import Sphinx
from sphinx.util.docutils import SphinxDirective


class video(nodes.General, nodes.Element):
    pass


class VideoDirective(SphinxDirective):
    required_arguments = 1
    option_spec = {"loop": directives.flag, "alt": directives.unchanged}

    def run(self) -> list[nodes.Node]:
        src = self.arguments[0]
        node = video(loop="loop" in self.options, alt=self.options.get("alt", ""))
        if "://" in src:
            node["uri"] = src
        else:
            rel, filename = self.env.relfn2path(src, self.env.docname)
            self.env.note_dependency(rel)
            node["download"] = self.env.dlfiles.add_file(self.env.docname, rel)
        return [node]


def visit_video_html(self, node: video) -> None:
    uri = node.get("uri") or posixpath.join(self.builder.dlpath, node["download"])
    attrs = "autoplay loop muted playsinline" if node["loop"] else "controls"
    label = f' aria-label="{escape(node["alt"])}"' if node["alt"] else ""
    self.body.append(f'<video class="docs-video" {attrs} preload="metadata"{label} src="{escape(uri)}"></video>\n')
    raise nodes.SkipNode


def setup(app: Sphinx) -> dict[str, bool]:
    app.add_node(video, html=(visit_video_html, None))
    app.add_directive("video", VideoDirective)
    return {"parallel_read_safe": True}
