"""``{video}`` directive: an HTML5 video from a file in the docs or a URL.

Local files are copied into the build like ``{download}`` targets, so pages can
show screen recordings without raw HTML. ``:loop:`` plays it muted on a loop,
like a GIF; otherwise it shows the player controls.
"""

import posixpath
from html import escape

from docutils import nodes
from docutils.parsers.rst import directives
from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.util.docutils import SphinxDirective


class video(addnodes.download_reference):
    pass


class VideoDirective(SphinxDirective):
    required_arguments = 1
    option_spec = {"loop": directives.flag, "alt": directives.unchanged}

    def run(self) -> list[nodes.Node]:
        node = video(
            reftarget=self.arguments[0],
            loop="loop" in self.options,
            alt=self.options.get("alt", ""),
        )
        self.set_source_info(node)
        return [node]


def visit_video_html(self, node: video) -> None:
    if "refuri" in node:
        uri = node["refuri"]
    elif "filename" in node:
        uri = posixpath.join(self.builder.dlpath, node["filename"])
    else:
        # The native download collector already warned about an unreadable file.
        raise nodes.SkipNode
    attrs = "autoplay loop muted playsinline" if node["loop"] else "controls"
    label = f' aria-label="{escape(node["alt"])}"' if node["alt"] else ""
    self.body.append(f'<video class="docs-video" {attrs} preload="metadata"{label} src="{escape(uri)}"></video>\n')
    raise nodes.SkipNode


def setup(app: Sphinx) -> dict[str, bool]:
    app.add_node(video, html=(visit_video_html, None))
    app.add_directive("video", VideoDirective)
    return {"parallel_read_safe": True}
