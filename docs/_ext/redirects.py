"""Write redirect pages at the old MkDocs site's URLs, so bookmarks to them still land.

``redirects`` in conf.py maps an old page (``reference/core``) to a docname of this site
(``sdk/core``) or to an absolute URL. Each old page becomes ``<old>/index.html`` (the MkDocs
site used folder-style URLs) holding a meta refresh and a canonical link.
"""

from html import escape
from pathlib import Path
from typing import Any

from sphinx.application import Sphinx
from sphinx.util.osutil import relative_uri

_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Moved</title>
<meta name="robots" content="noindex">
<link rel="canonical" href="{url}">
<meta http-equiv="refresh" content="0; url={url}">
</head>
<body><p>This page has moved to <a href="{url}">{url}</a>.</p></body>
</html>
"""


def _on_build_finished(app: Sphinx, exception: Exception | None) -> None:
    if exception or app.builder.format != "html":
        return
    for old, new in app.config.redirects.items():
        source = f"{old.strip('/')}/"
        if "://" in new:
            url = new
        else:
            if new not in app.env.all_docs:
                raise ValueError(f"redirects: {old!r} points at {new!r}, which is not a page of this site")
            url = relative_uri(source, app.builder.get_target_uri(new))
        path = Path(app.outdir, source, "index.html")
        if path.exists():
            raise ValueError(f"redirects: {old!r} would overwrite a page of this site")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_PAGE.format(url=escape(url)), encoding="utf-8")


def setup(app: Sphinx) -> dict[str, Any]:
    app.add_config_value("redirects", {}, "html", types=[dict])
    app.connect("build-finished", _on_build_finished)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
