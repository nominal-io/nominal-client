"""Checks for the example-gallery directive."""

from pathlib import Path

NOTEBOOK = """\
---
kernelspec:
  display_name: Python 3
  language: python
  name: python3
{icon}---

# {title}
"""


def test_gallery_lists_notebooks_beside_the_index(tmp_path, build_docs):
    """Notebooks get a card; other pages in the folder do not. The root toctree lists them all."""
    docs = tmp_path / "docs"
    source = docs / "src"
    examples = source / "examples"
    examples.mkdir(parents=True)
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (docs / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {str(extensions)!r})\n"
        "extensions = ['myst_nb', 'sphinx_design', 'examples']\n"
        "myst_enable_extensions = ['colon_fence']\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text(
        "Home\n====\n\n.. toctree::\n   :glob:\n\n   examples/index\n   examples/[!i]*\n", encoding="utf-8"
    )
    (examples / "index.md").write_text("# Examples\n\n```{example-gallery}\n```\n", encoding="utf-8")
    (examples / "running.md").write_text("# Running the examples\n", encoding="utf-8")
    (examples / "upload.md").write_text(NOTEBOOK.format(icon="icon: pulse\n", title="Upload a dataset"), "utf-8")
    (examples / "plain.md").write_text(NOTEBOOK.format(icon="", title="Plain card"), "utf-8")

    result = build_docs()
    assert result.returncode == 0, result.stdout + result.stderr
    html = (docs / "_build/dirhtml/examples/index.html").read_text(encoding="utf-8")
    assert html.count("sd-card ") == 2
    assert "Upload a dataset" in html and "Plain card" in html
    assert html.count("sd-octicon ") == 1
    assert 'href="upload/"' in html and 'href="plain/"' in html
