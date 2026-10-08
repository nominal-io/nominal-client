"""Checks for generated examples before the content migration lands."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_empty_examples_index_has_no_broken_source_link(tmp_path):
    """An empty Examples section does not link to a nonexistent source folder."""
    extension = Path(__file__).resolve().parents[1] / "_ext/examples.py"
    spec = importlib.util.spec_from_file_location("docs_examples", extension)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source = tmp_path / "docs/src"
    source.mkdir(parents=True, exist_ok=True)
    module.generate(SimpleNamespace(srcdir=source, confdir=source.parent))
    text = (source / "examples/index.md").read_text(encoding="utf-8")
    assert "No example scripts have been added yet." in text
    assert "github.com/nominal-io/nominal-client/tree/main/examples" not in text


@pytest.mark.parametrize("script", ["upload.py", "index.py", "nested/index.py"])
def test_examples_build_with_separate_source_and_config_directories(tmp_path, script, build_docs):
    """Every script gets a distinct page with source links for the configured revision."""
    docs = tmp_path / "docs"
    source = docs / "src"
    source.mkdir(parents=True, exist_ok=True)
    scripts = tmp_path / "examples"
    scripts.mkdir()
    (scripts / script).parent.mkdir(parents=True, exist_ok=True)
    (scripts / script).write_text('"""Example: Upload a dataset."""\nprint("upload fixture")\n', encoding="utf-8")
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (docs / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {str(extensions)!r})\n"
        "extensions = ['myst_parser', 'sphinx.ext.autosummary', 'examples', 'api_reference']\n"
        "autosummary_generate = True\n"
        "html_context = {'source_user': 'nominal-io', 'source_repo': 'nominal-client', "
        "'source_version': 'release/fixture'}\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("Home\n====\n\n.. toctree::\n\n   examples/index\n", encoding="utf-8")
    obsolete = source / "examples/obsolete.md"
    obsolete.parent.mkdir(parents=True)
    obsolete.write_text("# Old generated example\n", encoding="utf-8")
    output = docs / "_build/dirhtml"
    result = build_docs()
    assert result.returncode == 0, result.stdout + result.stderr
    page = source / f"examples/{script}.md"
    assert page.is_file()
    assert not obsolete.exists()
    assert "# Examples" in (source / "examples/index.md").read_text(encoding="utf-8")
    assert f"/blob/release/fixture/examples/{script}" in page.read_text(encoding="utf-8")
    assert "/tree/release/fixture/examples" in (source / "examples/index.md").read_text(encoding="utf-8")
    html = (output / f"examples/{script}/index.html").read_text(encoding="utf-8")
    assert "Upload a dataset" in html
    assert "upload fixture" in html

    # Removing a script must remove its output too on the next clean rebuild.
    (scripts / script).unlink()
    result = build_docs()
    assert result.returncode == 0, result.stdout + result.stderr
    assert not page.exists()
    assert not (output / f"examples/{script}/index.html").exists()


def test_example_edit_links_are_independent_between_apps(tmp_path):
    """Building another site cannot erase the first site's example source links."""
    extension = Path(__file__).resolve().parents[1] / "_ext/examples.py"
    spec = importlib.util.spec_from_file_location("docs_examples", extension)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    apps = []
    for name in ("first", "second"):
        root = tmp_path / name
        source = root / "docs/src"
        source.mkdir(parents=True, exist_ok=True)
        apps.append(
            SimpleNamespace(
                srcdir=source,
                confdir=source.parent,
                config=SimpleNamespace(
                    html_context={"source_user": "nominal-io", "source_repo": name, "source_version": "main"}
                ),
            )
        )
    scripts = tmp_path / "first/examples"
    scripts.mkdir()
    (scripts / "upload.py").write_text('"""Upload example."""\n', encoding="utf-8")
    for app in apps:
        module.generate(app)
    context = {}
    module._edit_link(apps[0], "examples/upload.py", "page.html", context, None)
    assert context["edit_source_link"]("ignored") == "https://github.com/nominal-io/first/blob/main/examples/upload.py"
