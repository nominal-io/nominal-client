"""Checks for generated examples before the content migration lands."""

import importlib.util
import subprocess
import sys
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
    source.mkdir(parents=True)
    module.generate(SimpleNamespace(srcdir=source, confdir=source.parent))
    text = (source / "examples/index.md").read_text(encoding="utf-8")
    assert "No example scripts have been added yet." in text
    assert "github.com/nominal-io/nominal-client/tree/main/examples" not in text


@pytest.mark.parametrize("script", ["upload.py", "index.py", "nested/index.py"])
def test_examples_build_with_separate_source_and_config_directories(tmp_path, script):
    """Every script gets a distinct page with source links for the configured revision."""
    docs = tmp_path / "docs"
    source = docs / "src"
    source.mkdir(parents=True)
    scripts = tmp_path / "examples"
    scripts.mkdir()
    (scripts / script).parent.mkdir(parents=True, exist_ok=True)
    (scripts / script).write_text('"""Example: Upload a dataset."""\nprint("upload fixture")\n', encoding="utf-8")
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (docs / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {str(extensions)!r})\n"
        "extensions = ['myst_parser', 'sphinx.ext.autosummary', 'examples', 'api_reference']\n"
        "autosummary_generate = False\n"
        "html_context = {'source_user': 'nominal-io', 'source_repo': 'nominal-client', "
        "'source_version': 'release/fixture'}\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("Home\n====\n\n.. toctree::\n\n   examples/index\n", encoding="utf-8")
    obsolete = source / "examples/obsolete.md"
    obsolete.parent.mkdir(parents=True)
    obsolete.write_text("# Old generated example\n", encoding="utf-8")
    output = tmp_path / "output"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-b", "dirhtml", "-c", str(docs), str(source), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
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
