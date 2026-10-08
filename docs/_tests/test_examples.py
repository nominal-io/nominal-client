"""Checks for generated examples before the content migration lands."""

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace


def test_empty_examples_index_has_no_broken_source_link(tmp_path):
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


def test_examples_build_with_separate_source_and_config_directories(tmp_path):
    docs = tmp_path / "docs"
    source = docs / "src"
    source.mkdir(parents=True)
    scripts = tmp_path / "examples"
    scripts.mkdir()
    (scripts / "upload.py").write_text('"""Example: Upload a dataset."""\nprint("upload fixture")\n', encoding="utf-8")
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (docs / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {str(extensions)!r})\n"
        "extensions = ['myst_parser', 'examples']\n"
        "html_context = {'source_user': 'nominal-io', 'source_repo': 'nominal-client', 'source_version': 'main'}\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("Home\n====\n\n.. toctree::\n\n   examples/index\n", encoding="utf-8")
    output = tmp_path / "output"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-b", "dirhtml", "-c", str(docs), str(source), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (source / "examples/upload.md").is_file()
    html = (output / "examples/upload/index.html").read_text(encoding="utf-8")
    assert "Upload a dataset" in html
    assert "upload fixture" in html
