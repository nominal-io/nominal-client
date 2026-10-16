"""Generated API tables keep authored prose and public re-exports discoverable."""

import pickle
from collections.abc import Callable
from pathlib import Path
from subprocess import CompletedProcess

from docutils import nodes
from sphinx.util.inventory import InventoryFile


def test_reference_tables_discover_exports_and_link_to_detail_pages(
    tmp_path: Path, build_docs: Callable[[], CompletedProcess[str]]
) -> None:
    """Keep enums and constant values in compact tables while preserving reference links and authored prose."""
    docs = tmp_path / "docs"
    source = docs / "src"
    templates = Path(__file__).resolve().parents[1] / "_templates"
    (tmp_path / "_sample_impl.py").write_text(
        '''from enum import Enum

class Mode(Enum):
    """Available modes."""
    FAST = 'fast'

class Resource:
    """A public resource."""
    def read(self) -> str:
        """Read the resource."""
        return "ready"

class _Internal:
    """An implementation detail."""

def create() -> Resource:
    """Create a resource."""
    return Resource()

MAX_BATCH = 10
"""Maximum items per batch."""
''',
        encoding="utf-8",
    )
    (tmp_path / "sample_api.py").write_text(
        '"""Public resource module."""\n'
        "from pathlib import Path\n"
        "from typing import TypeVar\n"
        "T = TypeVar('T')\n"
        "from _sample_impl import Resource, Mode, create, MAX_BATCH, _Internal\n"
        "__all__ = ['Resource', 'Mode', 'create', 'MAX_BATCH', 'T']\n",
        encoding="utf-8",
    )
    (docs / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {str(tmp_path)!r})\n"
        "extensions = ['myst_parser', 'sphinx.ext.autosummary']\n"
        f"templates_path = [{str(templates)!r}]\n"
        f"autosummary_generate = [{str(docs / 'autosummary.rst')!r}]\n"
        "autosummary_ignore_module_all = False\n"
        "autosummary_context = {'module_data': {'sample_api': {'enums': ['Mode'], "
        "'constants': {'MAX_BATCH': '10'}}}}\n"
        "autodoc_default_options = {'members': True}\n"
        "exclude_patterns = ['sdk/_summaries']\n",
        encoding="utf-8",
    )
    (docs / "autosummary.rst").write_text(
        ".. autosummary::\n   :toctree: src/sdk/_summaries\n"
        "   :template: autosummary/module-summary.rst\n\n   sample_api\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("SDK\n===\n\n.. toctree::\n\n   sdk/experimental/sample\n", encoding="utf-8")
    page = source / "sdk/experimental/sample.md"
    page.parent.mkdir(parents=True)
    page.write_text(
        "# Sample API\n\nAn authored walkthrough.\n\n## API reference\n\n"
        "```{eval-rst}\n.. include:: ../_summaries/sample_api.rst\n```\n",
        encoding="utf-8",
    )

    result = build_docs()
    assert result.returncode == 0, result.stdout + result.stderr
    output = docs / "_build/dirhtml"
    tree = pickle.loads((output / ".doctrees/sdk/experimental/sample.doctree").read_bytes())
    tables = list(tree.findall(nodes.table))
    assert [{row[0].astext().strip() for row in table.findall(nodes.row)} for table in tables] == [
        {"Resource"},
        {"Mode"},
        {"create"},
        {"MAX_BATCH"},
    ]
    assert [rubric.astext() for rubric in tree.findall(nodes.rubric)] == [
        "Classes",
        "Enumerations",
        "Functions",
        "Constants",
    ]
    assert list(tables[-1].findall(nodes.row))[0][1].astext() == "10"
    assert "int([x])" not in tree.astext()
    assert "An authored walkthrough." in tree.astext()
    assert "Public resource module." in tree.astext()
    inventory = InventoryFile.loads((output / "objects.inv").read_bytes(), uri="").data
    for kind, name in [("class", "Resource"), ("class", "Mode"), ("function", "create")]:
        entry = inventory[f"py:{kind}"][f"sample_api.{name}"]
        assert entry.uri.startswith(f"sdk/generated/sample_api.{name}/")
        assert (output / f"sdk/generated/sample_api.{name}/index.html").is_file()
    assert inventory["py:data"]["sample_api.MAX_BATCH"].uri.startswith("sdk/experimental/sample/")
    assert not (output / "sdk/generated/sample_api.MAX_BATCH/index.html").exists()
    assert inventory["py:method"]["sample_api.Resource.read"].uri.startswith("sdk/generated/sample_api.Resource/")
    assert not (output / "sdk/_summaries/sample_api/index.html").exists()
    assert "sample_api.T" not in inventory.get("py:data", {})
    assert "sample_api._Internal" not in inventory["py:class"]
    assert "sample_api.Path" not in inventory["py:class"]
