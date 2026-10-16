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
    """Keep compact reference tables, linked type definitions, and existing object anchors."""
    docs = tmp_path / "docs"
    source = docs / "src"
    templates = Path(__file__).resolve().parents[1] / "_templates"
    (tmp_path / "_sample_impl.py").write_text(
        '''from enum import Enum
from typing import Literal, TypeAlias

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

Label: TypeAlias = Literal['fast', 'safe']
ResourceOrLabel: TypeAlias = Resource | Label
RESOURCE_TYPE: TypeAlias = Resource | None

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
        "from _sample_impl import Resource, Mode, create, MAX_BATCH, Label, ResourceOrLabel, RESOURCE_TYPE, _Internal\n"
        "__all__ = ['Resource', 'Mode', 'create', 'MAX_BATCH', 'Label', 'ResourceOrLabel', 'RESOURCE_TYPE', 'T']\n",
        encoding="utf-8",
    )
    (tmp_path / "sample_local.py").write_text(
        'from _sample_impl import MAX_BATCH\n\nMAX_RETRIES = 3\n"""Maximum retry attempts."""\n',
        encoding="utf-8",
    )
    (docs / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {str(tmp_path)!r})\n"
        "extensions = ['myst_parser', 'sphinx.ext.autosummary']\n"
        "from sphinx.util.typing import restify\nimport sample_api\n"
        f"templates_path = [{str(templates)!r}]\n"
        f"autosummary_generate = [{str(docs / 'autosummary.rst')!r}]\n"
        "autosummary_ignore_module_all = False\n"
        "autosummary_context = {'module_data': {'sample_api': {'enums': ['Mode'], 'exports': sample_api.__all__, "
        "'constants': {'MAX_BATCH': '``10``'}, 'type_aliases': {\n"
        "name: restify(getattr(sample_api, name), mode='smart').replace(\"'\", \"\\\\'\")\n"
        "for name in ['Label', 'ResourceOrLabel', 'RESOURCE_TYPE']}},\n"
        "'sample_local': {'enums': [], 'exports': [], 'type_aliases': {},\n"
        "'constants': {'MAX_BATCH': '``10``', 'MAX_RETRIES': '``3``'}}}}\n"
        "autodoc_default_options = {'members': True}\n"
        "exclude_patterns = ['sdk/_summaries']\n",
        encoding="utf-8",
    )
    (docs / "autosummary.rst").write_text(
        ".. autosummary::\n   :toctree: src/sdk/_summaries\n"
        "   :template: autosummary/module-summary.rst\n\n   sample_api\n   sample_local\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("SDK\n===\n\n.. toctree::\n\n   sdk/experimental/sample\n", encoding="utf-8")
    page = source / "sdk/experimental/sample.md"
    page.parent.mkdir(parents=True)
    page.write_text(
        "# Sample API\n\nAn authored walkthrough.\n\n## API reference\n\n"
        "```{eval-rst}\n.. include:: ../_summaries/sample_api.rst\n\n"
        ".. include:: ../_summaries/sample_local.rst\n```\n",
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
        {"Label", "ResourceOrLabel", "RESOURCE_TYPE"},
        {"MAX_RETRIES"},
    ]
    assert [rubric.astext() for rubric in tree.findall(nodes.rubric)] == [
        "Classes",
        "Enumerations",
        "Functions",
        "Constants",
        "Type aliases",
        "Constants",
    ]
    assert list(tables[3].findall(nodes.row))[0][1].astext() == "10"
    aliases = {row[0].astext().strip(): row[1] for row in tables[4].findall(nodes.row)}
    assert aliases["Label"].astext() == "Literal['fast', 'safe']"
    assert aliases["ResourceOrLabel"].astext() == "Resource | Literal['fast', 'safe']"
    assert aliases["RESOURCE_TYPE"].astext() == "Resource | None"
    html = (output / "sdk/experimental/sample/index.html").read_text()
    for name in ["ResourceOrLabel", "RESOURCE_TYPE"]:
        row_html = html.split(f'id="sample_api.{name}"', 1)[1].split("</tr>", 1)[0]
        assert 'href="../../generated/sample_api.Resource/#sample_api.Resource"' in row_html
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
    for name in aliases:
        entry = inventory["py:data"][f"sample_api.{name}"]
        assert entry.uri == f"sdk/experimental/sample/#sample_api.{name}"
        assert not (output / f"sdk/generated/sample_api.{name}/index.html").exists()
    assert inventory["py:method"]["sample_api.Resource.read"].uri.startswith("sdk/generated/sample_api.Resource/")
    assert not (output / "sdk/_summaries/sample_api/index.html").exists()
    assert "sample_local.MAX_BATCH" not in inventory.get("py:data", {})
    assert "sample_local.MAX_RETRIES" in inventory["py:data"]
    assert "sample_api.T" not in inventory.get("py:data", {})
    assert "sample_api._Internal" not in inventory["py:class"]
    assert "sample_api.Path" not in inventory["py:class"]
