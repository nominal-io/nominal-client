"""Resource signatures show public dataclass fields without losing their documentation."""

import pickle
import subprocess
import sys
from pathlib import Path

import pytest
from docutils import nodes
from sphinx import addnodes
from sphinx.util.inventory import InventoryFile


def test_dataclass_reference_preserves_public_fields_and_member_order(tmp_path: Path) -> None:
    """Keep inherited and non-init fields in declaration order ahead of properties and alphabetical methods."""
    docs = tmp_path / "docs"
    docs.mkdir()
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (docs / "conf.py").write_text(
        f"import sys\nsys.path[:0] = [{str(tmp_path)!r}, {str(extensions)!r}]\n"
        "extensions = ['sphinx.ext.autodoc', 'reference_presentation']\n"
        "autodoc_default_options = {'members': True, 'undoc-members': True, 'inherited-members': 'object'}\n",
        encoding="utf-8",
    )
    (docs / "index.rst").write_text(
        "Resources\n=========\n\n.. automodule:: fixture_api\n\n.. autodata:: fixture_api.Alias\n", encoding="utf-8"
    )
    (tmp_path / "fixture_api.py").write_text(
        '''from __future__ import annotations
from dataclasses import dataclass, field

@dataclass
class _ResourceBase:
    rid: str
    secret: str = field(repr=False)

@dataclass
class Resource(_ResourceBase):
    label: str | None = None
    state: str = field(default="ready", init=False)
    hidden: str = field(default="internal", repr=False)

    @classmethod
    def create(cls) -> Resource:
        """Create a resource."""
        return cls("rid", "internal")

    def archive(self) -> None:
        """Archive the resource."""

    @property
    def name(self) -> str:
        """The resource name."""
        return self.label or self.rid

    def read(self) -> str:
        """Read the resource."""
        return self.rid

@dataclass
class Authored:
    """Useful caller-facing documentation."""
    value: int

@dataclass(init=False)
class Custom:
    value: int
    def __init__(self, value: int, *, normalize: bool = True):
        """Construct with a custom option."""
        self.value = value

Alias = str

@dataclass
class Visible:
    hidden: str = "caller-visible"

class Container:
    @dataclass
    class Nested:
        value: int
        hidden: int = field(repr=False)
''',
        encoding="utf-8",
    )
    output = tmp_path / "html"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-E", "-b", "html", str(docs), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    doctree = pickle.loads((output / ".doctrees/index.doctree").read_bytes())
    signatures = {node["ids"][0]: node.astext() for node in doctree.findall(addnodes.desc_signature) if node["ids"]}
    assert "hidden" not in signatures["fixture_api.Resource"]
    assert "secret" not in signatures["fixture_api.Resource"]
    assert "rid: str" in signatures["fixture_api.Resource"]
    assert "label: str | None = None" in signatures["fixture_api.Resource"]
    assert "fixture_api.Resource.rid" in signatures
    assert "hidden" not in signatures["fixture_api.Container.Nested"]
    assert "fixture_api.Container.Nested.value" in signatures
    assert "fixture_api.Visible.hidden" in signatures
    hidden_targets = [
        "fixture_api.Resource.hidden",
        "fixture_api.Resource.secret",
        "fixture_api.Container.Nested.hidden",
    ]
    inventory = InventoryFile.loads((output / "objects.inv").read_bytes(), uri="").data
    index = (output / "genindex.html").read_text(encoding="utf-8")
    for target in hidden_targets:
        assert target not in signatures
        assert target not in inventory["py:attribute"]
        assert target not in index
    assert "fixture_api.Visible.hidden" in inventory["py:attribute"]
    assert "normalize: bool = True" in signatures["fixture_api.Custom"]
    assert "Useful caller-facing documentation." in doctree.astext()

    resource = next(node for node in doctree.findall(addnodes.desc) if node[0].get("ids") == ["fixture_api.Resource"])
    members = [node[0]["ids"][0] for node in resource[-1] if isinstance(node, addnodes.desc)]
    assert members == [
        "fixture_api.Resource.rid",
        "fixture_api.Resource.label",
        "fixture_api.Resource.state",
        "fixture_api.Resource.name",
        "fixture_api.Resource.archive",
        "fixture_api.Resource.create",
        "fixture_api.Resource.read",
    ]

    env = pickle.loads((output / ".doctrees/environment.pickle").read_bytes())
    contents = {node.get("anchorname") for node in env.tocs["index"].findall(nodes.reference)}
    assert "#fixture_api.Resource" in contents
    assert "#fixture_api.Resource.read" in contents
    assert "#fixture_api.Resource.rid" not in contents
    assert "#fixture_api.Alias" not in contents
    assert "fixture_api.Alias" in signatures


@pytest.mark.parametrize("module", [None, "fixture_api", "not_installed"])
def test_handwritten_class_references_need_no_importable_object(tmp_path: Path, module: str | None) -> None:
    """Preserve authored class references with no module, a missing class, or an unavailable module."""
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (tmp_path / "conf.py").write_text(
        f"import sys\nsys.path[:0] = [{str(tmp_path)!r}, {str(extensions)!r}]\n"
        "extensions = ['sphinx.ext.autodoc', 'reference_presentation']\n",
        encoding="utf-8",
    )
    (tmp_path / "fixture_api.py").write_text("", encoding="utf-8")
    module_directive = f".. py:module:: {module}\n\n" if module else ""
    (tmp_path / "index.rst").write_text(
        "Types\n=====\n\n" + module_directive + ".. py:class:: Example(value)\n\n"
        "   An authored class reference.\n\n"
        "   .. py:attribute:: value\n\n"
        "      The input value.\n\n"
        "   .. py:method:: read()\n\n"
        "      Read the value.\n",
        encoding="utf-8",
    )
    output = tmp_path / "html"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-E", "-b", "html", str(tmp_path), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    html = (output / "index.html").read_text(encoding="utf-8")
    assert "An authored class reference." in html
    assert "The input value." in html
    assert "Read the value." in html
