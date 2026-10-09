"""Resource signatures show public dataclass fields without losing their documentation."""

import pickle
import subprocess
import sys
from pathlib import Path

from docutils import nodes
from sphinx import addnodes


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

@dataclass
class Resource(_ResourceBase):
    label: str | None = None
    state: str = field(default="ready", init=False)
    #: :meta private:
    hidden: str = field(default="internal", repr=False)

    @classmethod
    def create(cls) -> Resource:
        """Create a resource."""
        return cls("rid")

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
    assert "rid: str" in signatures["fixture_api.Resource"]
    assert "label: str | None = None" in signatures["fixture_api.Resource"]
    assert "fixture_api.Resource.rid" in signatures
    assert "fixture_api.Resource.hidden" not in signatures
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
