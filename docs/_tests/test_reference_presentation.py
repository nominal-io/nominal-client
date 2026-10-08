"""Resource signatures show public dataclass fields without losing their documentation."""

import pickle
import subprocess
import sys
from pathlib import Path

from docutils import nodes
from sphinx import addnodes


def test_dataclass_signatures_follow_field_visibility(tmp_path: Path) -> None:
    """Hide repr-disabled constructor fields, resolve annotations, and preserve authored prose."""
    docs = tmp_path / "docs"
    docs.mkdir()
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (docs / "conf.py").write_text(
        f"import sys\nsys.path[:0] = [{str(tmp_path)!r}, {str(extensions)!r}]\n"
        "extensions = ['sphinx.ext.autodoc', 'reference_presentation']\n"
        "autodoc_default_options = {'members': True, 'undoc-members': True}\n",
        encoding="utf-8",
    )
    (docs / "index.rst").write_text(
        "Resources\n=========\n\n.. automodule:: fixture_api\n\n.. autodata:: fixture_api.Alias\n", encoding="utf-8"
    )
    (tmp_path / "fixture_api.py").write_text(
        '''from __future__ import annotations
from dataclasses import dataclass, field

@dataclass
class Resource:
    rid: str
    label: str | None = None
    #: :meta private:
    hidden: str = field(default="internal", repr=False)

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

    env = pickle.loads((output / ".doctrees/environment.pickle").read_bytes())
    contents = {node.get("anchorname") for node in env.tocs["index"].findall(nodes.reference)}
    assert "#fixture_api.Resource" in contents
    assert "#fixture_api.Resource.read" in contents
    assert "#fixture_api.Resource.rid" not in contents
    assert "#fixture_api.Alias" not in contents
    assert "fixture_api.Alias" in signatures
