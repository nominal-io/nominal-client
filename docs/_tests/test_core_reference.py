"""Split Core references preserve public coverage and existing links."""

import subprocess
import sys
from pathlib import Path

from sphinx.util.inventory import InventoryFile


def test_core_module_pages_keep_exports_and_legacy_links(tmp_path: Path) -> None:
    """Filter implementation helpers, resolve re-exports, and forward old member bookmarks."""
    package = tmp_path / "nominal/core"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        "from .resource import Resource, ResourceList, helper\n__all__ = ['Resource', 'ResourceList', 'helper']\n",
        encoding="utf-8",
    )
    (package / "resource.py").write_text(
        "class Resource:\n"
        "    def read(self) -> str:\n"
        '        """Read a resource."""\n'
        "        return 'data'\n\n"
        "ResourceList = list[Resource]\n"
        '"""Resources returned by a search."""\n\n'
        "def helper() -> None:\n"
        '    """A public helper."""\n\n'
        "def implementation_helper() -> None:\n"
        '    """Implementation detail."""\n',
        encoding="utf-8",
    )
    docs = tmp_path / "docs"
    module_docs = docs / "sdk/core"
    module_docs.mkdir(parents=True)
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (docs / "conf.py").write_text(
        f"import sys\nsys.path[:0] = [{str(tmp_path)!r}, {str(extensions)!r}]\n"
        "extensions = ['sphinx.ext.autodoc', 'core_reference']\n"
        "autodoc_default_options = {'members': True, 'undoc-members': True}\n",
        encoding="utf-8",
    )
    (docs / "index.rst").write_text(
        "SDK\n===\n\n.. toctree::\n\n   sdk/core\n\n"
        "See :py:class:`nominal.core.Resource` and :py:meth:`nominal.core.Resource.read`.\n",
        encoding="utf-8",
    )
    (docs / "sdk/core.rst").write_text("Core\n====\n\n.. toctree::\n\n   core/resource\n", encoding="utf-8")
    (module_docs / "resource.rst").write_text(
        "Resource\n========\n\n.. automodule:: nominal.core.resource\n", encoding="utf-8"
    )
    output = tmp_path / "html"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-n", "-E", "-j", "2", "-b", "dirhtml", str(docs), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    inventory = InventoryFile.loads((output / "objects.inv").read_bytes(), uri="").data
    assert inventory["py:class"]["nominal.core.Resource"].uri == "sdk/core/resource/#nominal.core.resource.Resource"
    assert (
        inventory["py:method"]["nominal.core.Resource.read"].uri
        == "sdk/core/resource/#nominal.core.resource.Resource.read"
    )
    assert "nominal.core.helper" in inventory["py:function"]
    assert "nominal.core.ResourceList" in inventory["py:data"]
    assert "nominal.core.resource.implementation_helper" not in inventory["py:function"]
    assert "Implementation detail." not in (output / "sdk/core/resource/index.html").read_text(encoding="utf-8")
    core_html = (output / "sdk/core/index.html").read_text(encoding="utf-8")
    assert '"resource/", "nominal.core.resource.Resource"' in core_html
    assert "location.replace" in core_html

    # Dropping a page must fail loudly instead of silently losing public API coverage.
    (module_docs / "resource.rst").write_text("Resource\n========\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-E", "-b", "dirhtml", str(docs), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "Public Core export nominal.core.Resource has no reference page" in result.stdout + result.stderr
