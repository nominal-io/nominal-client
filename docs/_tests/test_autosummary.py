"""Build-level checks for API discovery and removal of stale autosummary stubs."""

import subprocess
import sys
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[1]


def _build(source: Path, output: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-E", "-b", "dirhtml", str(source), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("section", ["experimental", "thirdparty"])
def test_removed_member_does_not_leave_nested_autosummary_stubs(tmp_path: Path, section: str) -> None:
    source = tmp_path / "source"
    page_dir = source / "reference" / section
    page_dir.mkdir(parents=True)
    (source / "conf.py").write_text(
        "import runpy\nimport sys\n"
        f"sys.path.insert(0, {str(tmp_path)!r})\n"
        "extensions = ['sphinx.ext.autodoc', 'sphinx.ext.autosummary']\n"
        f"templates_path = [{str(DOCS / '_templates')!r}]\n"
        "autosummary_generate = True\n"
        f"def setup(app):\n    runpy.run_path({str(DOCS / 'conf.py')!r})['setup'](app)\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text(f"Home\n====\n\n.. toctree::\n\n   reference/{section}/api\n", encoding="utf-8")
    (page_dir / "api.rst").write_text(
        "API\n===\n\n.. autosummary::\n   :toctree: generated\n\n   fixture_api.Foo\n",
        encoding="utf-8",
    )
    module = tmp_path / "fixture_api.py"
    module.write_text(
        'class Foo:\n    """A class."""\n    def method(self):\n        """A method."""\n        pass\n',
        encoding="utf-8",
    )
    output = tmp_path / "output"
    first = _build(source, output)
    assert first.returncode == 0, first.stdout + first.stderr
    stale_stub = page_dir / "generated/fixture_api.Foo.method.rst"
    assert stale_stub.is_file()

    module.write_text('class Foo:\n    """A class with no methods now."""\n    pass\n', encoding="utf-8")
    second = _build(source, output)
    assert second.returncode == 0, second.stdout + second.stderr
    assert not stale_stub.exists()
    assert (page_dir / "generated/fixture_api.Foo.rst").is_file()


def test_module_catalog_tracks_public_reexports(tmp_path: Path) -> None:
    source = tmp_path / "source"
    reference = source / "reference"
    reference.mkdir(parents=True)
    (source / "conf.py").write_text(
        "import runpy\nimport sys\n"
        f"sys.path.insert(0, {str(tmp_path)!r})\n"
        f"config = runpy.run_path({str(DOCS / 'conf.py')!r})\n"
        "extensions = ['sphinx.ext.autodoc', 'sphinx.ext.autosummary']\n"
        f"templates_path = [{str(DOCS / '_templates')!r}]\n"
        "autosummary_generate = True\n"
        "autosummary_ignore_module_all = config.get('autosummary_ignore_module_all', True)\n"
        "def setup(app):\n    config['setup'](app)\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("Home\n====\n\n.. toctree::\n\n   reference/api\n", encoding="utf-8")
    (reference / "api.rst").write_text(
        "API\n===\n\n.. autosummary::\n   :toctree: generated\n\n   fixture_public\n", encoding="utf-8"
    )
    (tmp_path / "fixture_impl.py").write_text(
        'class Exported:\n    """A public reexport."""\n\nclass Added:\n    """Another public reexport."""\n',
        encoding="utf-8",
    )
    module = tmp_path / "fixture_public.py"
    module.write_text(
        "from fixture_impl import Added, Exported\n"
        "__all__ = ['Exported']\n\n"
        'class Internal:\n    """Not part of the public API."""\n',
        encoding="utf-8",
    )
    output = tmp_path / "output"
    first = _build(source, output)
    assert first.returncode == 0, first.stdout + first.stderr
    generated = reference / "generated"
    assert (generated / "fixture_public.Exported.rst").is_file()
    api_html = (output / "reference/api/index.html").read_text(encoding="utf-8")
    assert 'href="../generated/fixture_public/#module-fixture_public"' in api_html
    assert not (generated / "fixture_public.Internal.rst").exists()
    assert not (generated / "fixture_public.Added.rst").exists()

    # Only the Python export list changes; the docs page stays the same.
    module.write_text(module.read_text().replace("['Exported']", "['Added']"), encoding="utf-8")
    second = _build(source, output)
    assert second.returncode == 0, second.stdout + second.stderr
    assert (generated / "fixture_public.Added.rst").is_file()
    assert not (generated / "fixture_public.Exported.rst").exists()
    html = (output / "reference/generated/fixture_public/index.html").read_text(encoding="utf-8")
    assert "fixture_public.Added/" in html
