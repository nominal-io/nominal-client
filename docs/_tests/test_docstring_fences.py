"""Fenced examples remain literal inside Google sections and native notes."""

import pickle
import subprocess
import sys
from pathlib import Path

import pytest
from docutils import nodes


@pytest.fixture
def build_docs(tmp_path: Path):
    docs = tmp_path / "docs"
    source = docs / "src"
    (source / "reference").mkdir(parents=True)
    output = docs / "_build/dirhtml"

    def build() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "sphinx", "-W", "-E", "-b", "dirhtml", "-c", str(docs), str(source), str(output)],
            capture_output=True,
            text=True,
            check=False,
        )

    return build


def test_fences_preserve_code_and_argument_notes(tmp_path: Path, build_docs) -> None:
    """Code stays literal and highlighted without flattening nested argument notes."""
    source = tmp_path / "docs/src"
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (source.parent / "conf.py").write_text(
        f"import sys\nsys.path[:0] = [{str(tmp_path)!r}, {str(extensions)!r}]\n"
        "extensions = ['sphinx.ext.autodoc', 'sphinx.ext.napoleon', 'docstring_fences']\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("API\n===\n\n.. autofunction:: fixture_api.example\n", encoding="utf-8")
    (tmp_path / "fixture_api.py").write_text(
        '''def example(value: str, other: int) -> str:
    """Read ``value`` with :class:`str`.

    Args:
        value: Input value.

            .. note::

                Preserve the indentation and markup in code:

                ```python
                if value:
                    print("[label](https://example.com) and ``literal``")
                ```

                This stays inside the note.
        other: Other argument.

    Returns:
        Returned value.

    Example:
        ```matlab
        >> result = load("example.mat");
        ```
        ```text
        name,value
        example,1
        ```
        ```
        unhighlighted text
        ```

        After the examples.
    """
    return value
''',
        encoding="utf-8",
    )
    output = tmp_path / "docs/_build/dirhtml"
    result = build_docs()
    assert result.returncode == 0, result.stdout + result.stderr
    doctree = pickle.loads((output / ".doctrees/index.doctree").read_bytes())
    blocks = list(doctree.findall(nodes.literal_block))
    assert [(block["language"], block.astext()) for block in blocks] == [
        ("python", 'if value:\n    print("[label](https://example.com) and ``literal``")'),
        ("matlab", '>> result = load("example.mat");'),
        ("text", "name,value\nexample,1"),
        ("text", "unhighlighted text"),
    ]
    note = next(doctree.findall(nodes.note))
    assert blocks[0] in list(note.findall(nodes.literal_block))
    assert "This stays inside the note." in note.astext()
    html = (output / "index.html").read_text(encoding="utf-8")
    assert "Other argument." in html
    assert "Returned value." in html
    assert "After the examples." in html
    assert 'class="highlight-python' in html
    assert 'class="highlight-matlab' in html


@pytest.mark.parametrize("closing", ["", "    ```"])
def test_invalid_fences_fail_the_strict_build(tmp_path: Path, build_docs, closing: str) -> None:
    """Missing or misindented closing fences cannot silently publish as prose."""
    source = tmp_path / "docs/src"
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (source.parent / "conf.py").write_text(
        f"import sys\nsys.path[:0] = [{str(tmp_path)!r}, {str(extensions)!r}]\n"
        "extensions = ['sphinx.ext.autodoc', 'docstring_fences']\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("API\n===\n\n.. autofunction:: fixture_api.example\n", encoding="utf-8")
    (tmp_path / "fixture_api.py").write_text(
        f'def example():\n    """Example.\n\n```python\nprint("hello")\n{closing}\n    """\n', encoding="utf-8"
    )
    result = build_docs()
    assert result.returncode != 0
    assert "Inline literal start-string without end-string" in result.stderr


def test_cli_help_preserves_fenced_shell_examples(tmp_path: Path, build_docs) -> None:
    """Click help uses the same fenced-code convention as API docstrings."""
    source = tmp_path / "docs/src"
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (source.parent / "conf.py").write_text(
        f"import sys\nsys.path[:0] = [{str(tmp_path)!r}, {str(extensions)!r}]\n"
        "extensions = ['sphinx_click', 'docstring_fences']\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text(
        "CLI\n===\n\n.. click:: fixture_cli:example\n   :prog: example\n", encoding="utf-8"
    )
    (tmp_path / "fixture_cli.py").write_text(
        '''import click

@click.command()
def example():
    """Register an image.

    ```bash
    IMAGE_RID=$(example register --file image.tar)
    example activate --rid "$IMAGE_RID"
    ```
    """
''',
        encoding="utf-8",
    )
    result = build_docs()
    assert result.returncode == 0, result.stdout + result.stderr
    output = tmp_path / "docs/_build/dirhtml"
    doctree = pickle.loads((output / ".doctrees/index.doctree").read_bytes())
    examples = [block for block in doctree.findall(nodes.literal_block) if block["language"] == "bash"]
    assert len(examples) == 1
    assert examples[0].astext() == 'IMAGE_RID=$(example register --file image.tar)\nexample activate --rid "$IMAGE_RID"'
