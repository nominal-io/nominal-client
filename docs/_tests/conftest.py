"""Build small sites with the same cleanup contract as build-docs and serve-docs."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def build_docs(tmp_path: Path):
    docs = tmp_path / "docs"
    source = docs / "src"
    (source / "reference").mkdir(parents=True)
    justfile = Path(__file__).resolve().parents[2] / "justfile"

    def build(builder: str = "dirhtml") -> subprocess.CompletedProcess[str]:
        output = docs / "_build" / builder
        subprocess.run(
            ["just", "--justfile", str(justfile), "--working-directory", str(tmp_path), "_clean-docs"],
            capture_output=True,
            text=True,
            check=True,
        )
        return subprocess.run(
            [sys.executable, "-m", "sphinx", "-W", "-E", "-b", builder, "-c", str(docs), str(source), str(output)],
            capture_output=True,
            text=True,
            check=False,
        )

    return build
