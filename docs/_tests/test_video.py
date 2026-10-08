"""Build-level checks for local recordings and remote video URLs."""

import subprocess
import sys
from pathlib import Path

import pytest


def _build(tmp_path: Path, url: str, *, loop: bool = False) -> tuple[subprocess.CompletedProcess[str], Path]:
    source = tmp_path / "source"
    (source / "guides/nested").mkdir(parents=True)
    extensions = Path(__file__).resolve().parents[1] / "_ext"
    (source / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {str(extensions)!r})\nextensions = ['myst_parser', 'video']\n",
        encoding="utf-8",
    )
    (source / "index.rst").write_text("Home\n====\n\n.. toctree::\n\n   guides/nested/recording\n", encoding="utf-8")
    (source / "guides/nested/recording.md").write_text(
        f'# Recording\n\n```{{video}} {url}\n:alt: Video <example> "quoted"\n' + (":loop:\n" if loop else "") + "```\n",
        encoding="utf-8",
    )
    output = tmp_path / "output"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-W", "-b", "dirhtml", str(source), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result, output


@pytest.mark.parametrize("loop", [False, True])
def test_local_recording_is_copied_and_linked_from_nested_page(tmp_path: Path, loop: bool) -> None:
    """Nested guide pages point to the copied recording with the requested playback mode."""
    media = tmp_path / "source/media/recording.mp4"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"recording fixture")
    result, output = _build(tmp_path, "/media/recording.mp4", loop=loop)
    assert result.returncode == 0, result.stdout + result.stderr
    copied = next((output / "_downloads").rglob("recording.mp4"))
    assert copied.read_bytes() == media.read_bytes()
    html = (output / "guides/nested/recording/index.html").read_text(encoding="utf-8")
    assert f"../../../{copied.relative_to(output).as_posix()}" in html
    assert ("autoplay loop muted playsinline" if loop else "controls") in html
    assert 'aria-label="Video &lt;example&gt; &quot;quoted&quot;"' in html


def test_remote_recording_is_not_copied(tmp_path: Path) -> None:
    """Remote video URLs render directly without creating a local download."""
    url = "https://example.com/recording.mp4"
    result, output = _build(tmp_path, url)
    assert result.returncode == 0, result.stdout + result.stderr
    html = (output / "guides/nested/recording/index.html").read_text(encoding="utf-8")
    assert f'src="{url}"' in html
    assert not list((output / "_downloads").rglob("*.mp4"))


def test_missing_recording_fails_build_without_broken_player(tmp_path: Path) -> None:
    """An unreadable recording fails the build and produces no player with a broken source."""
    result, output = _build(tmp_path, "/media/missing.mp4")
    assert result.returncode != 0
    assert "Extension error" not in result.stderr
    html = (output / "guides/nested/recording/index.html").read_text(encoding="utf-8")
    assert "<video " not in html
