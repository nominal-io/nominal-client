#!/usr/bin/env python3
"""One-shot migration scan for a nominal bindings bump.

Resolves versions, installs both versions of the bindings into temp dirs (via uv),
extracts each API surface, diffs them, scans the client for references, and writes a
markdown report.

  uv run --no-project python bump_scan.py conjure --repo .          # pinned (pyproject) -> latest on PyPI
  uv run --no-project python bump_scan.py proto   --repo . --old 0.1282.0 --new 0.1286.0
  uv run --no-project python bump_scan.py both    --repo . --out ./reports

transport: conjure (nominal-api) · proto (nominal-api-protos) · both
Versions default to: old = pin in <repo>/pyproject.toml, new = latest on PyPI.

Run via `uv run --no-project python …`: `--no-project` makes uv ignore the project, so the developer's
`.venv` is never synced or modified (plain `uv run` would sync it to the lockfile first). Bindings are
fetched with `uv pip install --target` into a temp dir with the interpreter pinned and run outside the
project, so `.venv` is untouched regardless; the version lookup uses the stdlib PyPI JSON API. Bare
`python …` works too (the scripts are stdlib-only); only `uv` itself need be on PATH.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

from diff_surface import build_report
from extract_surface import extract, use_utf8_stdout

PKG = {"conjure": "nominal-api", "proto": "nominal-api-protos"}
PYPI_JSON = "https://pypi.org/pypi/{pkg}/json"


def pinned_version(pkg: str, pyproject: Path) -> str | None:
    if not pyproject.exists():
        return None
    m = re.search(rf'"{re.escape(pkg)}==([^"]+)"', pyproject.read_text(encoding="utf-8"))
    return m.group(1) if m else None


def ensure_uv() -> None:
    if subprocess.run(["uv", "--version"], capture_output=True, text=True, check=False).returncode != 0:
        sys.exit("`uv` is required to fetch the bindings but was not found on PATH (see https://docs.astral.sh/uv/).")


def latest_version(pkg: str) -> str | None:
    try:
        with urllib.request.urlopen(PYPI_JSON.format(pkg=pkg), timeout=30) as resp:
            return json.load(resp)["info"]["version"]
    except Exception:
        return None


def install_surface(pkg: str, ver: str, dest: Path) -> Path:
    """Install the package unpacked into dest; returns the dir holding the package tree.

    Isolated from the developer's project environment: `--target` installs into the temp dir,
    `--python` pins the running interpreter, and the subprocess runs from dest (no project
    context), so the repo's `.venv` is never read, synced, or modified.
    """
    dest.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["uv", "pip", "install", "--target", str(dest), "--no-deps", "--python", sys.executable, f"{pkg}=={ver}"],
        check=True, capture_output=True, text=True, cwd=str(dest),
    )
    return dest


def scan_one(transport: str, old: str | None, new: str | None, repo: str, pyproject: Path, outdir: Path) -> None:
    pkg = PKG[transport]
    old = old or pinned_version(pkg, pyproject)
    new = new or latest_version(pkg)
    if not old or not new:
        sys.exit(f"could not resolve versions for {pkg} (old={old}, new={new}); pass --old/--new")
    if old == new:
        print(f"# {pkg}: pinned version {old} is already the latest — nothing to compare.")
        return

    with tempfile.TemporaryDirectory(prefix=f"bump-{transport}-") as tmp:  # installs are large; clean up after
        work = Path(tmp)
        old_surface = extract(transport, install_surface(pkg, old, work / "old"))
        new_surface = extract(transport, install_surface(pkg, new, work / "new"))
        report = build_report(old_surface, new_surface, label=f"{pkg} {old} -> {new}", repo=repo)

    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / f"migration-report-{transport}-{old}-to-{new}.md"
    out.write_text(report, encoding="utf-8")
    sys.stdout.write(report)
    sys.stdout.write(f"\n[report written to {out}]\n")


def main() -> None:
    use_utf8_stdout()
    ensure_uv()
    ap = argparse.ArgumentParser()
    ap.add_argument("transport", choices=["conjure", "proto", "both"])
    ap.add_argument("--old", default=None, help="old version (default: pin in pyproject.toml)")
    ap.add_argument("--new", default=None, help="new version (default: latest on PyPI)")
    ap.add_argument("--repo", default=".", help="client repo root to scan for references")
    ap.add_argument("--pyproject", default=None, help="path to pyproject.toml (default: <repo>/pyproject.toml)")
    ap.add_argument("--out", default=".", help="directory to write the report(s) to")
    args = ap.parse_args()

    pyproject = Path(args.pyproject) if args.pyproject else Path(args.repo) / "pyproject.toml"
    transports = ["conjure", "proto"] if args.transport == "both" else [args.transport]
    for t in transports:
        scan_one(t, args.old, args.new, args.repo, pyproject, Path(args.out))


if __name__ == "__main__":
    main()
