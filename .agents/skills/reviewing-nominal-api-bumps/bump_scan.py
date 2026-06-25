#!/usr/bin/env python3
"""One-shot migration scan for a nominal bindings bump.

Resolves versions, downloads + unpacks both wheels, extracts each API surface,
diffs them, scans the client for references, and writes a markdown report.

  python bump_scan.py conjure --repo .                  # pinned (pyproject) -> latest on PyPI
  python bump_scan.py proto   --repo . --old 0.1282.0 --new 0.1286.0
  python bump_scan.py both    --repo . --out ./reports  # both transports

transport: conjure (nominal-api) · proto (nominal-api-protos) · both
Versions default to: old = pin in <repo>/pyproject.toml, new = latest on PyPI.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

HERE = Path(__file__).parent
PKG = {"conjure": "nominal-api", "proto": "nominal-api-protos"}


def run(cmd: list[str]) -> str:
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if res.returncode:
        sys.exit(f"command failed: {' '.join(cmd)}\n{res.stderr}")
    return res.stdout


def pinned_version(pkg: str, pyproject: Path) -> str | None:
    if not pyproject.exists():
        return None
    m = re.search(rf'"{re.escape(pkg)}==([^"]+)"', pyproject.read_text(encoding="utf-8"))
    return m.group(1) if m else None


def latest_version(pkg: str) -> str | None:
    out = run([sys.executable, "-m", "pip", "index", "versions", pkg])
    m = re.search(r"Available versions:\s*([0-9][^\s,]*)", out)
    return m.group(1) if m else None


def fetch_and_unpack(pkg: str, ver: str, dest: Path) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    run([sys.executable, "-m", "pip", "download", f"{pkg}=={ver}", "--no-deps", "-d", str(dest)])
    whl = next(dest.glob("*.whl"))
    root = dest / "unpacked"
    with zipfile.ZipFile(whl) as z:
        z.extractall(root)
    return root


def scan_one(transport: str, old: str | None, new: str | None, repo: str, pyproject: Path, outdir: Path) -> Path:
    pkg = PKG[transport]
    old = old or pinned_version(pkg, pyproject)
    new = new or latest_version(pkg)
    if not old or not new:
        sys.exit(f"could not resolve versions for {pkg} (old={old}, new={new}); pass --old/--new")
    if old == new:
        print(f"# {pkg}: pinned version {old} is already the latest — nothing to compare.")
        return outdir / f"migration-report-{transport}-noop.md"

    work = Path(tempfile.mkdtemp(prefix=f"bump-{transport}-"))
    old_root = fetch_and_unpack(pkg, old, work / "old")
    new_root = fetch_and_unpack(pkg, new, work / "new")
    (work / "old.txt").write_text(run([sys.executable, str(HERE / "extract_surface.py"), transport, str(old_root)]), encoding="utf-8")
    (work / "new.txt").write_text(run([sys.executable, str(HERE / "extract_surface.py"), transport, str(new_root)]), encoding="utf-8")

    cmd = [sys.executable, str(HERE / "diff_surface.py"), str(work / "old.txt"), str(work / "new.txt"), "--label", f"{pkg} {old} -> {new}"]
    if repo:
        cmd += ["--repo", repo]
    report = run(cmd)

    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / f"migration-report-{transport}-{old}-to-{new}.md"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n[report written to {out}]\n")
    return out


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # reports contain ✅/⚠️/→ on any platform
    except Exception:
        pass
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
