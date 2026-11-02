from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import IO

RESOURCES = Path(__file__).parent / "resources"
PACKAGES = [
    "numpy",
    "pandas",
    "scipy",
    "matplotlib",
    "plotly",
    "pyarrow",
    "openpyxl",
    "jupyterlab",
    "ipykernel",
    "nbconvert",
]


def run(
    args: Sequence[str | Path], *, capture_output: bool = False, stdout: IO[str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run([str(x) for x in args], check=True, text=True, capture_output=capture_output, stdout=stdout)


def host() -> str:
    key = (sys.platform, platform.machine().lower())
    mapping = {
        ("win32", "amd64"): "win-64",
        ("linux", "x86_64"): "linux-64",
        ("linux", "aarch64"): "linux-aarch64",
        ("darwin", "arm64"): "osx-arm64",
        ("darwin", "x86_64"): "osx-64",
    }
    if key not in mapping:
        raise ValueError(f"Unsupported build host: {key}")
    return mapping[key]


def python_at(prefix: Path) -> Path:
    return prefix / ("python.exe" if sys.platform == "win32" else "bin/python")


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def sdk_version(requested: str | None = None) -> str:
    with urllib.request.urlopen("https://pypi.org/pypi/nominal/json", timeout=60) as response:
        data = json.load(response)
    version = requested or data["info"]["version"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("Only stable major.minor.patch SDK versions are supported")
    if not data["releases"].get(version):
        raise ValueError(f"Nominal {version} is not published on PyPI")
    return version


def build(args: argparse.Namespace) -> None:
    import conda_pack

    if args.revision < 1:
        raise ValueError("Revision must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    version = sdk_version(args.sdk_version)
    name = f"nominal-{version}-{host()}-py{args.python}-r{args.revision}"
    destination = output / name
    if destination.exists():
        raise ValueError(f"Refusing to overwrite {destination}")
    conda = args.conda or os.environ.get("CONDA_EXE") or shutil.which("conda")
    if not conda:
        raise ValueError("Conda is required on the build host; pass --conda or set CONDA_EXE")
    with tempfile.TemporaryDirectory(prefix="nominal-build-") as temp:
        prefix = Path(temp) / "environment"
        run(
            [
                conda,
                "create",
                "--yes",
                "--copy",
                "--no-default-packages",
                "--override-channels",
                "--channel",
                args.channel,
                "--prefix",
                prefix,
                f"python={args.python}",
            ]
        )
        python = python_at(prefix)
        run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                python,
                "--refresh",
                "--no-build",
                "--index-url",
                "https://pypi.org/simple",
                f"nominal=={version}",
                *PACKAGES,
            ]
        )
        run(["uv", "pip", "check", "--python", python])
        destination.mkdir()
        try:
            conda_inventory = []
            for metadata_file in sorted((prefix / "conda-meta").glob("*.json")):
                metadata = json.loads(metadata_file.read_text())
                item = {
                    key: metadata.get(key)
                    for key in ("name", "version", "build", "subdir", "channel", "license", "license_family")
                }
                notices = {}
                cache = Path(metadata.get("link", {}).get("source", prefix / "nonexistent"))
                license_dir = cache / "info" / "licenses"
                if license_dir.is_dir():
                    for notice in license_dir.rglob("*"):
                        if notice.is_file():
                            notices[str(notice.relative_to(license_dir))] = notice.read_text(errors="replace")
                item["notices"] = notices
                conda_inventory.append(item)
            (destination / "conda-packages.json").write_text(json.dumps(conda_inventory, indent=2))
            inventory = run([python, RESOURCES / "inventory.py"], capture_output=True).stdout
            (destination / "packages.json").write_text(inventory)
            with (destination / "conda-explicit.txt").open("w") as stream:
                run([conda, "list", "--prefix", prefix, "--explicit"], stdout=stream)
            try:
                commit = run(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip()
            except subprocess.CalledProcessError:
                commit = "unavailable"
            manifest = {
                "sdk_version": version,
                "python": args.python,
                "platform": host(),
                "built_at": datetime.now(timezone.utc).isoformat(),
                "commit": commit,
                "conda_channel": args.channel,
                "python_index": "https://pypi.org/simple",
                "revision": args.revision,
                "validation": "not tested",
            }
            (destination / "manifest.json").write_text(json.dumps(manifest, indent=2))
            for resource in RESOURCES.iterdir():
                if resource.is_file():
                    shutil.copy2(resource, destination / resource.name)
            # No profiles, API keys, or test-only packages are installed before packing.
            conda_pack.pack(
                prefix=str(prefix), output=str(destination / "environment.tar.gz"), format="tar.gz", verbose=True
            )
            files = sorted(p for p in destination.iterdir() if p.is_file())
            (destination / "SHA256SUMS").write_text("".join(f"{digest(p)}  {p.name}\n" for p in files))
        except BaseException:
            shutil.rmtree(destination)
            raise
    print(destination)


def verify(args: argparse.Namespace) -> None:
    bundle = args.bundle.resolve()
    # The installer is standalone, runs from read-only media, and verifies every payload hash.
    run([sys.executable, bundle / "install.py", "--bundle", bundle, "--destination", args.destination.resolve()])


def ci(args: argparse.Namespace) -> None:
    request_id = f"bundle-{time.time_ns()}"
    run(
        [
            "gh",
            "workflow",
            "run",
            "conda-pack.yml",
            "--repo",
            args.repo,
            "--ref",
            args.ref,
            "-f",
            f"platform={args.platform}",
            "-f",
            f"python={args.python}",
            "-f",
            f"request_id={request_id}",
            "-f",
            f"sdk_version={args.sdk_version or ''}",
        ]
    )
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        result = run(
            [
                "gh",
                "run",
                "list",
                "--repo",
                args.repo,
                "--workflow",
                "conda-pack.yml",
                "--json",
                "databaseId,displayTitle",
                "--limit",
                "100",
            ],
            capture_output=True,
        )
        matches = [r for r in json.loads(result.stdout) if r["displayTitle"] == request_id]
        if matches:
            run_id = str(matches[0]["databaseId"])
            run(["gh", "run", "watch", run_id, "--repo", args.repo, "--exit-status"])
            run(["gh", "run", "download", run_id, "--repo", args.repo, "--dir", args.output.resolve()])
            return
        time.sleep(5)
    raise ValueError(f"Dispatch sent but run not found; inspect Actions for {request_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build")
    b.add_argument("--python", default="3.12", choices=[f"3.{i}" for i in range(11, 16)])
    b.add_argument("--sdk-version")
    b.add_argument("--revision", type=int, default=1)
    b.add_argument("--conda")
    b.add_argument("--channel", default="conda-forge")
    b.add_argument("--output", type=Path, default=Path("dist/conda-pack"))
    b.set_defaults(func=build)
    v = sub.add_parser("verify")
    v.add_argument("bundle", type=Path)
    v.add_argument("--destination", type=Path, required=True)
    v.set_defaults(func=verify)
    c = sub.add_parser("ci")
    c.add_argument("--repo", default="nominal-io/nominal-client")
    c.add_argument("--ref", default="main")
    c.add_argument("--platform", choices=["win-64", "linux-64", "osx-arm64", "osx-64"], default="win-64")
    c.add_argument("--python", default="3.12", choices=[f"3.{i}" for i in range(11, 16)])
    c.add_argument("--sdk-version")
    c.add_argument("--output", type=Path, default=Path("dist/ci"))
    c.set_defaults(func=ci)
    args = parser.parse_args()
    try:
        args.func(args)
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"Error: {exc}\n")
