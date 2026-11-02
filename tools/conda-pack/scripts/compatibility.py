"""Probe runtime and binary dependency availability, not execution compatibility."""

import argparse
import bz2
import json
import subprocess
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

TARGETS = {
    "windows-x64": ("win-64", "x86_64-pc-windows-msvc"),
    "macos-aarch64": ("osx-arm64", "aarch64-apple-darwin"),
    "linux-x86_64": ("linux-64", "x86_64-manylinux_2_28"),
    "linux-aarch64": ("linux-aarch64", "aarch64-manylinux_2_28"),
    "linux-armv7": (None, None),
    "musllinux-x86_64": (None, None),
    "musllinux-aarch64": (None, None),
    "musllinux-armv7": (None, None),
}
PYTHONS = [f"3.{minor}" for minor in range(11, 16)]
PACKAGES = "nominal\nnumpy\npandas\nscipy\nmatplotlib\nplotly\npyarrow\nopenpyxl\njupyterlab\nipykernel\nnbconvert\n"


def runtime_versions(subdir):
    url = f"https://conda.anaconda.org/conda-forge/{subdir}/current_repodata.json.bz2"
    with urllib.request.urlopen(url, timeout=120) as response:
        data = json.loads(bz2.decompress(response.read()))
    return {
        p["version"].rsplit(".", 1)[0]
        for group in ("packages", "packages.conda")
        for p in data.get(group, {}).values()
        if p["name"] == "python"
    }


def probe(item):
    label, python, runtime, target = item
    row = {
        "platform": label,
        "python": python,
        "runtime": runtime,
        "binary_resolution": "not tested",
        "offline_execution": "not tested",
        "staging_e2e": "not tested",
    }
    if target is None or runtime != "available":
        return row
    with tempfile.TemporaryDirectory() as temp:
        temp = Path(temp)
        requirements = temp / "requirements.in"
        requirements.write_text(PACKAGES)
        result = subprocess.run(
            [
                "uv",
                "pip",
                "compile",
                str(requirements),
                "--refresh",
                "--no-build",
                "--python-version",
                python,
                "--python-platform",
                target,
                "--index-url",
                "https://pypi.org/simple",
                "--output-file",
                str(temp / "resolved.txt"),
            ],
            capture_output=True,
            text=True,
        )
        row["binary_resolution"] = "passed" if result.returncode == 0 else "failed"
        row["detail"] = result.stderr[-5000:]
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    runtime = {}
    for subdir, _ in TARGETS.values():
        if subdir and subdir not in runtime:
            try:
                runtime[subdir] = runtime_versions(subdir)
            except Exception as error:
                runtime[subdir] = f"not tested: {error}"
    items = []
    for label, (subdir, target) in TARGETS.items():
        for python in PYTHONS:
            versions = runtime.get(subdir)
            state = (
                "unavailable"
                if subdir is None
                else ("available" if python in versions else "unavailable")
                if isinstance(versions, set)
                else versions
            )
            items.append((label, python, state, target))
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(probe, items))
    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "source": "nominal-streaming/.github/workflows/wheels.yml",
        "streaming_commit": "f1153fa26970dd4f0a7476fa085b33cc02a0ff1d",
        "note": "Availability probe only. current_repodata is not a full historical channel inventory. musl and armv7 have no supported Conda runtime mapping in this prototype. Python 3.15 is experimental.",
        "rows": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2))
    print(args.output)


if __name__ == "__main__":
    main()
