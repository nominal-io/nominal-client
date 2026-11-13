"""Offline installer. Run with an existing Anaconda Python; needs only the standard library."""

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tarfile
from pathlib import Path


def check(bundle: Path) -> None:
    entries = {}
    for line in (bundle / "SHA256SUMS").read_text().splitlines():
        expected, name = line.split("  ", 1)
        if Path(name).name != name or name in entries:
            raise ValueError("Invalid checksum entry")
        entries[name] = expected
    required = {
        "environment.tar.gz",
        "manifest.json",
        "smoke.py",
        "example.ipynb",
        "inventory.py",
        "install.py",
        "launch.py",
    }
    if not required.issubset(entries):
        raise ValueError("Incomplete checksum manifest")
    for name, expected in entries.items():
        with (bundle / name).open("rb") as stream:
            actual = (
                hashlib.file_digest(stream, "sha256").hexdigest()
                if hasattr(hashlib, "file_digest")
                else hashlib.sha256(stream.read()).hexdigest()
            )
        if actual != expected:
            raise ValueError(f"Checksum mismatch: {name}")


def extract(archive: Path, destination: Path) -> None:
    try:
        # The standard-library data filter rejects escaping links, traversal, and special files.
        shutil.unpack_archive(archive, destination, format="gztar", filter="data")
    except tarfile.FilterError as exc:
        raise ValueError(f"Unsafe archive member: {exc}") from exc


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bundle", type=Path, default=Path(__file__).resolve().parent)
    p.add_argument("--destination", required=True, type=Path)
    a = p.parse_args()
    bundle, dest = a.bundle.resolve(), a.destination.resolve()
    check(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text())
    mapping = {
        ("Windows", "amd64"): "win-64",
        ("Darwin", "arm64"): "osx-arm64",
        ("Darwin", "x86_64"): "osx-64",
        ("Linux", "x86_64"): "linux-64",
        ("Linux", "aarch64"): "linux-aarch64",
    }
    if mapping.get((platform.system(), platform.machine().lower())) != manifest["platform"]:
        raise ValueError("Bundle OS/architecture does not match this machine")
    if dest.exists() or dest.is_relative_to(bundle):
        raise ValueError("Choose a new writable destination outside the bundle source")
    dest.mkdir(parents=True)
    environment = dest / "environment"
    environment.mkdir()
    extract(bundle / "environment.tar.gz", environment)
    python = environment / ("python.exe" if os.name == "nt" else "bin/python")
    unpack = environment / ("Scripts/conda-unpack-script.py" if os.name == "nt" else "bin/conda-unpack")
    writable = dest / "work"
    writable.mkdir()
    env = os.environ.copy()
    env.update(
        {"PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1", "CONDA_PREFIX": str(environment), "UV_OFFLINE": "1"}
    )
    env.pop("PYTHONPATH", None)
    paths = [environment, environment / "Scripts", environment / "Library/bin", environment / "bin"]
    env["PATH"] = os.pathsep.join(map(str, paths)) + os.pathsep + env.get("PATH", "")
    for key in (
        "TEMP",
        "TMP",
        "TMPDIR",
        "MPLCONFIGDIR",
        "JUPYTER_CONFIG_DIR",
        "JUPYTER_DATA_DIR",
        "JUPYTER_RUNTIME_DIR",
        "IPYTHONDIR",
        "XDG_CACHE_HOME",
    ):
        path = writable / key.lower()
        path.mkdir()
        env[key] = str(path)
    kernel = writable / "jupyter_data_dir" / "kernels" / "nominal-offline"
    kernel.mkdir(parents=True)
    (kernel / "kernel.json").write_text(
        json.dumps(
            {
                "argv": [str(python), "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                "display_name": "Nominal offline",
                "language": "python",
            }
        )
    )
    subprocess.run([str(python), str(unpack)], env=env, cwd=writable, check=True)
    # Save launchers so later notebook sessions keep state off the source media.
    shutil.copy2(bundle / "example.ipynb", writable / "example.ipynb")
    local_env = {
        k: v
        for k, v in env.items()
        if k in ("PATH", "CONDA_PREFIX", "PYTHONNOUSERSITE", "PYTHONDONTWRITEBYTECODE", "UV_OFFLINE")
        or k
        in (
            "TEMP",
            "TMP",
            "TMPDIR",
            "MPLCONFIGDIR",
            "JUPYTER_CONFIG_DIR",
            "JUPYTER_DATA_DIR",
            "JUPYTER_RUNTIME_DIR",
            "IPYTHONDIR",
            "XDG_CACHE_HOME",
        )
    }
    (dest / "runtime-env.json").write_text(json.dumps(local_env, indent=2))
    shutil.copy2(bundle / "launch.py", dest / "launch.py")
    subprocess.run([str(python), str(bundle / "smoke.py"), str(writable)], env=env, cwd=writable, check=True)
    (dest / "verification.json").write_text(
        json.dumps(
            {
                "offline_checks": "passed",
                "network_blocking": "external enforcement required",
                "platform": manifest["platform"],
                "sdk_version": manifest["sdk_version"],
            },
            indent=2,
        )
    )
    print(f'Installed and verified: {dest}\nLaunch Jupyter: "{python}" "{dest / "launch.py"}"')


if __name__ == "__main__":
    main()
