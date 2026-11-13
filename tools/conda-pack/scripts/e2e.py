"""Run matching upstream tests without installing its checkout or altering the packed environment."""

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

root = Path(os.environ["BUNDLE_ROOT"])
source = Path(os.environ["BUNDLE_SOURCE"])
manifest = json.loads((source / "manifest.json").read_text())
python = root / "environment" / ("python.exe" if os.name == "nt" else "bin/python")
with tempfile.TemporaryDirectory(prefix="nominal-e2e-") as tmp:
    tmp = Path(tmp)
    checkout = tmp / "sdk"
    subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            f"v{manifest['sdk_version']}",
            "https://github.com/nominal-io/nominal-client.git",
            str(checkout),
        ],
        check=True,
    )
    tests = tmp / "suite"
    tests.mkdir()
    shutil.copytree(checkout / "tests", tests / "tests")
    constraints = tmp / "constraints.txt"
    inventory = json.loads((source / "packages.json").read_text())
    constraints.write_text("\n".join(f"{p['name']}=={p['version']}" for p in inventory))
    extra = tmp / "test-dependencies"
    resolved = tmp / "resolved-test-dependencies.txt"
    subprocess.run(
        [
            "uv",
            "--no-config",
            "pip",
            "compile",
            "--python",
            str(python),
            "--constraint",
            str(constraints),
            "--no-header",
            "--no-annotate",
            "--output-file",
            str(resolved),
            "-",
        ],
        input="pytest\npytest-cov\npytest-timeout\n",
        text=True,
        check=True,
    )
    bundled = {p["name"].lower().replace("_", "-") for p in inventory}
    missing = [
        line
        for line in resolved.read_text().splitlines()
        if "==" in line and line.split("==")[0].lower().replace("_", "-") not in bundled
    ]
    missing_file = tmp / "missing-test-dependencies.txt"
    missing_file.write_text("\n".join(missing))
    subprocess.run(
        [
            "uv",
            "--no-config",
            "pip",
            "install",
            "--python",
            str(python),
            "--target",
            str(extra),
            "--no-deps",
            "--requirement",
            str(missing_file),
        ],
        check=True,
    )
    # PYTHONPATH must not shadow any bundled distribution.
    bundled = {p["name"].lower().replace("_", "-") for p in inventory}
    import importlib.metadata

    duplicates = {
        d.metadata["Name"].lower().replace("_", "-") for d in importlib.metadata.distributions(path=[str(extra)])
    } & bundled
    if duplicates:
        raise RuntimeError(f"Test dependencies would shadow bundled packages: {duplicates}")
    env = os.environ.copy()
    env.update(json.loads((root / "runtime-env.json").read_text()))
    env["PYTHONPATH"] = str(extra)
    subprocess.run(
        [
            str(python),
            "-c",
            "from nominal.cli import nom; nom()",
            "config",
            "profile",
            "add",
            "bundle-e2e",
            "--base-url",
            "https://api-staging.gov.nominal.io/api",
            "--workspace-rid",
            os.environ["NOMINAL_E2E_WORKSPACE_RID"],
            "--token",
            os.environ["NOMINAL_E2E_TOKEN"],
        ],
        env=env,
        cwd=tmp,
        check=True,
    )
    subprocess.run(
        [
            str(python),
            "-m",
            "pytest",
            "tests/e2e",
            "--ignore=tests/e2e/migration",
            "--profile",
            "bundle-e2e",
            "--no-cov",
            "-v",
        ],
        cwd=tests,
        env=env,
        check=True,
    )
