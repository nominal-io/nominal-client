"""Launch the installed notebook environment with writable state paths."""

import json
import os
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
env = os.environ.copy()
env.pop("PYTHONPATH", None)
env.update(json.loads((root / "runtime-env.json").read_text()))
python = root / "environment" / ("python.exe" if os.name == "nt" else "bin/python")
raise SystemExit(subprocess.call([str(python), "-m", "jupyterlab", *sys.argv[1:]], cwd=root / "work", env=env))
