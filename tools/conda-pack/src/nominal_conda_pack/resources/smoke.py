"""Offline functional checks. External firewall rules enforce network isolation."""

import importlib
import importlib.metadata
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd
import plotly.express as px
from nbconvert.preprocessors import ExecutePreprocessor
from scipy import stats

for module in ("nominal", "pyarrow", "openpyxl", "jupyterlab", "ipykernel", "polars"):
    importlib.import_module(module)
# Exercise native Rust bindings when the SDK publishes them for this target.
try:
    native_version = importlib.metadata.version("nominal-streaming")
except importlib.metadata.PackageNotFoundError:
    native_version = "not packaged for target"
else:
    importlib.import_module("nominal_streaming")

from nominal.core import NominalClient

NominalClient.create(base_url="https://offline.invalid/api", token="offline-test-placeholder")
work = Path(sys.argv[1])
frame = pd.DataFrame({"seconds": [0, 1, 2], "volts": [1.0, 2.0, 3.0]})
frame.to_csv(work / "example.csv", index=False)
frame.to_parquet(work / "example.parquet", index=False)
frame.to_excel(work / "example.xlsx", index=False)
for read in (
    pd.read_csv(work / "example.csv"),
    pd.read_parquet(work / "example.parquet"),
    pd.read_excel(work / "example.xlsx"),
):
    pd.testing.assert_frame_equal(frame, read, check_dtype=False)
assert np.isclose(stats.linregress(frame.seconds, frame.volts).slope, 1)
plt.plot(frame.seconds, frame.volts)
plt.savefig(work / "example.png")
plt.close()
px.line(frame, x="seconds", y="volts").write_html(work / "example.html", include_plotlyjs=True)
notebook = nbformat.read(work / "example.ipynb", as_version=4)
ExecutePreprocessor(timeout=180, kernel_name="nominal-offline").preprocess(notebook, {"metadata": {"path": str(work)}})
nbformat.write(notebook, work / "executed.ipynb")
(work / "smoke-result.json").write_text(
    json.dumps({"status": "passed", "nominal": importlib.metadata.version("nominal"), "python": sys.version}, indent=2)
)
print("Offline functional checks passed")
