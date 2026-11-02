"""Emit installed package versions and available license notices without local paths."""

import importlib.metadata
import json

packages = []
for dist in importlib.metadata.distributions():
    notices = {}
    for file in dist.files or []:
        if any(part.lower().startswith(("license", "notice", "copying")) for part in file.parts):
            path = dist.locate_file(file)
            if path.is_file():
                notices[str(file)] = path.read_text(errors="replace")
    packages.append(
        {
            "name": dist.metadata["Name"],
            "version": dist.version,
            "license": dist.metadata.get("License-Expression") or dist.metadata.get("License"),
            "notices": notices,
        }
    )
print(json.dumps(sorted(packages, key=lambda p: p["name"].lower()), indent=2))
