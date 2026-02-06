from __future__ import annotations

import re

# Regex pattern to match strings that have a UUID format with a prefix.
UUID_PATTERN = re.compile(r"^(.*)([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")

# Keeping tight control over ids we consider to be UUIDs.
UUID_KEYS = ("id", "rid", "functionUuid", "plotId", "yAxisId", "chartRid")
