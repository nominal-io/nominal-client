from __future__ import annotations

import re

# Raw regex string matching a single UUID hex value (e.g. "12345678-abcd-1234-abcd-123456789abc").
UUID_RE = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

# Regex pattern to match strings that have a UUID format with a prefix.
UUID_PATTERN = re.compile(rf"^(.*)({UUID_RE})$")

# Keeping tight control over ids we consider to be UUIDs.
UUID_KEYS = ("id", "rid", "functionUuid", "plotId", "yAxisId", "chartRid")
