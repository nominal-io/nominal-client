"""Shared dry-run logging vocabulary.

The dry-run log lines emitted by the migrators are parsed downstream (e.g. by
``nom migrate summary --from-log`` and CI job summaries), so both sides must agree on the
exact wording. Build dry-run "would create" log messages with :func:`would_create_message`
and parse them with :func:`dry_run_create_pattern` so the two can never drift.
"""

from __future__ import annotations

import re

from nominal.experimental.migration.resource_type import ResourceType, resource_label

DRY_RUN_PREFIX = "[DRY RUN]"

_WOULD_CREATE = "Would create"


def would_create_message(resource_type: ResourceType) -> str:
    """Logging format string for a dry-run create line, with %s placeholders for name and source RID."""
    return f"{DRY_RUN_PREFIX} {_WOULD_CREATE} {resource_label(resource_type)} '%s' (source: %s)"


def dry_run_create_pattern() -> re.Pattern[str]:
    """Regex matching lines produced by :func:`would_create_message`, capturing the resource label as ``type``.

    Labels are alternated longest-first so multi-word labels (e.g. "workbook template") are not
    truncated to their first word ("workbook").
    """
    labels = sorted((resource_label(resource_type) for resource_type in ResourceType), key=len, reverse=True)
    alternation = "|".join(re.escape(label) for label in labels)
    return re.compile(
        rf"{re.escape(DRY_RUN_PREFIX)}\s+{_WOULD_CREATE}\s+(?P<type>{alternation})\b",
        re.IGNORECASE,
    )
