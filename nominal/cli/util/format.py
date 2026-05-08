from __future__ import annotations

from typing import Mapping, Optional, Sequence


def render_properties(props: Optional[Mapping[str, str]]) -> str:
    if not props:
        return "-"
    items = [f"'{k}'='{v}'" for k, v in list(props.items())[:6]]
    suffix = " ..." if props and len(props) > 6 else ""
    return ", ".join(items) + suffix


def render_labels(labels: Optional[Sequence[str]]) -> str:
    if not labels:
        return "-"
    items = [f"'{label}'" for label in labels[:6]]
    suffix = " ..." if labels and len(labels) > 6 else ""
    return ", ".join(items) + suffix
