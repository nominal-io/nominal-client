from __future__ import annotations

import json
from typing import Any, Callable, Iterable, Mapping, Sequence, TypeVar

import click

T = TypeVar("T")


def emit_jsonl(records: Iterable[Mapping[str, Any]]) -> None:
    """Emit one compact JSON object per line on stdout, suitable for piping into `jq` etc."""
    for record in records:
        click.echo(json.dumps(record, separators=(",", ":")))


def emit_records(
    records: Sequence[T],
    output_format: str,
    *,
    to_dict: Callable[[T], Mapping[str, Any]],
    render_table: Callable[[Sequence[T]], None],
    render_detail: Callable[[T], None] | None = None,
) -> None:
    """Render a sequence of records in the chosen output format.

    Pairs with the `output_fmt_options` decorator: pass the `output_format` kwarg through.

    For `json` format, encodes each record via `to_dict` and emits JSONL.
    For `table` format (the default), calls `render_detail` when there is exactly one record and a
    detail renderer was supplied; otherwise calls `render_table` with all records (including the
    empty case so the renderer can show its own "no results" message).
    """
    if output_format == "json":
        emit_jsonl(to_dict(record) for record in records)
        return
    if len(records) == 1 and render_detail is not None:
        render_detail(records[0])
    else:
        render_table(records)


def render_properties(props: Mapping[str, str] | None) -> str:
    if not props:
        return "-"
    items = [f"'{k}'='{v}'" for k, v in list(props.items())[:6]]
    suffix = " ..." if props and len(props) > 6 else ""
    return ", ".join(items) + suffix


def render_labels(labels: Sequence[str] | None) -> str:
    if not labels:
        return "-"
    items = [f"'{label}'" for label in labels[:6]]
    suffix = " ..." if labels and len(labels) > 6 else ""
    return ", ".join(items) + suffix
