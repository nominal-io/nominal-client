from __future__ import annotations

from typing import Sequence

import click


def parse_key_value(
    ctx: click.Context, param: click.Parameter, values: Sequence[str]
) -> Sequence[tuple[str, str]]:
    """Click callback for `multiple=True` options that accept `KEY=VALUE` strings.

    Splits on the first `=` so values may themselves contain `=`. Empty keys are rejected.
    """
    parsed: list[tuple[str, str]] = []
    for value in values:
        key, sep, val = value.partition("=")
        if not sep or not key:
            raise click.BadParameter(f"expected KEY=VALUE, got {value!r}", ctx=ctx, param=param)
        parsed.append((key, val))
    return parsed
