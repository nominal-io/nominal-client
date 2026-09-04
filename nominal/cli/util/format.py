from __future__ import annotations

import pathlib
from typing import Mapping

import click
import tabulate


def table_data_to_string(table_data: Mapping[str, list[str]], format: str) -> str:
    """Render columnar data (column name -> values) as `csv` or a human-readable `table`."""
    import pandas as pd

    if format == "csv":
        return pd.DataFrame(table_data).to_csv(index=False)
    elif format == "table":
        return tabulate.tabulate(table_data, headers=list(table_data.keys()))
    else:
        raise ValueError(f"Expected format to be one of csv or table, received {format}")


def emit_table(output_str: str, output: pathlib.Path | None, what: str) -> None:
    """Echo tabular output, or write it to a file when `-o/--output` is given."""
    if output is None:
        click.echo(output_str)
    else:
        click.secho(f"Writing {what} metadata to {output}", fg="cyan")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(output_str)
