from __future__ import annotations

from unittest.mock import MagicMock

import click
from click.testing import CliRunner

from nominal.cli.util.format import emit_records, render_labels, render_properties


def test_emit_records_json_outputs_one_compact_record_per_line() -> None:
    @click.command()
    def command() -> None:
        emit_records(
            [{"rid": "one"}, {"rid": "two"}],
            "json",
            to_dict=lambda record: record,
            render_table=MagicMock(),
        )

    result = CliRunner().invoke(command)

    assert result.exit_code == 0
    assert result.output == '{"rid":"one"}\n{"rid":"two"}\n'


def test_emit_records_table_format_uses_detail_for_single_record() -> None:
    render_table = MagicMock()

    @click.command()
    def command() -> None:
        emit_records(
            [{"rid": "one"}],
            "table",
            to_dict=lambda record: record,
            render_table=render_table,
            render_detail=lambda record: click.echo(f"detail {record['rid']}"),
        )

    result = CliRunner().invoke(command)

    assert result.exit_code == 0
    assert result.output == "detail one\n"
    render_table.assert_not_called()


def test_render_labels_and_properties_show_empty_and_truncated_values() -> None:
    assert render_labels([]) == "-"
    assert render_properties({}) == "-"
    assert render_labels(["a", "b", "c", "d", "e", "f", "g"]) == "'a', 'b', 'c', 'd', 'e', 'f' ..."
    assert (
        render_properties({str(i): str(i) for i in range(7)})
        == "'0'='0', '1'='1', '2'='2', '3'='3', '4'='4', '5'='5' ..."
    )
