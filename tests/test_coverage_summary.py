"""The coverage summary that CI writes for a run: the total, the changed files, and the comparison."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "coverage_summary.py"

# GitHub removes HTML colors from Markdown, so the summary colors text with math notation.
GREEN = r"$`\color{ForestGreen}{\textsf{%s}}`$"
RED = r"$`\color{Red}{\textsf{%s}}`$"
COLUMNS = (
    "- **Stmts**: the number of statements in the file.\n"
    "- **Miss**: the number of statements that no test ran.\n"
    "- **Coverage**: the share of statements and branches that the tests ran.\n"
)
LEGEND = (
    f"In parentheses is the change from the base branch: {GREEN % 'green'} is better, {RED % 'red'} is worse, "
    "and (-) is no change.\n"
)


def write_report(path: Path, total: float, files: dict[str, tuple[int, int, float]]) -> Path:
    """Write the parts of a `coverage json` report that the summary reads.

    Each file maps to its number of statements, its number of missed statements and its coverage percentage.
    """
    report = {
        "totals": {"percent_covered": total},
        "files": {
            name: {"summary": {"num_statements": statements, "missing_lines": missed, "percent_covered": percent}}
            for name, (statements, missed, percent) in files.items()
        },
    }
    path.write_text(json.dumps(report))
    return path


def write_changed(path: Path, *lines: str) -> Path:
    """Write the lines that `git diff --name-status` prints for the changed files."""
    path.write_text("".join(f"{line}\n" for line in lines))
    return path


def summarize(*args: object) -> str:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, args)],
        check=False,
        capture_output=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_run_without_pull_request_shows_only_the_total(tmp_path: Path) -> None:
    """A push has no changed files and no base, so the summary is the total alone."""
    head = write_report(tmp_path / "head.json", 81.26, {"nominal/a.py": (10, 5, 50.0)})

    assert summarize(head) == "## Test coverage: 81.3%\n"


def test_pull_request_compares_changed_files_with_the_base(tmp_path: Path) -> None:
    """Only measured changed files are listed, and each number shows its change from the base: good or bad."""
    head = write_report(
        tmp_path / "head.json",
        81.26,
        {
            "nominal/down.py": (200, 100, 50.0),
            "nominal/new.py": (10, 1, 90.0),
            "nominal/renamed.py": (30, 3, 90.0),
            "nominal/same.py": (50, 20, 60.0),
            "nominal/untouched.py": (40, 36, 10.0),
            "nominal/up.py": (405, 87, 79.64),
        },
    )
    base = write_report(
        tmp_path / "base.json",
        81.0,
        {
            "nominal/down.py": (190, 93, 50.8),
            "nominal/old_name.py": (30, 6, 80.0),
            "nominal/same.py": (50, 20, 60.04),  # lower by less than the rounding, so it counts as no change
            "nominal/untouched.py": (40, 32, 20.0),
            "nominal/up.py": (400, 98, 75.5),
        },
    )
    changed = write_changed(
        tmp_path / "changed.txt",
        "M\tnominal/core/__init__.py",  # changed, but not measured
        "M\tnominal/down.py",
        "A\tnominal/new.py",
        "R090\tnominal/old_name.py\tnominal/renamed.py",
        "M\tnominal/same.py",
        "M\tnominal/up.py",
    )

    assert summarize(head, "--base", base, "--changed", changed) == (
        f"## Test coverage: 81.3% ({GREEN % '+0.3'})\n"
        "\n"
        "### Files changed in this pull request\n"
        "\n"
        "| File | Stmts | Miss | Coverage |\n"
        "| --- | ---: | ---: | ---: |\n"
        f"| `nominal/down.py` | 200 | 100 ({RED % '+7'}) | 50.0% ({RED % '-0.8'}) |\n"
        "| `nominal/new.py` | 10 | 1 | 90.0% (new) |\n"
        f"| `nominal/renamed.py` | 30 | 3 ({GREEN % '-3'}) | 90.0% ({GREEN % '+10.0'}) |\n"
        "| `nominal/same.py` | 50 | 20 (-) | 60.0% (-) |\n"
        f"| `nominal/up.py` | 405 | 87 ({GREEN % '-11'}) | 79.6% ({GREEN % '+4.1'}) |\n"
        "\n"
        f"{COLUMNS}"
        "\n"
        f"{LEGEND}"
    )


@pytest.mark.parametrize("changed_lines", [[], ["M\tnominal/core/__init__.py"]], ids=["nothing", "unmeasured file"])
def test_pull_request_without_measured_changes_says_so(tmp_path: Path, changed_lines: list[str]) -> None:
    """A pull request that changes no measured file gets a sentence instead of an empty table."""
    head = write_report(tmp_path / "head.json", 81.0, {"nominal/a.py": (10, 5, 50.0)})
    base = write_report(tmp_path / "base.json", 81.0, {"nominal/a.py": (10, 5, 50.0)})
    changed = write_changed(tmp_path / "changed.txt", *changed_lines)

    assert summarize(head, "--base", base, "--changed", changed) == (
        "## Test coverage: 81.0% (-)\n"
        "\n"
        "### Files changed in this pull request\n"
        "\n"
        "This pull request changes no measured files.\n"
        "\n"
        f"{LEGEND}"
    )


@pytest.mark.parametrize(
    ("changed_line", "lists_a_file"),
    [("M\tnominal/a.py", True), ("M\tnominal/core/__init__.py", False)],
    ids=["measured file", "unmeasured file"],
)
def test_comment_file_is_written_only_when_the_summary_lists_a_file(
    tmp_path: Path, changed_line: str, lists_a_file: bool
) -> None:
    """The pull request comment repeats the summary, and there is none when it has no file to show."""
    head = write_report(tmp_path / "head.json", 81.0, {"nominal/a.py": (10, 5, 50.0)})
    changed = write_changed(tmp_path / "changed.txt", changed_line)
    comment = tmp_path / "comment.md"

    summary = summarize(head, "--changed", changed, "--comment", comment)

    assert (comment.read_text() if comment.exists() else None) == (summary if lists_a_file else None)


def test_pull_request_without_base_report_shows_no_comparison(tmp_path: Path) -> None:
    """When the base branch could not be measured, the summary gives the numbers and says why they stand alone."""
    head = write_report(tmp_path / "head.json", 81.0, {"nominal/a.py": (10, 5, 50.0)})
    changed = write_changed(tmp_path / "changed.txt", "M\tnominal/a.py")

    assert summarize(head, "--changed", changed) == (
        "## Test coverage: 81.0%\n"
        "\n"
        "### Files changed in this pull request\n"
        "\n"
        "| File | Stmts | Miss | Coverage |\n"
        "| --- | ---: | ---: | ---: |\n"
        "| `nominal/a.py` | 10 | 5 | 50.0% |\n"
        "\n"
        f"{COLUMNS}"
        "\n"
        "The base branch has no coverage report, so there is no comparison.\n"
    )
