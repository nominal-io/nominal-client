"""Print the test coverage summary of a CI run as Markdown, from `coverage json` reports.

The summary gives the total coverage. With --changed it also lists the measured files that a
pull request changes, and with --base it compares the numbers with the base branch.
"""

import argparse
import json
from pathlib import Path
from typing import Literal, TypedDict


class Summary(TypedDict):
    """The numbers this script reads from a summary of a `coverage json` report."""

    num_statements: int
    missing_lines: int
    percent_covered: float


def read_report(path: Path) -> tuple[Summary, dict[str, Summary]]:
    """Return the summary of the whole run and of each file from a `coverage json` report."""
    report = json.loads(path.read_text())
    return report["totals"], {name: entry["summary"] for name, entry in report["files"].items()}


def colored(text: str, color: str) -> str:
    """Color text for GitHub, which removes HTML colors from Markdown but renders math notation."""
    return f"$`\\color{{{color}}}{{\\textsf{{{text}}}}}`$"


def change(delta: float, higher_is_better: bool) -> str:
    """Describe a change from the base branch: green when it is for the better, red when it is for the worse."""
    if delta == 0:
        return "-"
    return colored(f"{delta:+}", "ForestGreen" if (delta > 0) == higher_is_better else "Red")


def compare(
    text: str,
    after: Summary,
    before: Summary | None,
    key: Literal["missing_lines", "percent_covered"],
    higher_is_better: bool,
) -> str:
    """Append to `text` the change of one number from the base branch, when there is a base value."""
    if before is None:
        return text
    return f"{text} ({change(round(after[key] - before[key], 1), higher_is_better)})"


def coverage(after: Summary, before: Summary | None) -> str:
    """Format the coverage percentage, with its change from the base branch when there is a base value."""
    return compare(f"{after['percent_covered']:.1f}%", after, before, "percent_covered", higher_is_better=True)


def summarize(head: Path, base: Path | None, changed: Path | None) -> tuple[str, bool]:
    """Render the summary, and say whether it lists a changed file.

    `changed` holds the `git diff --name-status` lines of a pull request.
    """
    total, files = read_report(head)
    base_total: Summary | None = None
    base_files: dict[str, Summary] = {}
    if base is not None:
        base_total, base_files = read_report(base)

    lines = [f"## Test coverage: {coverage(total, base_total)}"]
    if changed is None:
        return "\n".join(lines), False

    lines += ["", "### Files changed in this pull request", ""]
    # A line is a status and a path, separated by tabs. A renamed file has its old path before the new one.
    paths = [line.split("\t")[1:] for line in changed.read_text().splitlines()]
    measured = [(path[-1], path[0]) for path in paths if path[-1] in files]
    if measured:
        lines += ["| File | Stmts | Miss | Coverage |", "| --- | ---: | ---: | ---: |"]
        for name, base_name in measured:
            file, before = files[name], base_files.get(base_name)
            cells = [
                f"`{name}`",
                str(file["num_statements"]),
                compare(str(file["missing_lines"]), file, before, "missing_lines", higher_is_better=False),
                coverage(file, before) + (" (new)" if base is not None and before is None else ""),
            ]
            lines.append(f"| {' | '.join(cells)} |")
        lines += [
            "",
            "- **Stmts**: the number of statements in the file.",
            "- **Miss**: the number of statements that no test ran.",
            "- **Coverage**: the share of statements and branches that the tests ran.",
        ]
    else:
        lines.append("This pull request changes no measured files.")
    if base is not None:
        better, worse = colored("green", "ForestGreen"), colored("red", "Red")
        legend = f"In parentheses is the change from the base branch: {better} is better, {worse} is worse"
        lines += ["", f"{legend}, and (-) is no change."]
    else:
        lines += ["", "The base branch has no coverage report, so there is no comparison."]
    return "\n".join(lines), bool(measured)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("head", type=Path, help="`coverage json` report of this run")
    parser.add_argument("--base", type=Path, help="`coverage json` report of the base branch")
    parser.add_argument(
        "--changed", type=Path, help="file with the `git diff --name-status` lines of the changed paths"
    )
    parser.add_argument(
        "--comment", type=Path, help="also write the summary to this file, when it lists a changed file"
    )
    args = parser.parse_args()
    summary, lists_a_file = summarize(args.head, args.base, args.changed)
    print(summary)
    # A pull request comment is worth posting only when it has a file to show.
    if args.comment is not None and lists_a_file:
        args.comment.write_text(f"{summary}\n")


if __name__ == "__main__":
    main()
