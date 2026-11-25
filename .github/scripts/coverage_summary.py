"""Print the test coverage summary of a CI run as Markdown, from `coverage json` reports.

The summary gives the total coverage. With --changed it also lists the measured files that a
pull request changes, and with --base it compares the numbers with the base branch.
"""

import argparse
import json
from pathlib import Path
from typing import Any

# The numbers that a `coverage json` report gives for the whole run and for each file.
Summary = dict[str, Any]


def read_report(path: Path) -> tuple[Summary, dict[str, Summary]]:
    """Return the summary of the whole run and of each file from a `coverage json` report."""
    report = json.loads(path.read_text())
    return report["totals"], {name: entry["summary"] for name, entry in report["files"].items()}


def colored(text: str, color: str) -> str:
    """Color text for GitHub, which removes HTML colors from Markdown but renders math notation."""
    return f"$`\\color{{{color}}}{{\\textsf{{{text}}}}}`$"


def change(delta: float, better: int) -> str:
    """Describe a change from the base branch; `better` is the sign of a change for the better."""
    if delta == 0:
        return "-"
    return colored(f"{delta:+}", "ForestGreen" if delta * better > 0 else "Red")


def compare(text: str, after: Summary, before: Summary | None, key: str, better: int) -> str:
    """Append to `text` the change of one number from the base branch, when there is a base value."""
    if before is None:
        return text
    return f"{text} ({change(round(after[key] - before[key], 1), better)})"


def summarize(head: Path, base: Path | None, changed: Path | None) -> str:
    """Render the summary; `changed` lists the paths a pull request changes, one per line."""
    total, files = read_report(head)
    base_total, base_files = read_report(base) if base else (None, {})

    def coverage(after: Summary, before: Summary | None) -> str:
        return compare(f"{after['percent_covered']:.1f}%", after, before, "percent_covered", better=1)

    lines = [f"## Test coverage: {coverage(total, base_total)}"]
    if changed is None:
        return "\n".join(lines)

    lines += ["", "### Files changed in this pull request", ""]
    measured = [name for name in changed.read_text().splitlines() if name in files]
    if measured:
        lines += ["| File | Stmts | Miss | Coverage |", "| --- | ---: | ---: | ---: |"]
        for name in measured:
            file, before = files[name], base_files.get(name)
            cells = [
                f"`{name}`",
                str(file["num_statements"]),
                compare(str(file["missing_lines"]), file, before, "missing_lines", better=-1),
                coverage(file, before) + (" (new)" if base and before is None else ""),
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
    if base:
        better, worse = colored("green", "ForestGreen"), colored("red", "Red")
        legend = f"In parentheses is the change from the base branch: {better} is better, {worse} is worse"
        lines += ["", f"{legend}, and (-) is no change."]
    else:
        lines += ["", "The base branch has no coverage report, so there is no comparison."]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("head", type=Path, help="`coverage json` report of this run")
    parser.add_argument("--base", type=Path, help="`coverage json` report of the base branch")
    parser.add_argument("--changed", type=Path, help="file that lists the changed paths, one per line")
    args = parser.parse_args()
    print(summarize(args.head, args.base, args.changed))


if __name__ == "__main__":
    main()
