"""Regression checks for the bounded legacy Markdown adapter."""

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "markdown_docstrings", Path(__file__).resolve().parents[1] / "_ext/markdown_docstrings.py"
)
assert SPEC is not None and SPEC.loader is not None
ADAPTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ADAPTER)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        (
            ["Args:", "    value: Example:", "        ```python", "        print(1)", "        ```"],
            ["Args:", "    value: Example:", "        .. code-block:: python", "", "            print(1)", ""],
        ),
        (
            ["    ```python", "    print(1)", "```", "After."],
            ["    .. code-block:: python", "", "        print(1)", "", "After."],
        ),
        (["```", "raw data", "```"], [".. code-block:: text", "", "    raw data", ""]),
        (["[docs](https://example.com)"], ["`docs <https://example.com>`__"]),
        (["`[docs](https://example.com)`"], ["`[docs](https://example.com)`"]),
        (["`prefix [docs](https://example.com)`"], ["`prefix [docs](https://example.com)`"]),
        (
            ["``prefix `x` [docs](https://example.com)`` and [site](https://example.org)"],
            ["``prefix `x` [docs](https://example.com)`` and `site <https://example.org>`__"],
        ),
        (
            ["```text", "[docs](https://example.com)", "```"],
            [".. code-block:: text", "", "    [docs](https://example.com)", ""],
        ),
    ],
)
def test_legacy_markup_conversion(before: list[str], after: list[str]) -> None:
    """Convert legacy links and fences while preserving literal code contents."""
    assert ADAPTER._convert(before) == after
