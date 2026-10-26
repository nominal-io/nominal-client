"""Check identity is automatic unless callers opt into a stable key."""

from uuid import UUID

import pytest

from nominal.core import Priority
from nominal.experimental.compute_as_code import CheckDefinition, ChecklistBuilder


def builder_with(name: str = "Check", *, key: str | None = None, limit: int = 1) -> ChecklistBuilder:
    """Build one constant check with an optional stable identity."""
    return ChecklistBuilder().add(
        CheckDefinition(name, f"(NumericSeries.Constant(2) > {limit}).ranges()", key=key, priority=Priority.P1),
        inputs={},
    )


def test_anonymous_identity_is_captured_and_independent() -> None:
    """Each independently built anonymous check receives a distinct UUID4 identity."""
    builder = ChecklistBuilder().add(
        CheckDefinition("Check", "(NumericSeries.Constant(2) > 1).ranges()", priority=Priority.P1), inputs={}
    )
    first = builder._checks[0].check_lineage_rid
    assert UUID(first).version == 4
    assert builder_with()._checks[0].check_lineage_rid != first


def test_key_survives_renaming_and_expression_changes() -> None:
    """A stable key preserves lineage independently of display name and expression contents."""
    first = builder_with(key="engine.temperature")._checks[0].check_lineage_rid
    assert UUID(first).version == 5
    assert builder_with("Renamed", key="engine.temperature", limit=3)._checks[0].check_lineage_rid == first
    assert builder_with(key="other")._checks[0].check_lineage_rid != first


@pytest.mark.parametrize("key", ["", "   "])
def test_empty_keys_are_rejected(key: str) -> None:
    """An accidentally empty key is not a stable identity."""
    with pytest.raises(ValueError, match="nonempty"):
        builder_with(key=key)


def test_duplicate_key_does_not_add_a_check() -> None:
    """Two checks cannot accidentally claim the same stable identity in a definition."""
    builder = ChecklistBuilder()
    ranges = "(NumericSeries.Constant(2) > 1).ranges()"
    builder.add(CheckDefinition("First", ranges, key="same", priority=Priority.P1), inputs={})
    with pytest.raises(ValueError, match="Duplicate key"):
        builder.add(CheckDefinition("Second", ranges, key="same", priority=Priority.P1), inputs={})
    assert len(builder._checks) == 1
