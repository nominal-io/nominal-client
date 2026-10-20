"""Check identity is automatic unless callers opt into a stable key."""

from uuid import UUID

import pytest

from nominal.core import Priority
from nominal.experimental.compute_as_code import CheckDefinition, ChecklistBuilder


def definition(name="Check", *, key=None, limit=1):
    return ChecklistBuilder().add(
        CheckDefinition(name, f"(NumericSeries.Constant(2) > {limit}).ranges()", key=key, priority=Priority.P1),
        inputs={},
    )


def test_anonymous_identity_is_captured_and_independent():
    """Reusing a definition preserves its UUID, while rebuilding creates a new logical check."""
    builder = ChecklistBuilder().add(
        CheckDefinition("Check", "(NumericSeries.Constant(2) > 1).ranges()", priority=Priority.P1), inputs={}
    )
    first = builder._checks[0].check_lineage_rid
    assert UUID(first).version == 4
    assert builder._checks[0].check_lineage_rid == first
    assert definition()._checks[0].check_lineage_rid != first


def test_key_survives_renaming_and_expression_changes():
    """A stable key preserves lineage independently of display name and expression contents."""
    first = definition(key="engine.temperature")._checks[0].check_lineage_rid
    assert UUID(first).version == 5
    assert definition("Renamed", key="engine.temperature", limit=3)._checks[0].check_lineage_rid == first
    assert definition(key="other")._checks[0].check_lineage_rid != first


@pytest.mark.parametrize("key", ["", "   "])
def test_empty_keys_are_rejected(key):
    """An accidentally empty key is not a stable identity."""
    with pytest.raises(ValueError, match="nonempty"):
        definition(key=key)


def test_duplicate_key_does_not_add_a_check():
    """Two checks cannot accidentally claim the same stable identity in a definition."""
    builder = ChecklistBuilder()
    ranges = "(NumericSeries.Constant(2) > 1).ranges()"
    builder.add(CheckDefinition("First", ranges, key="same", priority=Priority.P1), inputs={})
    with pytest.raises(ValueError, match="Duplicate key"):
        builder.add(CheckDefinition("Second", ranges, key="same", priority=Priority.P1), inputs={})
    assert len(builder._checks) == 1
