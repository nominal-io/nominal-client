import pytest
import yaml
from click import UsageError

from nominal.experimental.migration.migration_cli import _load_added_labels


@pytest.mark.parametrize(
    ("yaml_value", "expected"),
    [
        ("null", ()),
        ("[]", ()),
        ("[migrated, '2026-10-20']", ("migrated", "2026-10-20")),
    ],
)
def test_load_added_labels_accepts_absent_or_string_list(yaml_value: str, expected: tuple[str, ...]) -> None:
    """A missing, empty, or string-list add_labels loads as that tuple of labels."""
    assert _load_added_labels(yaml.safe_load(yaml_value)) == expected


@pytest.mark.parametrize(
    "yaml_value",
    [
        "migrated",
        "[2026-10-20]",
        "[42]",
        "[yes]",
        "['']",
        "['  ']",
    ],
)
def test_load_added_labels_rejects_non_string_or_blank_labels(yaml_value: str) -> None:
    """A scalar, a YAML value that parses to a non-string, or a blank label is a config error."""
    with pytest.raises(UsageError, match="add_labels"):
        _load_added_labels(yaml.safe_load(yaml_value))
