from pathlib import Path
from typing import Iterable
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click import UsageError
from click.testing import CliRunner

from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import MigrationResources
from nominal.experimental.migration.migration_cli import _load_added_labels, migrate_cmd
from nominal.experimental.migration.migration_runner import MigrationRunner


def _make_runner(added_labels: Iterable[str]) -> MigrationRunner:
    return MigrationRunner(
        migration_resources=MigrationResources(source_assets={}, source_standalone_templates=[]),
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=False, include_dataset_files=False),
        destination_client=MagicMock(),
        added_labels=added_labels,  # type: ignore[arg-type]
    )


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


def test_runner_rejects_a_single_string_as_added_labels() -> None:
    """A bare string is rejected rather than split into one label per character."""
    with pytest.raises(TypeError, match="added_labels"):
        _make_runner("migrated")


def test_runner_labels_every_resource_from_a_one_pass_iterable() -> None:
    """A one-pass iterable such as map labels every resource, not only the first one created."""
    ctx = _make_runner(map(str.strip, [" migrated "])).build_context()
    assert [ctx.destination_labels([]) for _ in range(2)] == [["migrated"], ["migrated"]]


def test_runner_ignores_changes_to_a_passed_list_after_creation() -> None:
    """Editing the list passed as added_labels after the runner is created has no effect."""
    labels = ["migrated"]
    ctx = _make_runner(labels).build_context()
    labels.append("batch-1")
    assert ctx.destination_labels([]) == ["migrated"]


def test_copy_passes_config_add_labels_to_the_runner(tmp_path: Path) -> None:
    """`nom migrate copy` starts the migration with the config's add_labels."""
    config = tmp_path / "migration.yml"
    config.write_text(
        "migration:\n"
        "  name: labels\n"
        "  include_dataset_files: false\n"
        "  preserve_dataset_uuid: false\n"
        "  add_labels: [migrated, batch-1]\n",
        encoding="utf-8",
    )
    args = ["copy", "--source-profile", "source", "--destination-profile", "destination", "--config", str(config)]

    with (
        patch("nominal.experimental.migration.migration_decorators.NominalClient.from_profile"),
        patch("nominal.experimental.migration.migration_cli.run_parallel_migration") as run_parallel_migration,
    ):
        result = CliRunner().invoke(migrate_cmd, args)

    assert result.exit_code == 0, result.output
    assert run_parallel_migration.call_args.args[0].added_labels == ("migrated", "batch-1")
