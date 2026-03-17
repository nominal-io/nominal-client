from pathlib import Path

import pytest

import nominal.config as config_module
from nominal.config import ConfigProfile, NominalConfig
from nominal.config._config import NominalConfigV1
from nominal.core.exceptions import NominalConfigError


def test_nominal_config_round_trips_profiles(tmp_path: Path) -> None:
    """Writing and reading the v2 config should preserve profiles exactly."""
    config_path = tmp_path / "config.yml"
    config = NominalConfig(
        version=2,
        profiles={
            "default": ConfigProfile(
                base_url="https://api.gov.nominal.io/api",
                token="token-1",
            ),
            "staging": ConfigProfile(
                base_url="https://api-staging.gov.nominal.io/api",
                token="token-2",
                workspace_rid="ri.workspace.test.workspace.123",
            ),
        },
    )

    config.to_yaml(config_path)

    assert NominalConfig.from_yaml(config_path) == config


def test_nominal_config_guides_migration_when_only_deprecated_config_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing v2 config should point users at migration if the deprecated config still exists."""
    config_path = tmp_path / "config.yml"
    deprecated_path = tmp_path / "deprecated.yml"
    deprecated_path.write_text("environments:\n  api.gov.nominal.io/api: token\n")
    monkeypatch.setattr(config_module, "DEPRECATED_NOMINAL_CONFIG_PATH", deprecated_path)

    with pytest.raises(FileNotFoundError, match="migrate with `nom config migrate`"):
        NominalConfig.from_yaml(config_path)


def test_nominal_config_rejects_empty_files(tmp_path: Path) -> None:
    """An empty v2 config file should raise the same user-facing error as a missing version key."""
    config_path = tmp_path / "empty.yml"
    config_path.write_text("")

    with pytest.raises(NominalConfigError, match="missing 'version' key"):
        NominalConfig.from_yaml(config_path)


def test_deprecated_nominal_config_rejects_empty_files(tmp_path: Path) -> None:
    """An empty deprecated config should raise a NominalConfigError instead of an internal TypeError."""
    config_path = tmp_path / "empty.yml"
    config_path.write_text("")

    with pytest.raises(NominalConfigError, match="missing 'environments' key"):
        NominalConfigV1.from_yaml(config_path)


def test_deprecated_nominal_config_includes_the_invalid_url_in_errors() -> None:
    """Scheme validation should show the offending URL instead of a literal format placeholder."""
    config = NominalConfigV1(environments={})

    with pytest.raises(ValueError, match=r"url 'https://api\.nominal\.test/api' must not include"):
        config.set_token("https://api.nominal.test/api", "token")

    with pytest.raises(ValueError, match=r"url 'https://api\.nominal\.test/api' must not include"):
        config.get_token("https://api.nominal.test/api")
