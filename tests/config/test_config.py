from __future__ import annotations

from pathlib import Path

import pytest

import nominal.config as config_module
from nominal.config import ConfigProfile, NominalConfig
from nominal.core.exceptions import NominalConfigError


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    return tmp_path / "config.yml"


def test_nominal_config_round_trips_profiles(config_path: Path) -> None:
    """Writing and reading the v2 config should preserve profiles exactly."""
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


def test_nominal_config_get_profile_returns_matching_profile() -> None:
    """get_profile should return the ConfigProfile for the given name."""
    profile = ConfigProfile(base_url="https://api.gov.nominal.io/api", token="tok")
    config = NominalConfig(version=2, profiles={"default": profile})

    assert config.get_profile("default") == profile


def test_nominal_config_get_profile_raises_when_not_found() -> None:
    """get_profile should raise NominalConfigError when the profile name is absent."""
    config = NominalConfig(version=2, profiles={})

    with pytest.raises(NominalConfigError, match="'missing' not found"):
        config.get_profile("missing")


def test_nominal_config_raises_when_no_config_exists(
    config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing v2 config with no deprecated fallback should raise a FileNotFoundError."""
    monkeypatch.setattr(config_module, "DEPRECATED_NOMINAL_CONFIG_PATH", tmp_path / "deprecated.yml")

    with pytest.raises(FileNotFoundError, match="create with `nom config profile add`"):
        NominalConfig.from_yaml(config_path)


def test_nominal_config_guides_migration_when_only_deprecated_config_exists(
    config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing v2 config should point users at migration if the deprecated config still exists."""
    deprecated_path = tmp_path / "deprecated.yml"
    deprecated_path.write_text("environments:\n  api.gov.nominal.io/api: token\n")
    monkeypatch.setattr(config_module, "DEPRECATED_NOMINAL_CONFIG_PATH", deprecated_path)

    with pytest.raises(FileNotFoundError, match="migrate with `nom config migrate`"):
        NominalConfig.from_yaml(config_path)


def test_nominal_config_rejects_empty_files(config_path: Path) -> None:
    """An empty v2 config file should raise the same user-facing error as a missing version key."""
    config_path.write_text("")

    with pytest.raises(NominalConfigError, match="missing 'version' key"):
        NominalConfig.from_yaml(config_path)


def test_nominal_config_rejects_missing_profiles_key(config_path: Path) -> None:
    """A config file with a version but no profiles key should raise a NominalConfigError."""
    config_path.write_text("version: 2\n")

    with pytest.raises(NominalConfigError, match="missing 'profiles' key"):
        NominalConfig.from_yaml(config_path)


def test_nominal_config_rejects_unsupported_version(config_path: Path) -> None:
    """A config file with an unrecognised version number should raise a NominalConfigError."""
    config_path.write_text("version: 1\nprofiles: {}\n")

    with pytest.raises(NominalConfigError, match="unsupported config version"):
        NominalConfig.from_yaml(config_path)
