from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from nominal import config as config_module
from nominal.cli.config import config_cmd


@pytest.fixture
def fake_default_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect every default-pathed config read/write into a tmp file for the duration of the test.

    `NominalConfig.from_yaml` and `to_yaml` baked the real default path into their function `__defaults__`
    at module-import time, so monkeypatching the module-level constant alone isn't enough. We rebind the
    function defaults on the bound classmethod / regular method to point at the tmp path.
    """
    config_path = tmp_path / "config.yml"
    monkeypatch.setattr(config_module, "DEFAULT_NOMINAL_CONFIG_PATH", config_path)

    from_yaml_func = config_module.NominalConfig.from_yaml.__func__  # classmethod -> underlying function
    to_yaml_func = config_module.NominalConfig.to_yaml

    monkeypatch.setattr(from_yaml_func, "__defaults__", (config_path,))
    monkeypatch.setattr(to_yaml_func, "__defaults__", (config_path,))

    return config_path


def test_profile_add_writes_smartcard_flag(fake_default_path: Path) -> None:
    """`nom config profile add --enable-smartcard-auth` should persist the flag in the config file."""
    runner = CliRunner()
    result = runner.invoke(
        config_cmd,
        [
            "profile",
            "add",
            "gov-cac",
            "--token",
            "tok",
            "--base-url",
            "https://api.gov.nominal.mil/api",
            "--no-validate",
            "--enable-smartcard-auth",
        ],
    )

    assert result.exit_code == 0, result.output

    cfg = config_module.NominalConfig.from_yaml(fake_default_path)
    assert cfg.profiles["gov-cac"].enable_smartcard_auth is True


def test_profile_add_defaults_smartcard_flag_off(fake_default_path: Path) -> None:
    """The CLI default leaves the smartcard flag disabled."""
    runner = CliRunner()
    result = runner.invoke(
        config_cmd,
        [
            "profile",
            "add",
            "default",
            "--token",
            "tok",
            "--no-validate",
        ],
    )

    assert result.exit_code == 0, result.output

    cfg = config_module.NominalConfig.from_yaml(fake_default_path)
    assert cfg.profiles["default"].enable_smartcard_auth is False


def test_profile_list_renders_smartcard_flag(fake_default_path: Path) -> None:
    """`nom config profile list` annotates profiles that opt into smartcard auth."""
    cfg = config_module.NominalConfig(
        version=2,
        profiles={
            "gov-cac": config_module.ConfigProfile(
                base_url="https://api.gov.nominal.mil/api",
                token="tok",
                enable_smartcard_auth=True,
            ),
        },
    )
    cfg.to_yaml(fake_default_path)

    runner = CliRunner()
    result = runner.invoke(config_cmd, ["profile", "list"])

    assert result.exit_code == 0, result.output
    assert "smartcard auth" in result.output
