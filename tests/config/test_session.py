# ruff: noqa: D107
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml

import nominal.config as nc
from nominal.config import _session as profile
from nominal.core._constants import DEFAULT_API_BASE_URL


@pytest.fixture(autouse=True)
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A config file of our own, no profile chosen, and the profile variables unset."""
    path = tmp_path / "nominal" / "config.yml"
    monkeypatch.setattr(profile, "_config_path", lambda: path)
    monkeypatch.setattr(profile, "_chosen", None)
    monkeypatch.delenv(profile.ENV, raising=False)
    monkeypatch.delenv(profile.LEGACY_ENV, raising=False)
    monkeypatch.delenv(profile.WORKSPACE_ENV, raising=False)
    return path


def write(path: Path, *names: str) -> None:
    path.parent.mkdir(parents=True)
    profiles = {n: {"base_url": f"https://{n}.test/api", "token": f"secret-{n}", "workspace_rid": None} for n in names}
    path.write_text(yaml.dump({"version": 2, "profiles": profiles}))


class FakeClient:
    def __init__(self, token: str, base_url: str, **kwargs: Any) -> None:
        self.token, self.base_url = token, base_url

    def get_user(self) -> None:
        if self.token == "bad":
            raise RuntimeError("401")


def test_list_profiles(config: Path):
    """list_profiles maps names to URLs, hides tokens, and is empty without a config file."""
    assert nc.list_profiles() == {}  # no config file yet
    write(config, "default", "docs")
    assert nc.list_profiles() == {"default": "https://default.test/api", "docs": "https://docs.test/api"}


def test_active_profile(config: Path, monkeypatch: pytest.MonkeyPatch):
    """The session profile beats the environment variable, which beats the default."""
    write(config, "default", "docs", "me")
    assert nc.active_profile() == "default"
    monkeypatch.setenv(profile.ENV, "docs")
    assert nc.active_profile() == "docs"
    nc.use_profile("me")
    assert nc.active_profile() == "me"  # beats the environment variable
    nc.use_profile(None)
    assert nc.active_profile() == "docs"


def test_use_missing_profile(config: Path):
    """use_profile rejects a profile that doesn't exist and leaves the active one alone."""
    write(config, "default")
    with pytest.raises(ValueError, match="no Nominal profile 'nope'; profiles: 'default'"):
        nc.use_profile("nope")
    assert nc.active_profile() == "default"


def test_get_client(config: Path, monkeypatch: pytest.MonkeyPatch):
    """get_client uses the active profile unless given one."""
    write(config, "default", "docs", "me")
    asked: list[str] = []
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda name: asked.append(name) or name)
    monkeypatch.setenv(profile.ENV, "docs")
    assert nc.get_client() == "docs"
    assert nc.get_client("me") == "me"
    assert asked == ["docs", "me"]


def test_get_client_workspace(config: Path, monkeypatch: pytest.MonkeyPatch):
    """NOMINAL_WORKSPACE_RID replaces the profile's workspace."""
    write(config, "default")
    made: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        "nominal.core.NominalClient.from_token", lambda *a, **kw: made.append((*a, kw["workspace_rid"]))
    )
    monkeypatch.setenv(profile.WORKSPACE_ENV, "ri.workspace.docs")
    nc.get_client()
    assert made == [("secret-default", "https://default.test/api", "ri.workspace.docs")]


def test_get_missing_client(config: Path):
    """A profile that isn't in the config file raises a ValueError that says how to make one."""
    with pytest.raises(ValueError, match="there are no profiles yet. Make one with nominal.config.make_profile"):
        nc.get_client()
    write(config, "default")
    with pytest.raises(ValueError, match="no Nominal profile 'nope'; profiles: 'default'"):
        nc.get_client("nope")


def test_make_profile(config: Path, monkeypatch: pytest.MonkeyPatch):
    """make_profile saves a new profile, keeps the others, replaces a same-named one and restricts the file."""
    monkeypatch.setattr("nominal.core.NominalClient.from_token", FakeClient)
    write(config, "default")
    client = nc.make_profile("docs", token="key", base_url="https://docs.test/api")
    assert client.token == "key"
    saved = yaml.safe_load(config.read_text())["profiles"]
    assert saved["docs"] == {"base_url": "https://docs.test/api", "token": "key", "workspace_rid": None}
    assert saved["default"]["token"] == "secret-default"  # kept
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
    nc.make_profile("docs", token="new", base_url="https://docs.test/api")
    assert yaml.safe_load(config.read_text())["profiles"]["docs"]["token"] == "new"  # replaced


def test_make_first_profile(config: Path, monkeypatch: pytest.MonkeyPatch):
    """make_profile creates the config file with the default URL when none exists."""
    monkeypatch.setattr("nominal.core.NominalClient.from_token", FakeClient)
    nc.make_profile(token="key")
    assert nc.list_profiles() == {"default": DEFAULT_API_BASE_URL}


def test_make_profile_bad_key(config: Path, monkeypatch: pytest.MonkeyPatch):
    """A key that Nominal rejects is never saved."""
    monkeypatch.setattr("nominal.core.NominalClient.from_token", FakeClient)
    with pytest.raises(RuntimeError, match="401"):
        nc.make_profile(token="bad")
    assert not config.exists()  # nothing saved


def test_legacy_env_var(config: Path, monkeypatch: pytest.MonkeyPatch):
    """PANOMINAL_PROFILE still selects a profile, and NOMINAL_PROFILE wins over it."""
    write(config, "default", "docs", "legacy")
    monkeypatch.setenv(profile.LEGACY_ENV, "legacy")
    assert nc.active_profile() == "legacy"
    monkeypatch.setenv(profile.ENV, "docs")
    assert nc.active_profile() == "docs"  # the new variable wins


def test_invalid_existing_profile_is_not_reported_missing(config: Path, monkeypatch: pytest.MonkeyPatch):
    """A profile that exists but is invalid raises the client's NominalConfigError, not 'no such profile'."""
    from nominal.exceptions import NominalConfigError

    write(config, "default")

    def invalid(name: str) -> None:
        raise NominalConfigError("malformed base URL")

    monkeypatch.setattr("nominal.core.NominalClient.from_profile", invalid)
    with pytest.raises(NominalConfigError, match="malformed base URL"):
        nc.get_client()


def test_empty_profile_names_are_rejected(config: Path, monkeypatch: pytest.MonkeyPatch):
    """An empty profile name is rejected everywhere instead of silently choosing another profile."""
    monkeypatch.setattr("nominal.core.NominalClient.from_token", FakeClient)
    write(config, "default")
    with pytest.raises(ValueError, match="needs a name"):
        nc.make_profile("", token="key")
    with pytest.raises(ValueError, match="no Nominal profile ''"):
        nc.use_profile("")
    with pytest.raises(ValueError, match="no Nominal profile ''"):
        nc.get_client("")
    assert nc.active_profile() == "default"


def test_the_config_file_is_restricted_before_keys_are_written(config: Path, monkeypatch: pytest.MonkeyPatch):
    """make_profile makes the config file private before it writes any API key, whatever the umask."""
    import os

    monkeypatch.setattr("nominal.core.NominalClient.from_token", FakeClient)
    modes: list[int] = []
    real = nc.NominalConfig.to_yaml

    def spy(self: Any, path: Path) -> None:
        modes.append(stat.S_IMODE(path.stat().st_mode))
        real(self, path)

    monkeypatch.setattr(nc.NominalConfig, "to_yaml", spy)
    previous = os.umask(0o022)
    try:
        nc.make_profile(token="key")
    finally:
        os.umask(previous)
    assert modes == [0o600]  # already private when the keys were written, despite the umask
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
