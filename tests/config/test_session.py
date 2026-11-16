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
    assert nc.list_profiles() == {}  # no config file yet
    write(config, "default", "docs")
    assert nc.list_profiles() == {"default": "https://default.test/api", "docs": "https://docs.test/api"}


def test_active_profile(config: Path, monkeypatch: pytest.MonkeyPatch):
    write(config, "default", "docs", "me")
    assert nc.active_profile() == "default"
    monkeypatch.setenv(profile.ENV, "docs")
    assert nc.active_profile() == "docs"
    nc.use_profile("me")
    assert nc.active_profile() == "me"  # beats the environment variable
    nc.use_profile(None)
    assert nc.active_profile() == "docs"


def test_use_missing_profile(config: Path):
    write(config, "default")
    with pytest.raises(ValueError, match="no Nominal profile 'nope'; profiles: 'default'"):
        nc.use_profile("nope")
    assert nc.active_profile() == "default"


def test_get_client(monkeypatch: pytest.MonkeyPatch):
    asked: list[str] = []
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda name: asked.append(name) or name)
    monkeypatch.setenv(profile.ENV, "docs")
    assert nc.get_client() == "docs"
    assert nc.get_client("me") == "me"
    assert asked == ["docs", "me"]


def test_get_client_workspace(config: Path, monkeypatch: pytest.MonkeyPatch):
    write(config, "default")
    made: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        "nominal.core.NominalClient.from_token", lambda *a, **kw: made.append((*a, kw["workspace_rid"]))
    )
    monkeypatch.setenv(profile.WORKSPACE_ENV, "ri.workspace.docs")
    nc.get_client()
    assert made == [("secret-default", "https://default.test/api", "ri.workspace.docs")]


def test_get_missing_client(monkeypatch: pytest.MonkeyPatch):
    def missing(name: str) -> None:
        raise FileNotFoundError("no config file")

    monkeypatch.setattr("nominal.core.NominalClient.from_profile", missing)
    with pytest.raises(ValueError, match="there are no profiles yet. Make one with nominal.config.make_profile"):
        nc.get_client()


def test_make_profile(config: Path, monkeypatch: pytest.MonkeyPatch):
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
    monkeypatch.setattr("nominal.core.NominalClient.from_token", FakeClient)
    nc.make_profile(token="key")
    assert nc.list_profiles() == {"default": DEFAULT_API_BASE_URL}


def test_make_profile_bad_key(config: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("nominal.core.NominalClient.from_token", FakeClient)
    with pytest.raises(RuntimeError, match="401"):
        nc.make_profile(token="bad")
    assert not config.exists()  # nothing saved


def test_legacy_env_var(config: Path, monkeypatch: pytest.MonkeyPatch):
    write(config, "default", "docs", "legacy")
    monkeypatch.setenv(profile.LEGACY_ENV, "legacy")
    assert nc.active_profile() == "legacy"
    monkeypatch.setenv(profile.ENV, "docs")
    assert nc.active_profile() == "docs"  # the new variable wins
