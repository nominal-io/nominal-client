# ruff: noqa: D107
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import nominal.io
import nominal.pandas  # noqa: F401  (registers df.nom)
from nominal.io import instro

DATA = Path(__file__).parent.parent / "io" / "data"


class FakeFile:
    """Stands in for nominal.core.DatasetFile."""

    def __init__(self) -> None:
        self.polled = False

    def poll_until_ingestion_completed(self) -> "FakeFile":
        self.polled = True
        return self


class FakeDataset:
    """Stands in for nominal.core.Dataset: reads the uploaded file instead of sending it."""

    def __init__(self, name: str, rid: str, clients: Any = None) -> None:
        self.name, self.rid, self._clients = name, rid, clients
        self.uploaded: pd.DataFrame | None = None
        self.tags: Mapping[str, str] | None = None

    def add_avro_stream(self, path: Path, *, tags: Mapping[str, str] | None = None) -> FakeFile:
        assert path.suffix == ".avro"
        self.uploaded = instro.read_avro(path)
        self.tags = tags
        return FakeFile()


class FakeAsset:
    def __init__(self, name: str, rid: str, clients: Any = None) -> None:
        self.name, self.rid, self._clients = name, rid, clients
        self.scopes: dict[str, FakeDataset] = {}

    def get_or_create_dataset(self, scope: str, *, name: str) -> FakeDataset:
        if scope not in self.scopes:
            self.scopes[scope] = FakeDataset(name, f"ri.dataset.{name}")
        return self.scopes[scope]

    def list_datasets(self) -> list[tuple[str, FakeDataset]]:
        return list(self.scopes.items())

    def add_dataset(self, scope: str, dataset: FakeDataset) -> None:
        self.scopes[scope] = dataset


class FakeClient:
    """Stands in for nominal.core.NominalClient's asset calls."""

    def __init__(self, *assets: FakeAsset) -> None:
        self.assets = list(assets)

    def search_assets(self, *, exact_substring: str) -> list[FakeAsset]:
        return [a for a in self.assets if exact_substring.lower() in a.name.lower()]

    def create_asset(self, name: str) -> FakeAsset:
        asset = FakeAsset(name, f"ri.asset.new{len(self.assets)}")
        self.assets.append(asset)
        return asset


@pytest.fixture
def df() -> pd.DataFrame:
    return instro.read_avro(DATA / "capture.avro")


@pytest.fixture(autouse=True)
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A config file with default, docs and staging profiles, so no real config or profile variable is read."""
    import yaml

    path = tmp_path / "nominal" / "config.yml"
    path.parent.mkdir()
    profiles = {
        n: {"base_url": f"https://{n}.test/api", "token": "secret", "workspace_rid": None}
        for n in ("default", "docs", "staging")
    }
    path.write_text(yaml.dump({"version": 2, "profiles": profiles}))
    monkeypatch.setattr("nominal.config._session._config_path", lambda: path)
    monkeypatch.setattr("nominal.config._session._chosen", None)
    for variable in ("NOMINAL_PROFILE", "PANOMINAL_PROFILE", "NOMINAL_WORKSPACE_RID"):
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    """The client that the "default" profile gives."""
    client = FakeClient()
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda profile: client)
    return client


def test_defaults(df: pd.DataFrame, client: FakeClient):
    """With no arguments, to_nominal uploads to a reused asset and dataset named nominal-pandas."""
    asset, dataset, file = df.nom.to_nominal()
    assert (asset.name, dataset.name) == ("nominal-pandas", "nominal-pandas")
    assert not file.polled  # doesn't wait by default
    pd.testing.assert_frame_equal(
        dataset.uploaded.sort_values(["timestamp", "channel"], ignore_index=True),
        df.sort_values(["timestamp", "channel"], ignore_index=True),
    )
    again, dataset_again, _ = df.nom.to_nominal()
    assert again is asset and dataset_again is dataset  # reused, not recreated


def test_names(df: pd.DataFrame, client: FakeClient):
    """Naming an asset and dataset finds the asset by exact name and creates the dataset on it."""
    existing = FakeAsset("ECG record 119", "ri.asset.ecg")
    client.assets += [existing, FakeAsset("ECG record 119 (old)", "ri.asset.old")]
    asset, dataset, _ = df.nom.to_nominal(asset="ECG record 119", dataset="lead II", tags={"test_id": "42"})
    assert asset is existing  # exact name, not the substring match
    assert dataset.name == "lead II" and existing.scopes["lead II"] is dataset
    assert dataset.tags == {"test_id": "42"}


def test_ambiguous_asset_name(df: pd.DataFrame, client: FakeClient):
    """An asset name shared by several assets raises an error listing them."""
    client.assets += [FakeAsset("rig", "ri.asset.1"), FakeAsset("rig", "ri.asset.2")]
    with pytest.raises(ValueError, match="2 assets are named 'rig'"):
        df.nom.to_nominal(asset="rig")


def test_existing_asset_uses_its_connection(df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch):
    """An Asset object is used with its own connection, without consulting any profile."""
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda profile: pytest.fail("used the profile"))
    mine = FakeAsset("mine", "ri.asset.mine")
    asset, dataset, _ = df.nom.to_nominal(asset=mine)
    assert asset is mine and dataset.name == "nominal-pandas"


def test_existing_dataset_goes_on_default_asset(df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch):
    """A Dataset object is attached to the default asset using its own connection."""
    client = FakeClient()
    mine = FakeDataset("mine", "ri.dataset.mine")
    monkeypatch.setattr("nominal.pandas._resolve.client_of", lambda obj: client if obj is mine else None)
    asset, dataset, _ = df.nom.to_nominal(dataset=mine)
    assert asset.name == "nominal-pandas" and dataset is mine
    assert asset.scopes["mine"] is mine


def test_existing_dataset_name_taken(df: pd.DataFrame):
    """A Dataset whose name is taken on the asset is attached under its RID."""
    theirs = FakeDataset("nominal-pandas", "ri.dataset.theirs")
    asset = FakeAsset("rig", "ri.asset.rig")
    ours = asset.get_or_create_dataset("nominal-pandas", name="nominal-pandas")
    df.nom.to_nominal(asset=asset, dataset=theirs)
    assert asset.scopes == {"nominal-pandas": ours, "ri.dataset.theirs": theirs}


def test_existing_dataset_already_on_asset(df: pd.DataFrame):
    """A Dataset already on the asset isn't added a second time."""
    mine = FakeDataset("mine", "ri.dataset.mine")
    asset = FakeAsset("rig", "ri.asset.rig")
    asset.scopes["data"] = mine
    df.nom.to_nominal(asset=asset, dataset=mine)
    assert asset.scopes == {"data": mine}  # not added again under another name


def test_wait(df: pd.DataFrame, client: FakeClient):
    """wait=True polls until Nominal has ingested the file."""
    _, _, file = df.nom.to_nominal(wait=True)
    assert file.polled


def test_uses_active_profile(df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch):
    """With no profile argument, to_nominal uses the active profile's client."""
    clients = {"default": FakeClient(), "docs": FakeClient()}
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda profile: clients[profile])
    monkeypatch.setenv("NOMINAL_PROFILE", "docs")
    df.nom.to_nominal()
    assert [a.name for a in clients["docs"].assets] == ["nominal-pandas"] and not clients["default"].assets


def test_profile(df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch):
    """profile= selects the client to upload with."""
    clients = {"default": FakeClient(), "staging": FakeClient()}
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda profile: clients[profile])
    df.nom.to_nominal(profile="staging")
    assert [a.name for a in clients["staging"].assets] == ["nominal-pandas"] and not clients["default"].assets


def test_profile_and_client(df: pd.DataFrame, client: FakeClient):
    """Passing both profile and client is an error."""
    with pytest.raises(ValueError, match="not both"):
        df.nom.to_nominal(profile="default", client=client)  # type: ignore[arg-type]


def test_asset_object_cannot_be_combined_with_a_client_or_profile(df: pd.DataFrame, client: FakeClient):
    """An Asset already has its own connection, so passing client= or profile= with one is an error."""
    mine = FakeAsset("mine", "ri.asset.mine")
    with pytest.raises(ValueError, match="already has its own connection"):
        df.nom.to_nominal(asset=mine, client=client)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="already has its own connection"):
        df.nom.to_nominal(asset=mine, profile="staging")
    assert mine.scopes == {}


def test_missing_fastavro_creates_nothing_in_nominal(
    df: pd.DataFrame, client: FakeClient, monkeypatch: pytest.MonkeyPatch
):
    """If the Avro file can't be written (no fastavro), to_nominal fails before creating an asset or dataset."""
    from nominal.io import MissingDependencyError

    def no_avro(self: Any, path: Any) -> Any:
        raise MissingDependencyError("Avro needs fastavro: pip install nominal[avro]")

    monkeypatch.setattr("nominal.pandas.NomAccessor.to_avro", no_avro)
    with pytest.raises(MissingDependencyError, match=r"nominal\[avro\]"):
        df.nom.to_nominal()
    assert client.assets == []
