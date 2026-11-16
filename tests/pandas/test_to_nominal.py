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


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    """The client that the "default" profile gives."""
    client = FakeClient()
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda profile: client)
    return client


def test_defaults(df: pd.DataFrame, client: FakeClient):
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
    existing = FakeAsset("ECG record 119", "ri.asset.ecg")
    client.assets += [existing, FakeAsset("ECG record 119 (old)", "ri.asset.old")]
    asset, dataset, _ = df.nom.to_nominal(asset="ECG record 119", dataset="lead II", tags={"test_id": "42"})
    assert asset is existing  # exact name, not the substring match
    assert dataset.name == "lead II" and existing.scopes["lead II"] is dataset
    assert dataset.tags == {"test_id": "42"}


def test_ambiguous_asset_name(df: pd.DataFrame, client: FakeClient):
    client.assets += [FakeAsset("rig", "ri.asset.1"), FakeAsset("rig", "ri.asset.2")]
    with pytest.raises(ValueError, match="2 assets are named 'rig'"):
        df.nom.to_nominal(asset="rig")


def test_existing_asset_uses_its_connection(df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda profile: pytest.fail("used the profile"))
    mine = FakeAsset("mine", "ri.asset.mine")
    asset, dataset, _ = df.nom.to_nominal(asset=mine)
    assert asset is mine and dataset.name == "nominal-pandas"


def test_existing_dataset_goes_on_default_asset(df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch):
    client = FakeClient()
    mine = FakeDataset("mine", "ri.dataset.mine")
    monkeypatch.setattr("nominal.pandas._resolve.client_of", lambda obj: client if obj is mine else None)
    asset, dataset, _ = df.nom.to_nominal(dataset=mine)
    assert asset.name == "nominal-pandas" and dataset is mine
    assert asset.scopes["mine"] is mine


def test_existing_dataset_name_taken(df: pd.DataFrame):
    theirs = FakeDataset("nominal-pandas", "ri.dataset.theirs")
    asset = FakeAsset("rig", "ri.asset.rig")
    ours = asset.get_or_create_dataset("nominal-pandas", name="nominal-pandas")
    df.nom.to_nominal(asset=asset, dataset=theirs)
    assert asset.scopes == {"nominal-pandas": ours, "ri.dataset.theirs": theirs}


def test_existing_dataset_already_on_asset(df: pd.DataFrame):
    mine = FakeDataset("mine", "ri.dataset.mine")
    asset = FakeAsset("rig", "ri.asset.rig")
    asset.scopes["data"] = mine
    df.nom.to_nominal(asset=asset, dataset=mine)
    assert asset.scopes == {"data": mine}  # not added again under another name


def test_wait(df: pd.DataFrame, client: FakeClient):
    _, _, file = df.nom.to_nominal(wait=True)
    assert file.polled


def test_profile(df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch):
    clients = {"default": FakeClient(), "staging": FakeClient()}
    monkeypatch.setattr("nominal.core.NominalClient.from_profile", lambda profile: clients[profile])
    df.nom.to_nominal(profile="staging")
    assert [a.name for a in clients["staging"].assets] == ["nominal-pandas"] and not clients["default"].assets


def test_profile_and_client(df: pd.DataFrame, client: FakeClient):
    with pytest.raises(ValueError, match="not both"):
        df.nom.to_nominal(profile="default", client=client)  # type: ignore[arg-type]
