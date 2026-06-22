from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagger_client.models import SamplerType

from nominal.core.exceptions import NominalIngestError
from nominal.ouster import spatial

_WORKSPACE_LOCATOR = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_ORG_UUID = "11111111-2222-3333-4444-555555555555"
_FAKE_S3_PATH = "s3://nominal-uploads/ws-locator/ouster.csv"
_PRESIGNED_URL = "https://presigned.example/data.csv?sig=abc"


@pytest.fixture
def make_clients() -> Callable[..., tuple[MagicMock, MagicMock]]:
    """Factory for a mock _ClientsBunch wired with a resolved workspace and a presigned download URL."""

    def _make(
        *,
        workspace_locator: str = _WORKSPACE_LOCATOR,
        org_uuid: str = _ORG_UUID,
    ) -> tuple[MagicMock, MagicMock]:
        clients = MagicMock()
        workspace = MagicMock()
        workspace.rid = f"ri.scout.cerulean-staging.workspace.{workspace_locator}"
        workspace.org = f"ri.scout.cerulean-staging.organization.{org_uuid}"
        clients.resolve_workspace.return_value = workspace
        clients.auth_header = "Bearer test-token-123"
        clients._api_base_url = "https://api.nominal.test"
        clients.header_provider = None
        clients.upload.sign_download.return_value = MagicMock(url=_PRESIGNED_URL)
        return clients, workspace

    return _make


@dataclass
class _DaggerMocks:
    upload_multipart_file: MagicMock
    authenticated_client: MagicMock
    put_object_space: MagicMock
    post_import: MagicMock


@pytest.fixture
def dagger_mocks() -> Iterator[_DaggerMocks]:
    """Patch the upload + dagger integration so create_dagger_model runs without network."""
    with (
        patch.object(spatial, "upload_multipart_file", return_value=_FAKE_S3_PATH) as upload,
        patch.object(spatial, "AuthenticatedClient") as authenticated_client,
        patch("nominal.ouster.spatial.put_object_space.sync_detailed", return_value=MagicMock(status_code=200)) as put,
        patch("nominal.ouster.spatial.post_import.sync_detailed", return_value=MagicMock(status_code=202)) as post,
    ):
        yield _DaggerMocks(upload, authenticated_client, put, post)


def test_create_dagger_model_returns_uuid_and_source_handle(
    tmp_path: Path,
    make_clients: Callable[..., tuple[MagicMock, MagicMock]],
    dagger_mocks: _DaggerMocks,
) -> None:
    """create_dagger_model uploads the CSV, imports it, and returns the model uuid + source handle."""
    csv_path = tmp_path / "ouster.csv"
    csv_path.write_text("x,y,z,time,reflectivity\n1.0,2.0,3.0,1700000000.0,42\n")
    clients, workspace = make_clients()
    nominal_client = MagicMock()
    nominal_client._clients = clients

    model = spatial.create_dagger_model(nominal_client, csv_path)

    assert model.source_handle == _FAKE_S3_PATH
    # dagger_uuid is the model_uuid passed to the import POST
    assert model.dagger_uuid == str(dagger_mocks.post_import.call_args.kwargs["model_uuid"])
    dagger_mocks.upload_multipart_file.assert_called_once()
    import_request = dagger_mocks.post_import.call_args.kwargs["body"]
    assert import_request.source_uri == _PRESIGNED_URL
    assert import_request.columns.geometry == [0, 1, 2]
    assert import_request.columns.real == [3]
    assert import_request.columns.int_ == [4]

    # Assert that the PUT and import calls received the correct UUIDs derived from the workspace.
    expected_object_space = spatial._extract_rid_locator_uuid(workspace.rid)
    expected_tenant = spatial._extract_rid_locator_uuid(workspace.org)
    put_kwargs = dagger_mocks.put_object_space.call_args.kwargs
    assert put_kwargs["id"] == expected_object_space
    assert put_kwargs["tenant"] == expected_tenant
    post_kwargs = dagger_mocks.post_import.call_args.kwargs
    assert post_kwargs["object_space"] == expected_object_space
    assert post_kwargs["tenant"] == expected_tenant


def test_create_dagger_model_raises_on_missing_file() -> None:
    """create_dagger_model raises FileNotFoundError when the CSV path does not exist."""
    with pytest.raises(FileNotFoundError):
        spatial.create_dagger_model(MagicMock(), "/no/such/path.csv")


def test_create_dagger_model_propagates_dagger_failure(
    tmp_path: Path,
    make_clients: Callable[..., tuple[MagicMock, MagicMock]],
    dagger_mocks: _DaggerMocks,
) -> None:
    """Raises NominalIngestError when the dagger import endpoint returns a non-202 status."""
    csv_path = tmp_path / "ouster.csv"
    csv_path.write_text("x,y,z,t\n1,2,3,4\n")
    clients, _ = make_clients()
    nominal_client = MagicMock()
    nominal_client._clients = clients
    dagger_mocks.post_import.return_value = MagicMock(status_code=500, content=b"upstream broken")

    with pytest.raises(NominalIngestError, match="Dagger POST"):
        spatial.create_dagger_model(nominal_client, csv_path)


def test_create_dagger_model_propagates_put_failure(
    tmp_path: Path,
    make_clients: Callable[..., tuple[MagicMock, MagicMock]],
    dagger_mocks: _DaggerMocks,
) -> None:
    """Raises NominalIngestError when the dagger PUT object-space endpoint returns a non-2xx status."""
    csv_path = tmp_path / "ouster.csv"
    csv_path.write_text("x,y,z,t\n1,2,3,4\n")
    clients, _ = make_clients()
    nominal_client = MagicMock()
    nominal_client._clients = clients
    dagger_mocks.put_object_space.return_value = MagicMock(status_code=500, content=b"object space broken")

    with pytest.raises(NominalIngestError, match="Dagger PUT"):
        spatial.create_dagger_model(nominal_client, csv_path)


def test_extract_rid_locator_uuid_from_full_rid() -> None:
    """Parses the trailing locator segment of a RID as a UUID."""
    rid = "ri.scout.cerulean-staging.organization.dddddddd-4444-4444-4444-dddddddddddd"
    assert spatial._extract_rid_locator_uuid(rid) == uuid.UUID("dddddddd-4444-4444-4444-dddddddddddd")


def test_extract_rid_locator_uuid_rejects_non_uuid_locator() -> None:
    """Raises when the RID's locator segment is not a valid UUID."""
    with pytest.raises(ValueError):
        spatial._extract_rid_locator_uuid("ri.scout.cerulean-staging.organization.not-a-uuid")


def test_find_geometry_indices_basic() -> None:
    """Locates x/y/z columns at their header positions."""
    assert spatial._find_geometry_indices(["x", "y", "z"]) == [0, 1, 2]


def test_find_geometry_indices_reordered_and_case_insensitive() -> None:
    """Finds x/y/z regardless of order or case, returning indices in x, y, z order."""
    assert spatial._find_geometry_indices(["time", "Y", "X", "Z", "intensity"]) == [2, 1, 3]


def test_find_geometry_indices_missing_raises() -> None:
    """Raises when any of the required x/y/z columns is absent."""
    with pytest.raises(ValueError, match="missing required point-cloud columns"):
        spatial._find_geometry_indices(["x", "y", "intensity"])


@pytest.mark.parametrize(
    "value, expected",
    [
        ("", "string"),
        ("0", "int"),
        ("-42", "int"),
        ("3.14", "real"),
        ("1e9", "real"),
        ("abc", "string"),
    ],
)
def test_classify(value: str, expected: str) -> None:
    """Classifies a single cell value as int, real, or string."""
    assert spatial._classify(value) == expected


@pytest.mark.parametrize(
    "values, expected",
    [
        ([], "string"),
        (["", "", ""], "string"),
        (["1", "2", "3"], "int"),
        (["1", "2.0", "3"], "real"),
        (["1", "abc"], "string"),
        (["1", "", "2.5"], "real"),
    ],
)
def test_classify_column(values: list[str], expected: str) -> None:
    """Picks the most permissive type covering every non-empty value in a column."""
    assert spatial._classify_column(values) == expected


def test_read_csv_header_and_samples(tmp_path: Path) -> None:
    """Returns the header line and the data rows of a CSV."""
    csv_path = tmp_path / "pc.csv"
    csv_path.write_text("x,y,z,time,intensity\n1,2,3,1700000000.0,42\n4,5,6,1700000001.0,43\n")
    header, samples = spatial._read_csv_header_and_samples(csv_path)
    assert header == "x,y,z,time,intensity"
    assert samples == ["1,2,3,1700000000.0,42", "4,5,6,1700000001.0,43"]


def test_read_csv_header_and_samples_caps_at_n_samples(tmp_path: Path) -> None:
    """Reads at most n_samples data rows even when the file has more."""
    csv_path = tmp_path / "big.csv"
    csv_path.write_text("x,y,z\n" + "\n".join(f"{i},{i},{i}" for i in range(50)) + "\n")
    header, samples = spatial._read_csv_header_and_samples(csv_path, n_samples=10)
    assert header == "x,y,z"
    assert len(samples) == 10
    assert samples[0] == "0,0,0"
    assert samples[-1] == "9,9,9"


def test_read_csv_header_and_samples_empty_raises(tmp_path: Path) -> None:
    """Raises when the CSV file is completely empty."""
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("")
    with pytest.raises(ValueError, match="CSV is empty"):
        spatial._read_csv_header_and_samples(csv_path)


def test_build_archetype_attaches_reductions_per_attribute_type() -> None:
    """Attaches Min/Max(/Mean) reductions to int/real attributes and none to strings."""
    # Reductions let the renderer sample attributes at coarse LODs for ramp coloring +
    # ValueRange filtering; Mean is only valid for Real, not Int, attributes.
    header = "x,y,z,intensity,laser_power,label"
    samples = ["1.0,2.0,3.0,42,7.5,wall", "4.0,5.0,6.0,99,8.0,floor"]
    _columns, archetype = spatial._build_archetype(header, samples)
    by_name = {a.header.name: a for a in archetype.attributes}
    assert sorted(by_name["intensity"].reductions, key=str) == sorted([SamplerType.MIN, SamplerType.MAX], key=str)
    assert sorted(by_name["laser_power"].reductions, key=str) == sorted(
        [SamplerType.MIN, SamplerType.MAX, SamplerType.MEAN], key=str
    )
    assert by_name["label"].reductions == []


def test_build_archetype_classifies_ouster_columns() -> None:
    """Classifies the columns of a convert_ouster_dataset CSV into geometry/real/int."""
    header = "x,y,z,time,reflectivity,signal,near_infrared"
    sample = ["1.0,2.0,3.0,1700000000.0,42,17,9"]

    columns, archetype = spatial._build_archetype(header, sample)

    assert columns.geometry == [0, 1, 2]
    assert columns.real == [3]  # 1700000000.0 parses as real
    assert columns.int_ == [4, 5, 6]  # 42, 17, 9 parse as int
    assert columns.string == []
    assert columns.bool_ == [] and columns.normal == [] and columns.rgb == []
    # Geometry columns are excluded from archetype attributes.
    assert [a.header.name for a in archetype.attributes] == ["time", "reflectivity", "signal", "near_infrared"]


def test_build_archetype_defaults_unsampled_columns_to_string() -> None:
    """Defaults non-geometry columns to string when there are no data rows to sample."""
    columns, _archetype = spatial._build_archetype("x,y,z,label", [])
    assert columns.geometry == [0, 1, 2]
    assert columns.string == [3]


def test_build_archetype_promotes_int_to_real_across_rows() -> None:
    """Promotes a column to real when a later row is a float, even if the first row looks like an int."""
    header = "x,y,z,stress"
    samples = ["1.0,2.0,3.0,1", "1.0,2.0,3.0,0.998"]
    columns, _archetype = spatial._build_archetype(header, samples)
    assert columns.real == [3]
    assert columns.int_ == []


def test_build_archetype_demotes_to_string_on_any_non_numeric_value() -> None:
    """Demotes a column to string when any sampled value is non-numeric."""
    header = "x,y,z,label"
    samples = ["1.0,2.0,3.0,1", "1.0,2.0,3.0,WALL-OUTER"]
    columns, _archetype = spatial._build_archetype(header, samples)
    assert columns.string == [3]
    assert columns.int_ == [] and columns.real == []


def test_build_archetype_pads_short_rows() -> None:
    """Pads rows with fewer fields than the header instead of crashing."""
    header = "x,y,z,a,b"
    samples = ["1,2,3,42", "4,5,6,43,extra"]
    columns, _archetype = spatial._build_archetype(header, samples)
    assert columns.geometry == [0, 1, 2]
    assert columns.int_ == [3]  # populated in both rows
    assert columns.string == [4]  # empty in the first row → string default


def test_build_archetype_override_forces_real() -> None:
    """Lets an override force a column to real even when inference would say int."""
    columns, _ = spatial._build_archetype("x,y,z,stress", ["1.0,2.0,3.0,1"], {"stress": "real"})
    assert columns.real == [3]
    assert columns.int_ == []


def test_build_archetype_override_forces_string() -> None:
    """Lets an override force a numeric-looking column to string."""
    columns, _ = spatial._build_archetype("x,y,z,layer", ["1.0,2.0,3.0,7"], {"layer": "string"})
    assert columns.string == [3]
    assert columns.int_ == []


def test_build_archetype_override_ignores_geometry_columns() -> None:
    """Ignores overrides naming x/y/z, which always remain geometry."""
    overrides = {"x": "real", "y": "real", "z": "real", "extra": "real"}
    columns, _ = spatial._build_archetype("x,y,z,extra", ["1.0,2.0,3.0,42"], overrides)
    assert columns.geometry == [0, 1, 2]
    assert columns.real == [3]


def test_build_archetype_override_unknown_column_raises() -> None:
    """Raises when an override names a column that is not in the header."""
    with pytest.raises(ValueError, match="not in CSV header"):
        spatial._build_archetype("x,y,z,stress", ["1.0,2.0,3.0,1"], {"strss": "real"})


def test_build_archetype_override_invalid_type_raises() -> None:
    """Raises when an override value is not one of int/real/string."""
    with pytest.raises(ValueError, match="must be one of"):
        spatial._build_archetype("x,y,z,stress", ["1.0,2.0,3.0,1"], {"stress": "float64"})  # type: ignore[dict-item]


def test_build_archetype_override_falls_through_to_inference() -> None:
    """Applies overrides only to the named columns, inferring the rest."""
    header = "x,y,z,stress,idx"
    samples = ["1.0,2.0,3.0,1,0", "1.0,2.0,3.0,0.5,1"]
    columns, _ = spatial._build_archetype(header, samples, {"stress": "real"})
    assert columns.real == [3]
    assert columns.int_ == [4]


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.gov.nominal.io",
        "https://api.gov.nominal.io/",
        "https://api.gov.nominal.io/api",
        "https://api.gov.nominal.io/api/",
    ],
)
def test_dagger_base_url_resolves_proxy_path(base_url: str) -> None:
    """Resolves the dagger proxy URL regardless of a trailing slash or /api suffix."""
    clients = MagicMock()
    clients._api_base_url = base_url
    assert spatial._dagger_base_url(clients) == "https://api.gov.nominal.io/api/dagger"
