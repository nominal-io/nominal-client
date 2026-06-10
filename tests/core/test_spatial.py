from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nominal.core import spatial


def test_extract_rid_locator_uuid_from_full_rid():
    rid = "ri.scout.cerulean-staging.organization.dddddddd-4444-4444-4444-dddddddddddd"
    assert spatial._extract_rid_locator_uuid(rid) == uuid.UUID("dddddddd-4444-4444-4444-dddddddddddd")


def test_extract_rid_locator_uuid_rejects_non_uuid_locator():
    with pytest.raises(ValueError):
        spatial._extract_rid_locator_uuid("ri.scout.cerulean-staging.organization.not-a-uuid")


def test_find_geometry_indices_basic():
    assert spatial._find_geometry_indices(["x", "y", "z"]) == [0, 1, 2]


def test_find_geometry_indices_reordered_and_case_insensitive():
    assert spatial._find_geometry_indices(["time", "Y", "X", "Z", "intensity"]) == [2, 1, 3]


def test_find_geometry_indices_missing_raises():
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
def test_classify(value, expected):
    assert spatial._classify(value) == expected


def test_read_csv_header_and_samples(tmp_path: Path):
    csv = tmp_path / "pc.csv"
    csv.write_text("x,y,z,time,intensity\n1,2,3,1700000000.0,42\n4,5,6,1700000001.0,43\n")
    header, samples = spatial._read_csv_header_and_samples(csv)
    assert header == "x,y,z,time,intensity"
    assert samples == ["1,2,3,1700000000.0,42", "4,5,6,1700000001.0,43"]


def test_read_csv_header_and_samples_caps_at_n_samples(tmp_path: Path):
    csv = tmp_path / "big.csv"
    csv.write_text("x,y,z\n" + "\n".join(f"{i},{i},{i}" for i in range(50)) + "\n")
    header, samples = spatial._read_csv_header_and_samples(csv, n_samples=10)
    assert header == "x,y,z"
    assert len(samples) == 10
    assert samples[0] == "0,0,0"
    assert samples[-1] == "9,9,9"


def test_read_csv_header_and_samples_empty_raises(tmp_path: Path):
    csv = tmp_path / "empty.csv"
    csv.write_text("")
    with pytest.raises(ValueError, match="CSV is empty"):
        spatial._read_csv_header_and_samples(csv)


def test_build_archetype_attaches_reductions_per_attribute_type():
    # Without reductions on Int/Real attributes, the downstream renderer
    # (volumesight) can't sample them at coarse LODs — so Geometry
    # coloring falls back to solid white and ValueRange filter is a
    # no-op. Min + Max satisfy `VolumetricFilter::ValueRange`'s two-sided
    # filter; Mean rounds out the common aggregations for Real.
    from dagger_client.models import SamplerType

    header = "x,y,z,intensity,laser_power,label"
    samples = ["1.0,2.0,3.0,42,7.5,wall", "4.0,5.0,6.0,99,8.0,floor"]
    _columns, archetype = spatial._build_archetype(header, samples)
    by_name = {a.header.name: a for a in archetype.attributes}
    # intensity is int → Min/Max only (Mean is not valid with Int attributes).
    assert sorted(by_name["intensity"].reductions, key=str) == sorted([SamplerType.MIN, SamplerType.MAX], key=str)
    # laser_power is real (mixed 7.5 / 8.0) → Min/Max/Mean.
    assert sorted(by_name["laser_power"].reductions, key=str) == sorted(
        [SamplerType.MIN, SamplerType.MAX, SamplerType.MEAN], key=str
    )
    # label is string → no scalar aggregation makes sense.
    assert by_name["label"].reductions == []


def test_build_archetype_ouster_shape():
    # Mirrors the Ouster CSV produced by convert_ouster_dataset.
    header = "x,y,z,time,reflectivity,signal,near_infrared"
    sample = ["1.0,2.0,3.0,1700000000.0,42,17,9"]

    columns, archetype = spatial._build_archetype(header, sample)

    assert columns.geometry == [0, 1, 2]
    # 42, 17, 9 parse as int. 1700000000.0 parses as real.
    assert columns.real == [3]
    assert columns.int_ == [4, 5, 6]
    assert columns.string == []
    assert columns.bool_ == [] and columns.normal == [] and columns.rgb == []

    # Geometry columns are excluded from archetype attributes.
    names = [a.header.name for a in archetype.attributes]
    assert names == ["time", "reflectivity", "signal", "near_infrared"]


def test_build_archetype_no_samples_classifies_as_string():
    columns, _archetype = spatial._build_archetype("x,y,z,label", [])
    # No data rows → defaults to string for non-geometry columns.
    assert columns.geometry == [0, 1, 2]
    assert columns.string == [3]


def test_build_archetype_promotes_int_to_real_across_rows():
    # `stress` is integer-valued in the first row but float in the second.
    # The legacy single-row classifier mis-tagged this as int, which broke
    # downstream dagger ingest. Sampling multiple rows promotes it to real.
    header = "x,y,z,stress"
    samples = ["1.0,2.0,3.0,1", "1.0,2.0,3.0,0.998"]
    columns, _archetype = spatial._build_archetype(header, samples)
    assert columns.real == [3]
    assert columns.int_ == []


def test_build_archetype_any_string_value_forces_string():
    # A numeric-looking first row followed by a non-numeric row demotes the
    # column to string rather than failing later on the server.
    header = "x,y,z,label"
    samples = ["1.0,2.0,3.0,1", "1.0,2.0,3.0,WALL-OUTER"]
    columns, _archetype = spatial._build_archetype(header, samples)
    assert columns.string == [3]
    assert columns.int_ == [] and columns.real == []


def test_build_archetype_handles_short_rows():
    # Rows that have fewer commas than the header (trailing-empty fields)
    # should be padded rather than crash.
    header = "x,y,z,a,b"
    samples = ["1,2,3,42", "4,5,6,43,extra"]
    columns, _archetype = spatial._build_archetype(header, samples)
    assert columns.geometry == [0, 1, 2]
    # First row leaves column b empty (→ string default), second populates it.
    assert columns.int_ == [3]
    assert columns.string == [4]


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
def test_classify_column(values, expected):
    assert spatial._classify_column(values) == expected


def test_build_archetype_override_promotes_int_to_real():
    # First-row inference would tag `stress` as int; the override forces real
    # without the caller having to feed in more sample rows.
    header = "x,y,z,stress"
    samples = ["1.0,2.0,3.0,1"]
    columns, _ = spatial._build_archetype(header, samples, {"stress": "real"})
    assert columns.real == [3]
    assert columns.int_ == []


def test_build_archetype_override_demotes_numeric_to_string():
    header = "x,y,z,layer"
    samples = ["1.0,2.0,3.0,7"]
    columns, _ = spatial._build_archetype(header, samples, {"layer": "string"})
    assert columns.string == [3]
    assert columns.int_ == []


def test_build_archetype_override_ignores_geometry_columns():
    # Including x/y/z in the override is harmless — they always stay geometry.
    header = "x,y,z,extra"
    samples = ["1.0,2.0,3.0,42"]
    columns, _ = spatial._build_archetype(header, samples, {"x": "real", "y": "real", "z": "real", "extra": "real"})
    assert columns.geometry == [0, 1, 2]
    assert columns.real == [3]


def test_build_archetype_override_unknown_column_raises():
    header = "x,y,z,stress"
    with pytest.raises(ValueError, match="not in CSV header"):
        spatial._build_archetype(header, ["1.0,2.0,3.0,1"], {"strss": "real"})


def test_build_archetype_override_invalid_type_raises():
    header = "x,y,z,stress"
    with pytest.raises(ValueError, match="must be one of"):
        spatial._build_archetype(header, ["1.0,2.0,3.0,1"], {"stress": "float64"})


def test_build_archetype_partial_override_falls_through_to_inference():
    # `stress` overridden to real; `idx` left to auto-inference (sees an int).
    header = "x,y,z,stress,idx"
    samples = ["1.0,2.0,3.0,1,0", "1.0,2.0,3.0,0.5,1"]
    columns, _ = spatial._build_archetype(header, samples, {"stress": "real"})
    assert columns.real == [3]
    assert columns.int_ == [4]


def test_scan_pattern_enum_rejects_invalid_value():
    with pytest.raises(ValueError, match="Invalid scan_pattern"):
        spatial._scan_pattern_enum("SIDEWAYS")


def test_dagger_base_url_appends_dagger():
    clients = MagicMock()
    clients._api_base_url = "https://api.gov.nominal.io"
    assert spatial._dagger_base_url(clients) == "https://api.gov.nominal.io/api/dagger"


def test_dagger_base_url_strips_trailing_slash():
    clients = MagicMock()
    clients._api_base_url = "https://api.gov.nominal.io/"
    assert spatial._dagger_base_url(clients) == "https://api.gov.nominal.io/api/dagger"


def test_dagger_base_url_strips_trailing_api():
    # NominalClient.create's docstring example uses "https://host/api"; the
    # proxy is at /api/dagger regardless, so strip /api before appending.
    clients = MagicMock()
    clients._api_base_url = "https://api.gov.nominal.io/api"
    assert spatial._dagger_base_url(clients) == "https://api.gov.nominal.io/api/dagger"


def test_dagger_base_url_strips_trailing_api_with_slash():
    clients = MagicMock()
    clients._api_base_url = "https://api.gov.nominal.io/api/"
    assert spatial._dagger_base_url(clients) == "https://api.gov.nominal.io/api/dagger"


def _make_clients_mock(
    *,
    workspace_locator: str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    org_uuid: str = "11111111-2222-3333-4444-555555555555",
):
    clients = MagicMock()
    workspace = MagicMock()
    workspace.rid = f"ri.scout.cerulean-staging.workspace.{workspace_locator}"
    workspace.id = "admin"
    workspace.org = f"ri.scout.cerulean-staging.organization.{org_uuid}"
    clients.resolve_workspace.return_value = workspace
    clients.auth_header = "Bearer test-token-123"
    clients._api_base_url = "https://api.nominal.test"
    clients.header_provider = None
    presign_response = MagicMock()
    presign_response.url = "https://presigned.example/data.csv?sig=abc"
    clients.upload.sign_download.return_value = presign_response
    return clients, workspace


def test_upload_point_cloud_full_flow(tmp_path: Path):
    csv = tmp_path / "ouster.csv"
    csv.write_text("x,y,z,time,reflectivity\n1.0,2.0,3.0,1700000000.0,42\n")

    clients, workspace = _make_clients_mock()
    nominal_client = MagicMock()
    nominal_client._clients = clients

    s3_path = "s3://nominal-uploads/ws-locator/ouster.csv"

    created_spatial = MagicMock()
    created_spatial.rid = "ri.scout.cerulean-staging.spatial.abc-123"
    clients.spatial.create.return_value = created_spatial

    put_resp = MagicMock(status_code=200)
    import_resp = MagicMock(status_code=202)

    with (
        patch.object(spatial, "upload_multipart_file", return_value=s3_path) as upload_mock,
        patch.object(spatial, "AuthenticatedClient") as auth_client_cls,
        patch("nominal.core.spatial.put_object_space.sync_detailed", return_value=put_resp) as put_mock,
        patch("nominal.core.spatial.post_import.sync_detailed", return_value=import_resp) as post_mock,
        patch(
            "nominal.core.spatial.scout_spatial_api.CreateSpatialRequest",
            side_effect=lambda **kw: kw,
        ) as create_req_mock,
    ):
        result = spatial.upload_point_cloud(
            nominal_client,
            csv,
            name="my-cloud",
            description="a test",
            labels=["lidar"],
            properties={"site": "north"},
            sensor_model="Ouster OS1-128",
            coordinate_system="ENU",
            resolution_mm=10.0,
            scan_pattern="ROTATING",
        )

    assert result == "ri.scout.cerulean-staging.spatial.abc-123"

    # Upload happened with the expected workspace.
    upload_mock.assert_called_once()
    assert upload_mock.call_args.args[1] == workspace.rid

    clients.upload.sign_download.assert_called_once()
    sign_call = clients.upload.sign_download.call_args
    assert sign_call.args[0] == "Bearer test-token-123"
    assert sign_call.args[1].path == s3_path

    # Dagger client built with proxy URL and bare token (no "Bearer " prefix).
    auth_client_cls.assert_called_once()
    assert auth_client_cls.call_args.kwargs["base_url"] == "https://api.nominal.test/api/dagger"
    assert auth_client_cls.call_args.kwargs["token"] == "test-token-123"

    # ensureObjectSpace got called with the workspace UUID and the org UUID.
    workspace_uuid = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    put_kwargs = put_mock.call_args.kwargs
    assert put_kwargs["id"] == workspace_uuid
    assert put_kwargs["tenant"] == uuid.UUID("11111111-2222-3333-4444-555555555555")

    # post_import got the right args. tenant is org UUID, object_space is the workspace UUID.
    post_kwargs = post_mock.call_args.kwargs
    assert post_kwargs["tenant"] == uuid.UUID("11111111-2222-3333-4444-555555555555")
    assert post_kwargs["object_space"] == workspace_uuid
    import_req = post_kwargs["body"]
    assert import_req.source_uri == "https://presigned.example/data.csv?sig=abc"
    assert import_req.columns.geometry == [0, 1, 2]
    assert import_req.columns.real == [3]
    assert import_req.columns.int_ == [4]

    # CreateSpatialRequest received all the metadata + the dagger UUID + source handle.
    create_kwargs = create_req_mock.call_args.kwargs
    assert create_kwargs["title"] == "my-cloud"
    assert create_kwargs["dagger_uuid"] == str(post_kwargs["model_uuid"])
    assert create_kwargs["description"] == "a test"
    assert create_kwargs["labels"] == ["lidar"]
    assert create_kwargs["properties"] == {"site": "north"}
    assert create_kwargs["marking_rids"] == []
    assert create_kwargs["workspace"] == workspace.rid
    # PointCloudMetadata carries the sensor kwargs.
    pcm = create_kwargs["type_metadata"].point_cloud
    assert pcm.sensor_model == "Ouster OS1-128"
    assert pcm.coordinate_system == "ENU"
    assert pcm.resolution_mm == 10.0
    # source_handle wraps the s3 path.
    assert create_kwargs["source_handle"].s3 == s3_path


def test_upload_point_cloud_raises_on_missing_file():
    nominal_client = MagicMock()
    with pytest.raises(FileNotFoundError):
        spatial.upload_point_cloud(nominal_client, "/no/such/path.csv")


def test_upload_point_cloud_propagates_dagger_failure(tmp_path: Path):
    csv = tmp_path / "ouster.csv"
    csv.write_text("x,y,z,t\n1,2,3,4\n")

    clients, _ = _make_clients_mock()
    nominal_client = MagicMock()
    nominal_client._clients = clients

    put_resp = MagicMock(status_code=200)
    fail_resp = MagicMock(status_code=500, content=b"upstream broken")

    with (
        patch.object(spatial, "upload_multipart_file", return_value="s3://x/y"),
        patch.object(spatial, "AuthenticatedClient"),
        patch("nominal.core.spatial.put_object_space.sync_detailed", return_value=put_resp),
        patch("nominal.core.spatial.post_import.sync_detailed", return_value=fail_resp),
        pytest.raises(RuntimeError, match="Dagger POST"),
    ):
        spatial.upload_point_cloud(nominal_client, csv)
