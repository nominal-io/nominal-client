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


def test_read_first_two_csv_lines(tmp_path: Path):
    csv = tmp_path / "pc.csv"
    csv.write_text("x,y,z,time,intensity\n1,2,3,1700000000.0,42\n4,5,6,1700000001.0,43\n")
    header, first_data = spatial._read_first_two_csv_lines(csv)
    assert header == "x,y,z,time,intensity"
    assert first_data == "1,2,3,1700000000.0,42"


def test_read_first_two_csv_lines_empty_raises(tmp_path: Path):
    csv = tmp_path / "empty.csv"
    csv.write_text("")
    with pytest.raises(ValueError, match="CSV is empty"):
        spatial._read_first_two_csv_lines(csv)


def test_build_archetype_ouster_shape():
    # Mirrors the Ouster CSV produced by convert_ouster_dataset.
    header = "x,y,z,time,reflectivity,signal,near_infrared"
    data = "1.0,2.0,3.0,1700000000.0,42,17,9"

    columns, archetype = spatial._build_archetype(header, data)

    assert columns.geometry == [0, 1, 2]
    # 42, 17, 9 parse as int. 1700000000.0 parses as real.
    assert columns.real == [3]
    assert columns.int_ == [4, 5, 6]
    assert columns.string == []
    assert columns.bool_ == [] and columns.normal == [] and columns.rgb == []

    # Geometry columns are excluded from archetype attributes.
    names = [a.header.name for a in archetype.attributes]
    assert names == ["time", "reflectivity", "signal", "near_infrared"]


def test_build_archetype_missing_data_row_classifies_as_string():
    columns, archetype = spatial._build_archetype("x,y,z,label", "")
    # No data row → label classifies as string by virtue of empty value.
    assert columns.geometry == [0, 1, 2]
    assert columns.string == [3]


def test_dagger_base_url_appends_dagger():
    clients = MagicMock()
    clients._api_base_url = "https://api.gov.nominal.io/api"
    assert spatial._dagger_base_url(clients) == "https://api.gov.nominal.io/api/dagger"


def test_dagger_base_url_strips_trailing_slash():
    clients = MagicMock()
    clients._api_base_url = "https://api.gov.nominal.io/api/"
    assert spatial._dagger_base_url(clients) == "https://api.gov.nominal.io/api/dagger"


def _make_clients_mock(
    *, workspace_locator: str = "ws-locator", org_uuid: str = "11111111-2222-3333-4444-555555555555"
):
    clients = MagicMock()
    workspace = MagicMock()
    workspace.rid = f"ri.scout.cerulean-staging.workspace.{workspace_locator}"
    workspace.id = workspace_locator
    workspace.org = f"ri.scout.cerulean-staging.organization.{org_uuid}"
    clients.resolve_workspace.return_value = workspace
    clients.auth_header = "Bearer test-token-123"
    clients._api_base_url = "https://api.nominal.test/api"
    clients.header_provider = None
    presign_response = MagicMock()
    presign_response.json.return_value = {
        "url": "https://presigned.example/data.csv?sig=abc",
        "expiresAt": "2026-05-12T16:00:00Z",
    }
    clients.spatial._request.return_value = presign_response
    clients.spatial._uri = "https://api.nominal.test/api"
    return clients, workspace


def test_upload_point_cloud_full_flow(tmp_path: Path):
    csv = tmp_path / "ouster.csv"
    csv.write_text("x,y,z,time,reflectivity\n1.0,2.0,3.0,1700000000.0,42\n")

    clients, workspace = _make_clients_mock()
    nominal_client = MagicMock()
    nominal_client._clients = clients

    s3_path = "s3://nominal-uploads/ws-locator/ouster.csv"

    # Spatial.create returns a real spatial RID string. The SDK's CreateSpatialRequest
    # doesn't yet have dagger_uuid/source_handle kwargs (waiting on the regenerated SDK
    # post-scout-#13407), so patch the constructor to swallow kwargs.
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

    # Presign request shape.
    presign_call = clients.spatial._request.call_args
    assert presign_call.args[0] == "POST"
    assert presign_call.args[1].endswith("/signed-urls/v1/download")
    assert presign_call.kwargs["json"] == {"path": s3_path}

    # Dagger client built with proxy URL and bare token (no "Bearer " prefix).
    auth_client_cls.assert_called_once()
    assert auth_client_cls.call_args.kwargs["base_url"] == "https://api.nominal.test/api/dagger"
    assert auth_client_cls.call_args.kwargs["token"] == "test-token-123"

    # ensureObjectSpace got called with workspace.id and the org UUID.
    put_kwargs = put_mock.call_args.kwargs
    assert put_kwargs["id"] == workspace.id
    assert put_kwargs["tenant"] == uuid.UUID("11111111-2222-3333-4444-555555555555")

    # post_import got the right args. tenant is org UUID, object_space is workspace.id.
    post_kwargs = post_mock.call_args.kwargs
    assert post_kwargs["tenant"] == uuid.UUID("11111111-2222-3333-4444-555555555555")
    assert post_kwargs["object_space"] == workspace.id
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
