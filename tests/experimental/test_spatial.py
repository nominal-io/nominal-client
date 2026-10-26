from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from nominal_api import ingest_api

from nominal.core import Asset, Run
from nominal.exceptions import NominalNotFoundError
from nominal.experimental.spatial import (
    PointCloudMetadata,
    ScanPattern,
    Spatial,
    create_point_cloud_spatial,
    get_spatial,
    get_spatial_from_asset,
    get_spatial_from_run,
    list_spatials_in_asset,
    list_spatials_in_run,
)
from nominal.experimental.spatial._spatial import _PointCloudTimeMetadata
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.run.v1 import run_service_pb2
from nominal.protos.spatial.v2 import spatial_pb2
from nominal.ts import Epoch, Relative

_SPATIAL_RID = "ri.scout.x.spatial.abc"
_JOB_RID = "ri.scout.x.ingest-job.j"
_WORKSPACE_RID = "ri.scout.x.workspace.w"

_STATIC_CSV = "x,y,z,count\n0,0,0,1\n"
_TIMED_CSV = "x,y,z,t_s\n0,0,0,0\n1,1,1,45\n"


@pytest.fixture
def clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    clients.resolve_default_workspace_rid.return_value = _WORKSPACE_RID
    clients.ingest.ingest.return_value.ingest_job_rid = _JOB_RID
    clients.ingest_jobs.get_ingest_job.return_value = _raw_ingest_job()
    # The time-range write reads properties back before replacing the map.
    clients.spatial.GetSpatial.return_value = spatial_pb2.GetSpatialResponse(spatial=_raw_spatial())
    clients.spatial.UpdateSpatial.return_value = spatial_pb2.UpdateSpatialResponse(spatial=_raw_spatial())
    return clients


@pytest.fixture
def client(clients: MagicMock) -> MagicMock:
    """A NominalClient stand-in whose methods run for real against mocked services."""
    nominal_client = MagicMock()
    nominal_client._clients = clients
    return nominal_client


@pytest.fixture
def spatial(clients: MagicMock) -> Spatial:
    return Spatial(
        rid=_SPATIAL_RID,
        name="scan",
        description=None,
        labels=(),
        properties={},
        is_archived=False,
        dagger_uuid="dagger-uuid",
        metadata=PointCloudMetadata(),
        created_at=1_700_000_000_000_000_000,
        start_timestamp=None,
        end_timestamp=None,
        _clients=clients,
    )


def _raw_spatial(
    type_metadata: spatial_pb2.SpatialTypeMetadata | None = None,
    start_timestamp: Timestamp | None = None,
) -> spatial_pb2.Spatial:
    """A real spatial protobuf returned by the service."""
    return spatial_pb2.Spatial(
        rid=_SPATIAL_RID,
        title="scan",
        dagger_model_uuid="dagger-uuid",
        created_at=Timestamp(seconds=1_700_000_000),
        type_metadata=type_metadata or spatial_pb2.SpatialTypeMetadata(point_cloud=spatial_pb2.PointCloudMetadata()),
        start_timestamp=start_timestamp,
    )


def _raw_ingest_job() -> MagicMock:
    """A conjure IngestJob bean as the service would return it for a point-cloud ingest.

    `dataset_rid` is None and no files are produced: a point-cloud ingest writes into
    the spatial's own model, so the job is the only artifact it yields.
    """
    job = MagicMock()
    job.ingest_job_rid = _JOB_RID
    job.status = ingest_api.IngestJobStatus.IN_PROGRESS
    job.ingest_type = ingest_api.IngestType.POINT_CLOUD
    job.dataset_rid = None
    job.origin_files = []
    job.produced_file_count = None
    job.created_by_rid = None
    job.created_at = None
    job.start_time = None
    job.end_time = None
    return job


def _stubbed_upload(s3_path: str = "s3://bucket/scan.csv") -> object:
    """Stub the multipart upload.

    It runs a thread pool over presigned PUTs to object storage, so there is no
    way to drive it from a mock that is not worse than replacing it outright.
    """
    return patch("nominal.experimental.spatial._spatial.upload_multipart_file", return_value=s3_path)


def _csv(tmp_path: Path, text: str = _STATIC_CSV) -> Path:
    path = tmp_path / "scan.csv"
    path.write_text(text)
    return path


# --- reading ------------------------------------------------------------------


def test_get_spatial_returns_typed_metadata_and_nanosecond_bounds(clients: MagicMock, client: MagicMock) -> None:
    """A fetched spatial exposes a typed metadata object and integral-nanosecond bounds, not raw beans."""
    clients.spatial.GetSpatial.return_value.spatial.CopyFrom(
        _raw_spatial(
            type_metadata=spatial_pb2.SpatialTypeMetadata(
                point_cloud=spatial_pb2.PointCloudMetadata(sensor_model="OS1-128", scan_pattern="ROTATING")
            ),
            start_timestamp=Timestamp(seconds=1_700_000_000, nanos=0),
        )
    )

    result = get_spatial(client, _SPATIAL_RID)

    assert result.metadata == PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING)
    assert result.start_timestamp == 1_700_000_000_000_000_000
    assert result.end_timestamp is None


# --- creation -----------------------------------------------------------------


def test_create_spatial_reserves_a_model_in_the_default_workspace(clients: MagicMock, client: MagicMock) -> None:
    """Creation names the model that will hold the data and lands in the client's default workspace."""
    clients.spatial.CreateSpatial.return_value = spatial_pb2.CreateSpatialResponse(spatial=_raw_spatial())

    create_point_cloud_spatial(
        client, "scan", metadata=PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING)
    )

    req = clients.spatial.CreateSpatial.call_args.args[0]
    assert uuid.UUID(req.spatial.dagger_model_uuid)
    assert req.workspace_rid == _WORKSPACE_RID
    assert req.spatial.type_metadata.point_cloud.sensor_model == "OS1-128"
    assert req.spatial.type_metadata.point_cloud.scan_pattern == "ROTATING"


def test_each_spatial_reserves_its_own_model(clients: MagicMock, client: MagicMock) -> None:
    """Two spatials sharing a model uuid would index their imports on top of each other."""
    clients.spatial.CreateSpatial.return_value = spatial_pb2.CreateSpatialResponse(spatial=_raw_spatial())

    uuids = set()
    for _ in range(3):
        create_point_cloud_spatial(client, "scan", metadata=PointCloudMetadata())
        uuids.add(clients.spatial.CreateSpatial.call_args.args[0].spatial.dagger_model_uuid)

    assert len(uuids) == 3


def test_create_spatial_applies_markings(clients: MagicMock, client: MagicMock) -> None:
    """Markings ride along in the create request, so a restricted spatial is never briefly visible."""
    clients.spatial.CreateSpatial.return_value = spatial_pb2.CreateSpatialResponse(spatial=_raw_spatial())

    create_point_cloud_spatial(
        client, "scan", metadata=PointCloudMetadata(), markings=["ri.scout.x.marking.m1", "ri.scout.x.marking.m2"]
    )

    req = clients.spatial.CreateSpatial.call_args.args[0]
    assert req.marking_rids == ["ri.scout.x.marking.m1", "ri.scout.x.marking.m2"]


def test_update_replaces_only_the_fields_passed(clients: MagicMock, spatial: Spatial) -> None:
    """Everything not named is left untouched rather than cleared, which is how the service reads it."""
    spatial.update(name="renamed")

    request = clients.spatial.UpdateSpatial.call_args.args[0]
    assert request.spatial.title == "renamed"
    assert list(request.update_mask.paths) == ["title"]
    assert not request.spatial.HasField("description")


# --- the measured time range --------------------------------------------------


def test_time_metadata_places_the_measured_extent_on_the_wall_clock() -> None:
    """An extent plus the instant it counts from yield both coordinate systems a workbook needs.

    `relative_*_us` is the extent of the time column, which per-point filtering
    compares against; `*_timestamp_us` is where that extent sits on the wall
    clock, which playhead progress is measured over. A workbook with only one of
    them has nothing to anchor to.
    """
    start = datetime(2026, 3, 4, 9, 30, tzinfo=UTC)
    start_us = int(start.timestamp() * 1_000_000)

    metadata = _PointCloudTimeMetadata.from_extent((0, 45_000_000), start)

    assert metadata.properties["relative_start_us"] == "0"
    assert metadata.properties["relative_end_us"] == "45000000"
    assert metadata.properties["start_timestamp_us"] == str(start_us)
    assert metadata.properties["end_timestamp_us"] == str(start_us + 45_000_000)


def test_time_metadata_shifts_both_bounds_by_a_nonzero_minimum() -> None:
    """A time column that starts late must move the spatial start, not just the end.

    The origin is where the column reads zero, which is not where the data begins.
    Anchoring the start at the origin while the end is offset stretches the window,
    and the playhead then maps onto the wrong part of the scan.
    """
    start = datetime(2026, 3, 4, 9, 30, tzinfo=UTC)
    origin_ns = int(start.timestamp()) * 1_000_000_000

    metadata = _PointCloudTimeMetadata.from_extent((9_000_000, 180_000_000), start)

    assert metadata.start == origin_ns + 9_000_000_000
    assert metadata.end == origin_ns + 180_000_000_000
    assert metadata.properties["start_timestamp_us"] == str(origin_ns // 1_000 + 9_000_000)


# --- point cloud ingest -------------------------------------------------------


def test_ingest_targets_this_spatial_and_records_the_uploaded_object(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """The ingest points at this spatial, and the object it uploaded is written back as provenance.

    The target must already exist -- the model uuid this spatial reserved at
    creation is what names the model the import writes into. The source handle is
    only knowable after the upload, so it cannot be set at create time.
    """
    with _stubbed_upload("s3://bucket/scan.csv"):
        job = spatial.add_point_cloud_csv(_csv(tmp_path))

    assert job.rid == _JOB_RID
    opts = clients.ingest.ingest.call_args.args[1].options.point_cloud
    assert opts.source.s3.path == "s3://bucket/scan.csv"
    assert opts.target.existing.spatial_rid == _SPATIAL_RID
    assert opts.target.new is None
    assert opts.dagger_import_config["format"]["geometry_type"] == "Point"
    # source_uri is the backend's to fill in from the presigned URL.
    assert "source_uri" not in opts.dagger_import_config

    request = clients.spatial.UpdateSpatial.call_args.args[0]
    assert request.spatial.source_handle.s3 == "s3://bucket/scan.csv"
    # A cloud with no time column gets no invented range.
    assert not request.spatial.HasField("start_timestamp")
    assert list(request.update_mask.paths) == ["source_handle"]


def test_ingest_merges_onto_server_properties_not_a_stale_snapshot(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """Properties are read back before overwriting, because the service replaces the map wholesale.

    A large upload can leave the in-memory snapshot minutes old, and anything added
    to the spatial meanwhile would be erased.
    """
    clients.spatial.GetSpatial.return_value.spatial.properties["added"] = "while uploading"

    with _stubbed_upload():
        spatial.add_point_cloud_csv(
            _csv(tmp_path, _TIMED_CSV),
            timestamp_column="t_s",
            timestamp_type=Relative("seconds", start=datetime(2026, 3, 4, tzinfo=UTC)),
        )

    properties = clients.spatial.UpdateSpatial.call_args.args[0].spatial.properties
    assert properties["added"] == "while uploading"
    assert properties["relative_end_us"] == "45000000"


def test_ingest_returns_the_job_even_if_recording_metadata_fails(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """The ingest is already running; losing its rid would orphan it.

    A retry would resubmit and duplicate every point, which is not recoverable,
    whereas the metadata can simply be written again.
    """
    clients.spatial.UpdateSpatial.side_effect = ConnectionError("boom")

    with _stubbed_upload():
        job = spatial.add_point_cloud_csv(_csv(tmp_path))

    assert job.rid == _JOB_RID


def test_ingest_validates_the_csv_before_uploading(clients: MagicMock, spatial: Spatial, tmp_path: Path) -> None:
    """A malformed CSV must fail before many GB are pushed to object storage."""
    with pytest.raises(ValueError, match="missing required point-cloud columns"):
        spatial.add_point_cloud_csv(_csv(tmp_path, "a,b,c\n1,2,3\n"))

    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timestamp_column": "t_s"},
        {"timestamp_type": Relative("seconds", start=datetime(2026, 3, 4, tzinfo=UTC))},
    ],
)
def test_ingest_rejects_half_of_the_time_pair(
    clients: MagicMock, spatial: Spatial, tmp_path: Path, kwargs: dict[str, object]
) -> None:
    """Neither half does anything alone, so half a pair is refused instead of silently dropped.

    Refused by `nominal.ts._validate_timestamp_pair`, the same guard every other
    method taking the pair uses, so the message is the one callers already know.
    Reaching the upload first would cost a full extra pass over the file and a
    potentially multi-GB transfer before the extent was dropped on the floor.
    """
    with pytest.raises(ValueError, match="pass both timestamp_column and timestamp_type"):
        spatial.add_point_cloud_csv(_csv(tmp_path, _TIMED_CSV), **kwargs)  # type: ignore[arg-type]

    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()


def test_ingest_rejects_a_timestamp_type_that_is_not_relative(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """An `Epoch` is refused up front rather than after the ingest has been submitted.

    Only `Relative` carries the start instant the time range is recorded against, so
    anything else fails on reading `.start` -- which, unguarded, happens after the
    upload and the submission. That loses the job rid, leaving an ingest running that
    nothing is tracking and a retry would submit a second time, duplicating every point.
    """
    with pytest.raises(TypeError, match="timestamp_type must be a Relative, not Epoch"):
        spatial.add_point_cloud_csv(
            _csv(tmp_path, _TIMED_CSV),
            timestamp_column="t_s",
            timestamp_type=Epoch("seconds"),  # type: ignore[call-overload]
        )

    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()


@pytest.mark.parametrize("unit", ["picoseconds", "minutes", "hours", "days"])
def test_ingest_rejects_units_the_panel_cannot_read(
    clients: MagicMock, spatial: Spatial, tmp_path: Path, unit: str
) -> None:
    """A unit `nominal.ts` knows but the 3D panel cannot read is refused before the upload.

    The panel re-reads the raw column values in a unit picked at view time and offers
    only nanoseconds through seconds. Converting the extent anyway would leave the two
    on different scales -- a 0..1 minute column recorded as a 0..60s range, swept across
    raw 0..1 values -- which renders wrongly with nothing to signal it.
    """
    with pytest.raises(ValueError, match="time_unit must be one of"):
        spatial.add_point_cloud_csv(
            _csv(tmp_path, _TIMED_CSV),
            timestamp_column="t_s",
            timestamp_type=Relative(unit, start=datetime(2026, 3, 4, tzinfo=UTC)),  # type: ignore[arg-type]
        )

    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()


def _asset_with_spatials(clients: MagicMock, *rids: str) -> Asset:
    """An asset with one spatial data scope per RID, each named after its RID."""
    api = asset_pb2.Asset(
        rid="asset",
        data_scopes=[
            asset_pb2.DataScope(data_scope_name=rid, data_source=asset_pb2.DataSource(spatial=rid)) for rid in rids
        ],
    )
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(responses={"asset": api})
    return Asset._from_proto(clients, api)


def _run_with_spatials(clients: MagicMock, *rids: str) -> Run:
    """A run with one spatial ref per RID, each named after its RID."""
    api = run_service_pb2.Run(
        rid="run",
        data_sources={
            rid: run_service_pb2.RunDataSource(data_source=run_service_pb2.DataSource(spatial=rid)) for rid in rids
        },
    )
    clients.run.GetRun.return_value = run_service_pb2.GetRunResponse(run=api)
    return Run._from_proto(clients, api)


@pytest.mark.parametrize(
    ("make_parent", "list_spatials", "get_spatial_from", "not_found"),
    [
        (_asset_with_spatials, list_spatials_in_asset, get_spatial_from_asset, "data scope name 'unreadable'"),
        (_run_with_spatials, list_spatials_in_run, get_spatial_from_run, "ref name 'unreadable'"),
    ],
)
def test_spatial_scopes_resolve_in_one_request_and_leave_out_unreadable_spatials(
    clients: MagicMock, make_parent, list_spatials, get_spatial_from, not_found: str
) -> None:
    """Spatial scopes resolve in one batch request; an unreadable one is left out of lists and not found by lookup."""
    readable = _raw_spatial()
    readable.rid = "readable"
    clients.spatial.BatchGetSpatials.return_value = spatial_pb2.BatchGetSpatialsResponse(spatials=[readable])
    parent = make_parent(clients, "readable", "unreadable")

    assert [(name, spatial.rid) for name, spatial in list_spatials(parent)] == [("readable", "readable")]
    (request,), _ = clients.spatial.BatchGetSpatials.call_args
    assert set(request.spatial_rids) == {"readable", "unreadable"}
    clients.spatial.GetSpatial.assert_not_called()

    with pytest.raises(NominalNotFoundError, match=f"No spatial with {not_found} found"):
        get_spatial_from(parent, "unreadable")


def test_update_empty_values_refreshes_same_instance(clients: MagicMock, spatial: Spatial) -> None:
    """Empty values replace metadata and the authoritative response refreshes the existing object."""
    response = _raw_spatial()
    response.title = "server title"
    response.description = ""
    clients.spatial.UpdateSpatial.return_value = spatial_pb2.UpdateSpatialResponse(spatial=response)
    assert spatial.update(description="", labels=[], properties={}) is spatial
    request = clients.spatial.UpdateSpatial.call_args.args[0]
    assert set(request.update_mask.paths) == {"description", "labels", "properties"}
    assert request.spatial.HasField("description")
    assert request.spatial.description == ""
    assert dict(request.spatial.properties) == {}
    assert list(request.spatial.labels) == []
    assert spatial.name == "server title"
    assert spatial.description == ""
    assert spatial.labels == ()
    assert spatial.properties == {}
    clients.spatial.GetSpatial.assert_not_called()


def test_update_without_fields_refreshes_without_empty_mask(clients: MagicMock, spatial: Spatial) -> None:
    """A no-op update reloads the snapshot because the service rejects an empty update mask."""
    clients.spatial.GetSpatial.return_value.spatial.title = "new snapshot"
    assert spatial.update() is spatial
    assert spatial.name == "new snapshot"
    clients.spatial.UpdateSpatial.assert_not_called()


@pytest.mark.parametrize(
    ("scan_pattern", "expected_pattern"),
    [(None, None), ("", ScanPattern.UNKNOWN), ("FUTURE_PATTERN", ScanPattern.UNKNOWN), ("FLASH", ScanPattern.FLASH)],
)
def test_proto_conversion_preserves_optional_metadata_and_exact_times(
    clients: MagicMock, scan_pattern: str | None, expected_pattern: ScanPattern | None
) -> None:
    """Presence differs from zero or empty, unknown patterns remain readable, and timestamps retain nanoseconds."""
    raw = _raw_spatial(
        type_metadata=spatial_pb2.SpatialTypeMetadata(
            point_cloud=spatial_pb2.PointCloudMetadata(sensor_model="", resolution_mm=0, scan_pattern=scan_pattern)
        ),
        start_timestamp=Timestamp(seconds=-1, nanos=999_999_999),
    )
    raw.created_at.CopyFrom(Timestamp(seconds=-1, nanos=1))
    raw.end_timestamp.CopyFrom(Timestamp(seconds=0, nanos=1))
    raw.created_by = "creator"
    spatial = Spatial._from_proto(clients, raw)
    assert spatial.metadata == PointCloudMetadata(sensor_model="", resolution_mm=0, scan_pattern=expected_pattern)
    assert spatial.created_at == -999_999_999
    assert spatial.start_timestamp == -1
    assert spatial.end_timestamp == 1
    assert spatial.created_by_rid == "creator"


def test_unknown_spatial_type_metadata_remains_readable(clients: MagicMock) -> None:
    """An unknown oneof arm yields empty metadata rather than preventing hydration."""
    raw = _raw_spatial()
    raw.type_metadata.ClearField("point_cloud")
    assert Spatial._from_proto(clients, raw).metadata == PointCloudMetadata()


def test_ingest_time_metadata_preserves_nanoseconds_and_order(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """Metadata follows accepted ingest and preserves a negative submicrosecond origin exactly."""
    with _stubbed_upload():
        spatial.add_point_cloud_csv(
            _csv(tmp_path, _TIMED_CSV), timestamp_column="t_s", timestamp_type=Relative("seconds", start=-1)
        )
    request = clients.spatial.UpdateSpatial.call_args.args[0]
    assert request.spatial.start_timestamp.ToNanoseconds() == -1
    assert request.spatial.end_timestamp.ToNanoseconds() == 44_999_999_999
    assert set(request.update_mask.paths) == {"source_handle", "properties", "start_timestamp", "end_timestamp"}
    calls = [entry[0] for entry in clients.mock_calls]
    assert calls.index("ingest.ingest") < calls.index("ingest_jobs.get_ingest_job") < calls.index("spatial.GetSpatial")
    assert calls.index("spatial.GetSpatial") < calls.index("spatial.UpdateSpatial")


def test_scope_batch_hydration_chunks_service_limit(clients: MagicMock) -> None:
    """Large scope lists stay readable when spatial batch requests are limited to 1000 RIDs."""
    rids = [str(index) for index in range(1001)]
    first = _raw_spatial()
    first.rid = rids[0]
    last = _raw_spatial()
    last.rid = rids[-1]
    clients.spatial.BatchGetSpatials.side_effect = [
        spatial_pb2.BatchGetSpatialsResponse(spatials=[first]),
        spatial_pb2.BatchGetSpatialsResponse(spatials=[last]),
    ]
    result = list_spatials_in_asset(_asset_with_spatials(clients, *rids))
    assert [(name, spatial.rid) for name, spatial in result] == [(rids[0], rids[0]), (rids[-1], rids[-1])]
    requests = [entry.args[0] for entry in clients.spatial.BatchGetSpatials.call_args_list]
    assert [len(request.spatial_rids) for request in requests] == [1000, 1]
    assert [rid for request in requests for rid in request.spatial_rids] == rids


def test_empty_scope_list_does_not_fetch_spatials(clients: MagicMock) -> None:
    """An empty spatial collection requires no batch request."""
    assert list_spatials_in_asset(_asset_with_spatials(clients)) == []
    clients.spatial.BatchGetSpatials.assert_not_called()
