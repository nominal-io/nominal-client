from unittest.mock import MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from nominal.core import Dataset
from nominal.core.bounds import Bounds
from nominal.core.dataset_file import DatasetFile, IngestStatus, _dataset_file_from_proto
from nominal.core.video_dataset_file import VideoDatasetFile
from nominal.protos.datasets.v1 import dataset_files_pb2 as files_pb
from nominal.protos.ingest.v2 import common_pb2
from nominal.ts import Epoch


def test_proto_optional_metadata_and_epoch_timestamps():
    """Absent optional metadata stays absent, while explicit epoch and empty maps survive conversion."""
    clients = MagicMock()
    absent = DatasetFile._from_proto(clients, files_pb.DatasetFile(uploaded_at=Timestamp()))
    assert absent.uploaded_at == 0
    assert absent.ingested_at is None
    assert absent.deleted_at is None
    assert absent.bounds is None
    assert absent.file_tags is None
    assert absent.tag_columns is None
    assert absent.ingest_status is IngestStatus.UNKNOWN

    present = DatasetFile._from_proto(
        clients,
        files_pb.DatasetFile(
            uploaded_at=Timestamp(seconds=-1, nanos=999999999),
            ingested_at=Timestamp(),
            deleted_at=Timestamp(),
            ingest_tag_metadata=files_pb.IngestTagMetadata(),
        ),
    )
    assert present.uploaded_at == -1
    assert present.ingested_at == 0
    assert present.deleted_at == 0
    assert present.file_tags == {}
    assert present.tag_columns == {}


def test_file_metadata_preserves_bounds_timestamp_type_and_detaches_tags():
    """Conversion retains nanosecond bounds and parsing metadata without exposing mutable proto maps."""
    bounds = Bounds(start=-1, end=1_700_000_000_123_456_789)
    row = files_pb.DatasetFile(
        bounds=files_pb.DataBounds(
            start=Timestamp(seconds=-1, nanos=999999999),
            end=Timestamp(seconds=1_700_000_000, nanos=123456789),
        ),
        timestamp_metadata=common_pb2.TimestampMetadata(column="time", type=Epoch("microseconds")._to_proto()),
        ingest_tag_metadata=files_pb.IngestTagMetadata(
            additional_file_tags={"vehicle": "a"}, tag_columns={"id": "vehicle"}
        ),
        ingest_status=files_pb.IngestStatus(failed=files_pb.IngestFailed(message="bad input", error_type="FORMAT")),
    )
    file = DatasetFile._from_proto(MagicMock(), row)
    row.ingest_tag_metadata.additional_file_tags["vehicle"] = "b"
    assert file.bounds == bounds
    assert file.timestamp_channel == "time"
    assert file.timestamp_type == Epoch("microseconds")
    assert file.file_tags == {"vehicle": "a"}
    assert file.tag_columns == {"id": "vehicle"}
    assert file._ingest_error_message == "bad input (FORMAT)"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (files_pb.IngestStatus(succeeded=files_pb.IngestStatus.Succeeded()), IngestStatus.SUCCESS),
        (files_pb.IngestStatus(failed=files_pb.IngestFailed()), IngestStatus.FAILED),
        (files_pb.IngestStatus(deleted=files_pb.IngestStatus.Deleted()), IngestStatus.DELETED),
        (
            files_pb.IngestStatus(deletion_in_progress=files_pb.IngestStatus.DeletionInProgress()),
            IngestStatus.DELETION_IN_PROGRESS,
        ),
        (files_pb.IngestStatus(in_progress=files_pb.IngestInProgress()), IngestStatus.IN_PROGRESS),
        (
            files_pb.IngestStatus(in_progress=files_pb.IngestInProgress(phase=files_pb.INGEST_PHASE_QUEUED)),
            IngestStatus.QUEUED,
        ),
        (
            files_pb.IngestStatus(in_progress=files_pb.IngestInProgress(phase=files_pb.INGEST_PHASE_PARSING)),
            IngestStatus.PARSING,
        ),
        (
            files_pb.IngestStatus(in_progress=files_pb.IngestInProgress(phase=files_pb.INGEST_PHASE_INGESTING)),
            IngestStatus.INGESTING,
        ),
        (files_pb.IngestStatus(in_progress=files_pb.IngestInProgress(phase=99)), IngestStatus.UNKNOWN),
    ],
)
def test_ingestion_status_preserves_phases_and_unknown_values(status, expected):
    """Polling sees distinct ingestion phases and tolerates future status values."""
    assert IngestStatus._from_proto(status) is expected


def test_video_refresh_preserves_identity_and_zero_summary():
    """A video refresh keeps the subtype and distinguishes an absent summary from zero values."""
    clients = MagicMock()
    row = files_pb.DatasetFile(
        id="file", dataset_rid="dataset", metadata=files_pb.DatasetFileMetadata(video=files_pb.VideoFileMetadata())
    )
    file = _dataset_file_from_proto(clients, row)
    assert isinstance(file, VideoDatasetFile)
    assert file.num_frames is None
    row.metadata.video.segments_summary.CopyFrom(files_pb.VideoSegmentsSummary())
    clients.grpc_dataset_files.GetDatasetFile.return_value = files_pb.GetDatasetFileResponse(dataset_file=row)
    assert file.refresh() is file
    assert (
        file.num_frames,
        file.num_segments,
        file.media_duration_seconds,
        file.media_frame_rate,
        file.scale_factor,
    ) == (0, 0, 0, 0, 0)


def test_origin_uri_lookup_follows_pages_and_matches_path():
    """Renewing an origin URL finds its path on later pages rather than selecting by position."""
    clients = MagicMock()
    clients.grpc_dataset_files.ListDatasetFileOriginUris.side_effect = [
        files_pb.ListDatasetFileOriginUrisResponse(
            origin_file_uris=[files_pb.OriginFileUri(path="other", uri="other-url")], next_page_token="next"
        ),
        files_pb.ListDatasetFileOriginUrisResponse(
            origin_file_uris=[files_pb.OriginFileUri(path="target", uri="target-url")]
        ),
    ]
    file = DatasetFile._from_proto(clients, files_pb.DatasetFile(id="file", dataset_rid="dataset"))
    provider = file._origin_presigned_url_provider("target")
    assert provider.get_url() == "target-url"
    requests = [call.args[0] for call in clients.grpc_dataset_files.ListDatasetFileOriginUris.call_args_list]
    assert [(request.dataset_rid, request.file_id, request.page_token) for request in requests] == [
        ("dataset", "file", ""),
        ("dataset", "file", "next"),
    ]


def test_dataset_file_search_preserves_truncation_and_pages(caplog):
    """Search retains whole-second overlap bounds, empty-tag match-all semantics, ordering, and page cursors."""
    clients = MagicMock()
    dataset = Dataset(
        rid="dataset",
        name="test",
        description=None,
        bounds=None,
        properties={},
        labels=[],
        is_archived=False,
        _clients=clients,
    )
    clients.grpc_dataset_files.SearchDatasetFiles.side_effect = [
        files_pb.SearchDatasetFilesResponse(dataset_files=[files_pb.DatasetFile(id="first")], next_page_token="next"),
        files_pb.SearchDatasetFilesResponse(dataset_files=[files_pb.DatasetFile(id="second")]),
    ]
    assert [file.id for file in dataset.search_files(start=-1, file_tags={})] == ["first", "second"]
    requests = [call.args[0] for call in clients.grpc_dataset_files.SearchDatasetFiles.call_args_list]
    assert [request.page_token for request in requests] == ["", "next"]
    for request in requests:
        assert request.dataset_rid == "dataset"
        assert request.sort.descending
        assert request.sort.field == files_pb.DATASET_FILE_SORT_FIELD_UPLOADED_AT
        clauses = getattr(request.query, "and").queries
        assert clauses[0].data_time_range.start == Timestamp(seconds=-1)
        assert not clauses[0].data_time_range.HasField("end")
        assert len(clauses) == 1
    assert "Truncating sub-second precision" in caplog.text


def test_list_files_filters_success_across_pages_and_retains_video_subtype():
    """Lazy listing advances empty-success pages and returns only successful files, including videos."""
    clients = MagicMock()
    dataset = Dataset(
        rid="dataset",
        name="test",
        description=None,
        bounds=None,
        properties={},
        labels=[],
        is_archived=False,
        _clients=clients,
    )
    clients.grpc_dataset_files.ListDatasetFiles.side_effect = [
        files_pb.ListDatasetFilesResponse(
            dataset_files=[
                files_pb.DatasetFile(
                    id="pending", ingest_status=files_pb.IngestStatus(in_progress=files_pb.IngestInProgress())
                )
            ],
            next_page_token="next",
        ),
        files_pb.ListDatasetFilesResponse(
            dataset_files=[
                files_pb.DatasetFile(
                    id="video",
                    ingest_status=files_pb.IngestStatus(succeeded=files_pb.IngestStatus.Succeeded()),
                    metadata=files_pb.DatasetFileMetadata(video=files_pb.VideoFileMetadata()),
                )
            ]
        ),
    ]
    files = dataset.list_files()
    clients.grpc_dataset_files.ListDatasetFiles.assert_not_called()
    results = list(files)
    assert [file.id for file in results] == ["video"]
    assert isinstance(results[0], VideoDatasetFile)
    assert [call.args[0].page_token for call in clients.grpc_dataset_files.ListDatasetFiles.call_args_list] == [
        "",
        "next",
    ]


@pytest.mark.parametrize("file_tags", [None, {}])
def test_dataset_file_search_without_filters_uses_unbounded_range(file_tags):
    """No filters match all files using a valid query, including files without bounds."""
    clients = MagicMock()
    dataset = Dataset(
        rid="dataset",
        name="test",
        description=None,
        bounds=None,
        properties={},
        labels=[],
        is_archived=False,
        _clients=clients,
    )
    clients.grpc_dataset_files.SearchDatasetFiles.return_value = files_pb.SearchDatasetFilesResponse(
        dataset_files=[files_pb.DatasetFile(id="without-bounds")]
    )
    assert [file.id for file in dataset.search_files(file_tags=file_tags)] == ["without-bounds"]
    query = clients.grpc_dataset_files.SearchDatasetFiles.call_args.args[0].query
    assert query.WhichOneof("query") == "data_time_range"
    assert not query.data_time_range.HasField("start")
    assert not query.data_time_range.HasField("end")


def test_unknown_ingest_phase_warning_identifies_wire_value(caplog):
    """Future server phases stay nonblocking and leave enough detail to diagnose the wire value."""
    status = files_pb.IngestStatus(in_progress=files_pb.IngestInProgress(phase=99))
    assert IngestStatus._from_proto(status) is IngestStatus.UNKNOWN
    assert "in_progress" in caplog.text
    assert "99" in caplog.text
