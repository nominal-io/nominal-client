from __future__ import annotations

import contextlib
import io
from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import MagicMock, Mock, patch

import pytest

from nominal import ts
from nominal.core.dataset import Dataset, DatasetBounds, _DatasetWrapper
from nominal.core.exceptions import NominalIngestError
from nominal.core.log import LogPoint
from nominal.core.unit import Unit
from nominal.core.video_dataset_file import VideoDatasetFile

UNITS = [
    Unit(name="coulomb", symbol="C"),
    Unit(name="kilograms", symbol="kg"),
    Unit(name="mole", symbol="mol"),
]


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.logical_series = MagicMock()
    return clients


@pytest.fixture
def mock_dataset(mock_clients):
    ds = Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=mock_clients,
    )

    spy: MagicMock = MagicMock(wraps=ds.refresh)
    object.__setattr__(ds, "refresh", spy)
    spy.return_value = ds

    return ds


def test_write_logs_more_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=2)

    assert len(endpoint.call_args_list) == 2

    _auth, _rid, first_req = endpoint.call_args_list[0][0]
    assert len(first_req.logs) == 2

    _auth, _rid, second_req = endpoint.call_args_list[1][0]
    assert len(second_req.logs) == 1


def test_write_logs_less_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=1000)

    assert len(endpoint.call_args_list) == 1
    _auth, _rid, req = endpoint.call_args_list[0][0]
    assert len(req.logs) == 3


def _video_response(dataset_file_id, ingest_job_rid="job-1"):
    response = MagicMock()
    response.details.dataset.dataset_rid = "ds-1"
    response.details.dataset.dataset_file_id = dataset_file_id
    response.ingest_job_rid = ingest_job_rid
    return response


def test_resolve_ingested_video_file_prefers_direct_dataset_file_id():
    """A response carrying a dataset-file id is resolved through the catalog, not the ingest job."""
    ds = MagicMock()
    video_file = MagicMock(spec=VideoDatasetFile)
    with patch("nominal.core.dataset._dataset_file_from_conjure", return_value=video_file):
        result = Dataset._resolve_ingested_video_file(ds, _video_response("file-1"))
    assert result is video_file
    ds._clients.catalog.get_dataset_file.assert_called_once()


def test_resolve_ingested_video_file_direct_id_non_video_raises():
    """A direct file id pointing at a non-video row is an error, not a silent base-type return."""
    ds = MagicMock()
    with (
        patch("nominal.core.dataset._dataset_file_from_conjure", return_value=MagicMock()),  # not a VideoDatasetFile
        pytest.raises(NominalIngestError, match="not a video dataset file"),
    ):
        Dataset._resolve_ingested_video_file(ds, _video_response("file-1"))


def test_resolve_ingested_video_file_falls_back_to_ingest_job_single_video_file():
    """Without a file id, the single video file produced by the ingest job is returned."""
    ds = MagicMock()
    video_file = MagicMock(spec=VideoDatasetFile)
    job = MagicMock()
    # A non-video file (bare MagicMock) and the one video file; the handler must filter to the video.
    job.dataset_files.return_value = [MagicMock(), video_file]
    with patch("nominal.core.dataset.IngestionJob._from_conjure", return_value=job):
        result = Dataset._resolve_ingested_video_file(ds, _video_response(None))
    assert result is video_file


def test_resolve_ingested_video_file_fallback_zero_or_multiple_raises():
    """The job fallback requires exactly one produced video file."""
    ds = MagicMock()
    job = MagicMock()
    job.dataset_files.return_value = []
    with (
        patch("nominal.core.dataset.IngestionJob._from_conjure", return_value=job),
        pytest.raises(NominalIngestError, match="exactly one video file"),
    ):
        Dataset._resolve_ingested_video_file(ds, _video_response(None))


def test_resolve_ingested_video_file_no_id_and_no_job_raises():
    """A response with neither a file id nor an ingest job cannot be resolved."""
    ds = MagicMock()
    with pytest.raises(NominalIngestError, match="neither a dataset-file id nor an ingest job"):
        Dataset._resolve_ingested_video_file(ds, _video_response(None, ingest_job_rid=None))


@pytest.mark.parametrize(
    "bad_timestamps",
    [{}, {"start": 0, "frame_timestamps": [1]}],
    ids=["neither", "both"],
)
@pytest.mark.parametrize(
    ("method", "args"),
    [("add_video", ("v.mp4",)), ("add_video_from_io", (io.BytesIO(b""), "v.mp4"))],
    ids=["path", "io"],
)
def test_video_entry_points_require_exactly_one_timestamp_mode(method, args, bad_timestamps):
    """Every video entry point rejects zero or two timestamp modes before doing any work."""
    ds = MagicMock()
    with pytest.raises(ValueError, match="'start' or 'frame_timestamps'"):
        getattr(Dataset, method)(ds, *args, channel="c", **bad_timestamps)


@pytest.mark.parametrize(
    ("method", "kwargs"),
    [("add_video_from_io", {"start": 0}), ("add_mcap_video_from_io", {"topic": "/t"})],
    ids=["video", "mcap"],
)
def test_video_from_io_entry_points_reject_text_streams(method, kwargs):
    """Both from-io video entry points reject text-mode streams before uploading."""
    ds = MagicMock()
    with pytest.raises(TypeError, match="binary mode"):
        getattr(Dataset, method)(ds, io.StringIO("x"), "v", channel="c", **kwargs)


def test_add_video_from_io_submits_video_v2_ingest(mock_dataset: Dataset):
    """The real IngestRequest handed to ingest carries the manifest arm, channel, tags, source, and target."""
    ingest_endpoint = Mock()
    cast(Any, mock_dataset._clients.ingest).ingest = ingest_endpoint
    video_file = MagicMock(spec=VideoDatasetFile)
    with (
        patch("nominal.core.dataset.upload_multipart_io", return_value="s3://bucket/front.mp4"),
        patch("nominal.core.dataset._dataset_file_from_conjure", return_value=video_file),
    ):
        result = mock_dataset.add_video_from_io(
            io.BytesIO(b"data"),
            "front.mp4",
            channel="camera/front",
            start=123,
            tags={"vehicle": "alpha"},
        )

    _auth, request = ingest_endpoint.call_args[0]
    opts = request.options.video_v2
    assert opts is not None
    assert opts.channel == "camera/front"
    assert opts.tags == {"vehicle": "alpha"}
    assert opts.source.s3.path == "s3://bucket/front.mp4"
    assert opts.target.existing.dataset_rid == "test-rid"
    assert opts.timestamp_manifest.no_manifest is not None
    assert opts.over_write_segments is None  # overwrite_overlapping=False maps to an absent field
    assert result is video_file


def test_add_mcap_video_rejects_non_mcap_path(tmp_path):
    """A non-.mcap path raises locally instead of uploading under an mcap name."""
    ds = MagicMock()
    f = tmp_path / "front.mp4"
    f.write_bytes(b"\x00")
    with pytest.raises(ValueError, match="must end in"):
        Dataset.add_mcap_video(ds, str(f), channel="cam", topic="/t")
    ds.add_mcap_video_from_io.assert_not_called()


def test_list_video_files_yields_only_video_subtypes():
    """list_video_files filters mixed dataset files down to the video subtype."""
    ds = MagicMock()
    video = MagicMock(spec=VideoDatasetFile)
    plain = MagicMock()
    ds.list_files.return_value = [video, plain, video]
    result = list(Dataset.list_video_files(ds, successful_only=False))
    assert result == [video, video]
    ds.list_files.assert_called_once_with(successful_only=False)


def test_get_video_file_raises_type_error_for_non_video():
    """get_video_file refuses to hand back a non-video file as a video handle."""
    ds = MagicMock()
    ds.get_dataset_file.return_value = MagicMock()  # not a VideoDatasetFile
    with pytest.raises(TypeError, match="not a video dataset file"):
        Dataset.get_video_file(ds, "file-1")


@pytest.mark.parametrize("filename", ["records.parquet", "records.csv", "records.jsonl", "records"])
def test_add_avro_stream_rejects_non_avro_extensions(mock_dataset: Dataset, tmp_path, filename: str) -> None:
    """A wrong extension fails before any bytes are uploaded, matching the other add_* methods."""
    path = tmp_path / filename
    path.write_bytes(b"not avro")

    with pytest.raises(ValueError, match="avro-stream path"):
        mock_dataset.add_avro_stream(path)

    mock_dataset._clients.upload.assert_not_called()


@pytest.mark.parametrize(
    ("filename", "expected_extension"),
    [("records.avro", ".avro"), ("records.avro.gz", ".avro.gz")],
)
def test_add_avro_stream_uploads_with_the_resolved_file_type(
    mock_dataset: Dataset, tmp_path, filename: str, expected_extension: str
) -> None:
    """A gzipped stream uploads described as .avro.gz, not as plain .avro."""
    path = tmp_path / filename
    path.write_bytes(b"avro")

    # The mocked ingest response cannot build a DatasetFile; what is under test is the upload itself.
    with patch("nominal.core.dataset.upload_multipart_file", return_value="s3://path") as upload:
        with contextlib.suppress(ValueError):
            mock_dataset.add_avro_stream(path)

    assert upload.call_args.kwargs["file_type"].extension == expected_extension


def test_add_avro_stream_accepts_tags_and_an_epoch_string_literal(mock_dataset: Dataset, tmp_path) -> None:
    """The avro request's options carry the merged tags and the converted numeric timestamp type."""
    path = tmp_path / "records.avro"
    path.write_bytes(b"avro")

    with patch("nominal.core.dataset.upload_multipart_file", return_value="s3://path"):
        with contextlib.suppress(ValueError):
            mock_dataset.add_avro_stream(path, timestamp_type="epoch_microseconds", tags={"vehicle": "v1"})

    # ingest() is called positionally as (auth_header, request).
    (_, request) = mock_dataset._clients.ingest.ingest.call_args.args
    avro_opts = request.options.avro_stream
    assert avro_opts.additional_file_tags == {"vehicle": "v1"}
    assert avro_opts.timestamp_type.epoch.time_unit.value == "MICROSECONDS"


def test_add_journal_json_without_timestamp_arguments_ingests(mock_dataset: Dataset, tmp_path) -> None:
    """Omitting both timestamp arguments is the default call, not a half-specified override."""
    path = tmp_path / "logs.jsonl"
    path.write_text('{"MESSAGE": "hello"}')

    # The mocked client reports no real ingest status, so a ValueError escapes once the ingest is
    # triggered. Reaching the upload at all is what is under test. (Verified: the escaping exception
    # is ValueError("Unknown ingest status: ..."), so do not widen this to Exception.)
    with patch("nominal.core.dataset.upload_multipart_file", return_value="s3://path") as upload:
        with contextlib.suppress(ValueError):
            mock_dataset.add_journal_json(path)

    upload.assert_called_once()


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"timestamp_column": "ts"}, id="column-without-type"),
        pytest.param({"timestamp_type": ts.Epoch("microseconds")}, id="type-without-column"),
    ],
)
def test_add_journal_json_rejects_half_specified_timestamps(mock_dataset: Dataset, tmp_path, kwargs) -> None:
    """One half of the timestamp pair is an error, and fails before any bytes are uploaded."""
    path = tmp_path / "logs.jsonl"
    path.write_text('{"MESSAGE": "hello"}')

    with patch("nominal.core.dataset.upload_multipart_file") as upload:
        with pytest.raises(ValueError, match="pass both"):
            mock_dataset.add_journal_json(path, **kwargs)

    upload.assert_not_called()


def test_add_journal_json_accepts_an_epoch_string_literal(mock_dataset: Dataset, tmp_path) -> None:
    """The string form of an epoch type works here as it does everywhere else in the client."""
    path = tmp_path / "logs.jsonl"
    path.write_text('{"ts": 1}')

    with patch("nominal.core.dataset.upload_multipart_file", return_value="s3://path"):
        with contextlib.suppress(ValueError):
            mock_dataset.add_journal_json(path, timestamp_column="ts", timestamp_type="epoch_microseconds")

    # ingest() is called positionally as (auth_header, request).
    (_, request) = mock_dataset._clients.ingest.ingest.call_args.args
    metadata = request.options.journal_json.timestamp_metadata
    assert metadata.field_name == "ts"
    assert metadata.epoch_of_time_unit.time_unit.value == "MICROSECONDS"


@pytest.mark.parametrize(
    "timestamp_type",
    [
        pytest.param("iso_8601", id="string-format-literal"),
        pytest.param(ts.Relative("seconds", start=0), id="relative"),
    ],
)
def test_add_journal_json_rejects_non_epoch_timestamps_before_uploading(
    mock_dataset: Dataset, tmp_path, timestamp_type
) -> None:
    """A timestamp type journal ingest cannot express fails at the call, not after the file is uploaded."""
    path = tmp_path / "logs.jsonl"
    path.write_text('{"ts": 1}')

    with patch("nominal.core.dataset.upload_multipart_file") as upload:
        with pytest.raises(ValueError, match="epoch"):
            mock_dataset.add_journal_json(path, timestamp_column="ts", timestamp_type=timestamp_type)

    upload.assert_not_called()


class _StubWrapper(_DatasetWrapper):
    """The wrapper with scope resolution stubbed: what is under test is the delegation, not lookup."""

    def _list_dataset_scopes(self):
        return []


def test_data_scope_avro_stream_merges_scope_tags() -> None:
    """Avro ingest carries tags now, so a tagged scope no longer blocks the file; caller tags win on collision."""
    dataset = MagicMock()
    scope_tags = {"vehicle": "v1", "run": "scope-run"}
    with patch.object(_StubWrapper, "_get_dataset_scope", return_value=(dataset, scope_tags)):
        _StubWrapper().add_avro_stream("scope", "records.avro", tags={"run": "r1"})

    dataset.add_avro_stream.assert_called_once_with(
        "records.avro", timestamp_type=None, tags={"vehicle": "v1", "run": "r1"}
    )


def test_data_scope_avro_stream_forwards_the_timestamp_type() -> None:
    """The scope wrapper is a passthrough for the timestamp type, not a second policy layer."""
    dataset = MagicMock()
    with patch.object(_StubWrapper, "_get_dataset_scope", return_value=(dataset, {})):
        _StubWrapper().add_avro_stream("scope", "records.avro", timestamp_type="epoch_microseconds")

    assert dataset.add_avro_stream.call_args.kwargs["timestamp_type"] == "epoch_microseconds"


def test_data_scope_journal_json_forwards_the_timestamp_pair() -> None:
    """The journal wrapper forwards both halves of the pair untouched."""
    dataset = MagicMock()
    with patch.object(_StubWrapper, "_get_dataset_scope", return_value=(dataset, {})):
        _StubWrapper().add_journal_json(
            "scope", "logs.jsonl", channel="events", timestamp_column="ts", timestamp_type="epoch_microseconds"
        )

    dataset.add_journal_json.assert_called_once_with(
        "logs.jsonl", channel="events", timestamp_column="ts", timestamp_type="epoch_microseconds"
    )


def test_data_scope_journal_json_still_refuses_a_tagged_scope() -> None:
    """Journal ingest carries no tags, so a tagged scope must keep failing rather than lose them."""
    dataset = MagicMock()
    with patch.object(_StubWrapper, "_get_dataset_scope", return_value=(dataset, {"vehicle": "v1"})):
        with pytest.raises(RuntimeError, match="would not get"):
            _StubWrapper().add_journal_json("scope", "logs.jsonl")

    dataset.add_journal_json.assert_not_called()


def test_data_scope_journal_json_default_call_forwards_an_explicit_none_pair() -> None:
    """The default call shape forwards both halves as None, which is the shape the delegation suppresses.

    The suppressed argument types are only sound while this holds: the delegate must accept a pair of
    Nones as "no timestamp metadata". If that ever changes, this test is what fails.
    """
    dataset = MagicMock()
    with patch.object(_StubWrapper, "_get_dataset_scope", return_value=(dataset, {})):
        _StubWrapper().add_journal_json("scope", "logs.jsonl")

    dataset.add_journal_json.assert_called_once_with(
        "logs.jsonl", channel=None, timestamp_column=None, timestamp_type=None
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"timestamp_column": "ts"}, id="column-without-type"),
        pytest.param({"timestamp_type": "epoch_microseconds"}, id="type-without-column"),
    ],
)
def test_data_scope_journal_json_rejects_a_half_pair_before_resolving_the_scope(kwargs) -> None:
    """A mispaired call fails on its own arguments, without a round trip to resolve the data scope."""
    with patch.object(_StubWrapper, "_get_dataset_scope") as get_scope:
        with pytest.raises(ValueError, match="pass both"):
            _StubWrapper().add_journal_json("scope", "logs.jsonl", **kwargs)

    get_scope.assert_not_called()
