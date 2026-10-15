from datetime import timedelta
from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core._stream.batch_processor import process_log_batch
from nominal.core._stream.write_stream import BatchItem
from nominal.core.dataset import Dataset
from nominal.core.log import LogPoint
from nominal.exceptions import NominalPermissionDeniedError
from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2 as writer_pb


@pytest.fixture
def dataset():
    """A dataset whose transport calls are isolated from the network."""
    return Dataset(
        rid="ri.catalog.main.dataset.00000000-0000-0000-0000-000000000001",
        name="logs",
        description=None,
        bounds=None,
        properties={},
        labels=[],
        is_archived=False,
        _clients=MagicMock(),
    )


def test_log_batch_groups_channels_and_preserves_point_args():
    """Grouping remains stable within each channel and preserves exact negative timestamps and args."""
    writer = MagicMock()
    process_log_batch(
        [
            BatchItem("b", 1_000_000_007, "second", {"level": "info"}),
            BatchItem("a", -1, "first", {"level": "warn"}),
            BatchItem("b", 0, "third", None),
        ],
        "data-source",
        storage_writer=writer,
    )
    requests = [call.args[0] for call in writer.WriteLogs.call_args_list]
    assert [request.channel for request in requests] == ["a", "b"]
    assert all(isinstance(request, writer_pb.WriteLogsRequest) for request in requests)
    assert all(request.data_source_rid == "data-source" for request in requests)
    assert (requests[0].logs[0].timestamp.seconds, requests[0].logs[0].timestamp.nanos) == (-1, 999_999_999)
    assert [point.value.message for point in requests[1].logs] == ["second", "third"]
    assert requests[1].logs[0].timestamp.nanos == 7
    assert dict(requests[0].logs[0].value.args) == {"level": "warn"}
    assert dict(requests[1].logs[1].value.args) == {}


def test_write_logs_stops_consuming_generator_on_translated_failure(dataset, fake_rpc_error):
    """A failed synchronous batch raises NominalError before consuming the next generator batch."""
    consumed = []

    def logs():
        for index in range(3):
            consumed.append(index)
            yield LogPoint(index, str(index), {"source": "test"})

    dataset._clients.storage_writer.WriteLogs.side_effect = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)
    with pytest.raises(NominalPermissionDeniedError):
        dataset.write_logs(logs(), batch_size=2)
    assert consumed == [0, 1]
    request = dataset._clients.storage_writer.WriteLogs.call_args.args[0]
    assert request.data_source_rid == dataset.rid
    assert request.channel == "logs"
    assert request.logs[1].timestamp.nanos == 1
    assert dict(request.logs[0].value.args) == {"source": "test"}


def test_log_stream_flush_drains_and_logs_translated_failures(dataset, fake_rpc_error, caplog):
    """Background writes retain their existing logged-failure behavior and drain on close."""
    dataset._clients.storage_writer.WriteLogs.side_effect = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)
    with dataset.get_log_stream(batch_size=10, max_wait=timedelta(hours=1)) as stream:
        stream.enqueue("logs", -1, "denied", {"x": "y"})
        stream.flush(wait=True)
        stream.enqueue("other", 1, "also denied")
    assert dataset._clients.storage_writer.WriteLogs.call_count == 2
    failures = [record for record in caplog.records if record.exc_info]
    assert len(failures) == 2
    assert all(isinstance(record.exc_info[1], NominalPermissionDeniedError) for record in failures)
