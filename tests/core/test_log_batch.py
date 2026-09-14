from unittest.mock import MagicMock, patch

import requests
import zstandard
from conjure_python_client import ServiceConfiguration

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core._stream.batch_processor_proto import create_log_write_request, process_log_batch
from nominal.core._stream.write_stream import LogItem
from nominal.core._utils.networking import create_conjure_service_client
from nominal.protos.direct_channel_writer.v2 import direct_nominal_channel_writer_pb2 as wire


def test_log_batch_combines_channels_and_preserves_values():
    """The builder preserves timestamps and values while combining channels in insertion order."""
    items = [LogItem("b", -1, "雪", {"x": "1"}), LogItem("a", 1, "", None), LogItem("b", 2, "last", {"x": "2"})]
    request = create_log_write_request(items, "dataset")
    assert request.data_source_rid == "dataset"
    assert [b.channel for b in request.batches] == ["b", "a"]
    b, a = request.batches
    assert [(t.seconds, t.nanos) for t in b.points.timestamps] == [(-1, 999999999), (0, 2)]
    assert [p.value.message for p in b.points.log_points.points] == ["雪", "last"]
    assert [dict(p.value.args) for p in b.points.log_points.points] == [{"x": "1"}, {"x": "2"}]
    assert a.points.log_points.points[0].value.message == ""
    assert not a.tags


def test_configured_service_sends_zstd_protobuf_with_headers():
    """The configured service sends a decodable zstd/protobuf body with its normal HTTP headers."""
    service = create_conjure_service_client(
        ProtoWriteService, "log-test", ServiceConfiguration(uris=["https://api.example.com/api"])
    )
    with patch("nominal.core._utils.networking.SslBypassRequestsAdapter.send") as send:
        response = requests.Response()
        response.status_code = 204
        response._content = b""
        send.return_value = response
        service.write_columnar_batches(
            "Bearer synthetic", create_log_write_request([LogItem("a", 123, "hello", {"arg": "value"})], "dataset")
        )
    request = send.call_args.args[0]
    assert request.url == "https://api.example.com/api/storage/writer/v1/nominal-columnar"
    assert request.headers["Content-Encoding"] == "zstd"
    assert request.headers["Authorization"] == "Bearer synthetic"
    assert request.headers["User-Agent"] == "log-test"
    assert request.headers["Content-Type"] == "application/x-protobuf"
    decoded = wire.WriteBatchesRequest.FromString(zstandard.ZstdDecompressor().decompress(request.body))
    assert decoded.batches[0].points.log_points.points[0].value.message == "hello"


def test_empty_log_batch_does_not_upload():
    """An empty batch does not send an HTTP request."""
    writer = MagicMock()
    process_log_batch([], "dataset", "Bearer token", writer)
    writer.write_columnar_batches.assert_not_called()
